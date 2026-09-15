package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
)

const (
	RegionFeishu     = "feishu"
	RegionLark       = "lark"
	defaultFeishuURL = "https://open.feishu.cn"
	defaultLarkURL   = "https://open.larksuite.com"
	displayNameTTL   = 30 * time.Minute
	displayNameMax   = 2048
)

type Config struct {
	ConnectorID     string
	AppID           string
	AppSecret       string
	Region          string
	BotOpenID       string
	BotUnionID      string
	BaseURL         string
	CallbackBaseURL string
	HTTPClient      *http.Client
	Dialer          WSDialer
	ResourceStore   *uvim.ResourceStore
	PingInterval    time.Duration
	ReadDeadline    time.Duration
	WriteTimeout    time.Duration
	ChunkTTL        time.Duration
	Now             func() time.Time
	Logger          *slog.Logger
}

type Provider struct {
	config Config
	now    func() time.Time

	tokenMu  sync.Mutex
	token    string
	tokenExp time.Time

	displayNameMu    sync.Mutex
	displayNameCache map[string]displayNameEntry

	stateMu sync.Mutex
	state   string
}

type displayNameEntry struct {
	name      string
	expiresAt time.Time
}

type WSDialer interface {
	DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (WSConn, *http.Response, error)
}

type WSConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	WriteMessage(messageType int, data []byte) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	Close() error
}

type GorillaDialer struct {
	Dialer *websocket.Dialer
}

func (g GorillaDialer) DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (WSConn, *http.Response, error) {
	dialer := g.Dialer
	if dialer == nil {
		dialer = &websocket.Dialer{Proxy: http.ProxyFromEnvironment}
	}
	return uvim.DialWebSocket(ctx, dialer, urlStr, requestHeader)
}

func New(config Config) (*Provider, error) {
	if strings.TrimSpace(config.AppID) == "" || strings.TrimSpace(config.AppSecret) == "" {
		return nil, fmt.Errorf("lark provider: app_id and app_secret are required")
	}
	if config.ConnectorID == "" {
		config.ConnectorID = "lark"
	}
	if config.Region == "" {
		config.Region = RegionFeishu
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{}
	}
	if config.Dialer == nil {
		config.Dialer = GorillaDialer{Dialer: &websocket.Dialer{}}
	}
	if config.PingInterval <= 0 {
		config.PingInterval = 2 * time.Minute
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Provider{config: config, now: config.Now, displayNameCache: map[string]displayNameEntry{}, state: "configured"}, nil
}

func (p *Provider) ID() string          { return "lark" }
func (p *Provider) ConnectorID() string { return p.config.ConnectorID }
func (p *Provider) Capabilities() uvim.Capabilities {
	return uvim.Capabilities{
		Inbound:          true,
		Outbound:         true,
		DirectMessage:    true,
		GroupMessage:     true,
		ThreadReply:      true,
		ReplyMessage:     true,
		ProactiveDirect:  true,
		ProactiveGroup:   true,
		TargetKinds:      []string{uvim.TargetUser, uvim.TargetGroup, uvim.TargetConversation},
		UploadResource:   p.config.ResourceStore != nil,
		DownloadResource: true,
		ResourceKinds:    []string{uvim.ElementImage, uvim.ElementAudio, uvim.ElementVideo, uvim.ElementFile},
		ChannelTypes:     []string{uvim.ChannelDirect, uvim.ChannelGroup},
	}
}

func (p *Provider) Health(context.Context) uvim.Health {
	p.stateMu.Lock()
	state := p.state
	p.stateMu.Unlock()
	return uvim.Health{Provider: p.ID(), Connector: p.ConnectorID(), State: state, CheckedAt: time.Now().UTC(), Capabilities: p.Capabilities()}
}

func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error {
	endpoint, err := p.endpoint(ctx)
	if err != nil {
		p.setState("error")
		return err
	}
	conn, _, err := p.config.Dialer.DialContext(ctx, endpoint.URL, endpoint.Headers)
	if err != nil {
		p.setState("error")
		return fmt.Errorf("dial ws: %w", err)
	}
	defer conn.Close()
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopClose := context.AfterFunc(runCtx, func() { _ = conn.Close() })
	defer stopClose()
	var writeMu sync.Mutex
	pingInterval := endpoint.PingInterval
	if pingInterval <= 0 {
		pingInterval = p.config.PingInterval
	}
	transportCtx, stopTransport := context.WithCancel(runCtx)
	defer stopTransport()
	pingDone := make(chan struct{})
	go p.pingLoop(transportCtx, conn, &writeMu, endpoint.ServiceID, pingInterval, pingDone)
	incoming, events := make(chan uvim.Event), make(chan uvim.Event)
	readErr, emitErr := make(chan error, 1), make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(2)
	go func(incoming chan<- uvim.Event, readErr chan<- error) {
		defer workers.Done()
		readErr <- p.readEvents(transportCtx, conn, &writeMu, endpoint.ServiceID, incoming)
	}(incoming, readErr)
	go func(events <-chan uvim.Event) {
		defer workers.Done()
		for {
			select {
			case <-runCtx.Done():
				return
			case event, ok := <-events:
				if !ok {
					emitErr <- nil
					return
				}
				if runCtx.Err() != nil {
					return
				}
				p.enrichEventDisplayNames(runCtx, &event)
				p.enrichQuotedMessage(runCtx, &event)
				if err := sink.Emit(runCtx, event); err != nil {
					emitErr <- err
					return
				}
			}
		}
	}(events)
	defer func() {
		cancel()
		_ = conn.Close()
		workers.Wait()
		<-pingDone
	}()
	p.setState("connected")
	var queued []uvim.Event
	var connectionErr error
	for {
		if readErr == nil && len(queued) == 0 && events != nil {
			close(events)
			events = nil
		}
		var ready chan uvim.Event
		var next uvim.Event
		if len(queued) > 0 {
			ready, next = events, queued[0]
		}
		select {
		case <-runCtx.Done():
			return nil
		case err := <-readErr:
			connectionErr = err
			if err != nil {
				p.setState("error")
			}
			readErr, incoming = nil, nil
			stopTransport()
			_ = conn.Close()
		case err := <-emitErr:
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return errors.Join(connectionErr, fmt.Errorf("emit event: %w", err))
			}
			return connectionErr
		case event := <-incoming:
			p.setState("event")
			queued = append(queued, event)
		case ready <- next:
			queued[0] = uvim.Event{}
			queued = queued[1:]
		}
	}
}

// readEvents acknowledges frames without waiting for display-name lookups or
// attachment downloads. Run retains received events until serial delivery ends.
func (p *Provider) readEvents(ctx context.Context, conn WSConn, writeMu *sync.Mutex, serviceID int32, events chan<- uvim.Event) error {
	assembler := newChunkAssembler(p.config.ChunkTTL, p.config.Now)
	for {
		if err := conn.SetReadDeadline(uvim.OptionalDeadline(p.now(), p.config.ReadDeadline)); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.setState("error")
			return fmt.Errorf("set read deadline: %w", err)
		}
		msgType, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}
			p.setState("error")
			return fmt.Errorf("read message: %w", err)
		}
		if msgType != websocket.BinaryMessage {
			continue
		}
		inbound, err := unmarshalFrame(raw)
		if err != nil {
			p.config.Logger.Warn("lark frame decode failed", "err", err.Error(), "raw_len", len(raw))
			continue
		}
		if inbound.Method == frameMethodControl {
			if inbound.headerValue(frameHeaderTypeKey) == frameHeaderTypePing {
				if err := p.writeFrame(writeMu, conn, newPongFrame(serviceID)); err != nil {
					return fmt.Errorf("write pong: %w", err)
				}
			}
			continue
		}
		sum, seq, messageID := parseChunkHeaders(inbound)
		payload := inbound.Payload
		if sum > 1 {
			assembled, complete := assembler.admit(messageID, sum, seq, inbound.Payload)
			if !complete {
				continue
			}
			payload = assembled
		}
		event, ok, decodeErr := DecodePayload(payload, DecoderConfig{
			AppID:      p.config.AppID,
			BotOpenID:  p.config.BotOpenID,
			BotUnionID: p.config.BotUnionID,
			Connector:  p.ConnectorID(),
		})
		if decodeErr != nil {
			p.config.Logger.Warn("lark payload decode failed", "err", decodeErr.Error(), "payload_len", len(payload))
			if err := p.writeFrame(writeMu, conn, newAckFrame(inbound, true)); err != nil {
				return fmt.Errorf("write ack: %w", err)
			}
			continue
		}
		if err := p.writeFrame(writeMu, conn, newAckFrame(inbound, true)); err != nil {
			return fmt.Errorf("write ack: %w", err)
		}
		if !ok {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case events <- event:
		}
	}
}

func (p *Provider) enrichEventDisplayNames(ctx context.Context, event *uvim.Event) {
	if event == nil {
		return
	}
	if event.Channel.Type == uvim.ChannelGroup && strings.TrimSpace(event.Channel.ID) != "" && strings.TrimSpace(event.Channel.Name) == "" {
		name, err := p.cachedDisplayName(ctx, "chat:"+event.Channel.ID, func(ctx context.Context) (string, error) {
			return p.larkChatName(ctx, event.Channel.ID)
		})
		if err != nil {
			p.config.Logger.Debug("lark chat display-name lookup failed", "err", err.Error())
		} else {
			event.Channel.Name = name
		}
	}
	if strings.TrimSpace(event.User.ID) != "" && strings.TrimSpace(uvim.FirstNonEmpty(event.User.DisplayName, event.User.Name)) == "" {
		name, err := p.cachedDisplayName(ctx, "user:"+event.User.ID, func(ctx context.Context) (string, error) {
			return p.larkUserName(ctx, event.User.ID)
		})
		if err != nil {
			p.config.Logger.Debug("lark user display-name lookup failed", "err", err.Error())
		} else {
			event.User.DisplayName = name
		}
	}
}

func (p *Provider) cachedDisplayName(ctx context.Context, key string, lookup func(context.Context) (string, error)) (string, error) {
	now := p.now()
	p.displayNameMu.Lock()
	entry, ok := p.displayNameCache[key]
	if ok && now.Before(entry.expiresAt) {
		p.displayNameMu.Unlock()
		return entry.name, nil
	}
	if ok {
		delete(p.displayNameCache, key)
	}
	p.displayNameMu.Unlock()

	name, err := lookup(ctx)
	name = strings.TrimSpace(name)
	if err != nil || name == "" {
		return name, err
	}
	p.displayNameMu.Lock()
	if len(p.displayNameCache) >= displayNameMax {
		for cachedKey, cached := range p.displayNameCache {
			if !now.Before(cached.expiresAt) {
				delete(p.displayNameCache, cachedKey)
			}
		}
	}
	if len(p.displayNameCache) >= displayNameMax {
		for cachedKey := range p.displayNameCache {
			delete(p.displayNameCache, cachedKey)
			break
		}
	}
	p.displayNameCache[key] = displayNameEntry{name: name, expiresAt: now.Add(displayNameTTL)}
	p.displayNameMu.Unlock()
	return name, err
}

func (p *Provider) larkChatName(ctx context.Context, chatID string) (string, error) {
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL()+"/open-apis/im/v1/chats/"+url.PathEscape(chatID), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	raw, err := p.doJSON(req, "lark chat display name")
	if err != nil {
		return "", err
	}
	var decoded struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			Name string `json:"name"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return "", fmt.Errorf("decode chat display name: %w", err)
	}
	if decoded.Code != 0 {
		return "", fmt.Errorf("lark chat display name: code=%d msg=%q", decoded.Code, decoded.Msg)
	}
	return strings.TrimSpace(decoded.Data.Name), nil
}

func (p *Provider) larkUserName(ctx context.Context, userID string) (string, error) {
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return "", err
	}
	reqURL, err := url.Parse(p.baseURL() + "/open-apis/contact/v3/users/" + url.PathEscape(userID))
	if err != nil {
		return "", err
	}
	query := reqURL.Query()
	query.Set("user_id_type", "open_id")
	reqURL.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	raw, err := p.doJSON(req, "lark user display name")
	if err != nil {
		return "", err
	}
	var decoded struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			User struct {
				Name string `json:"name"`
			} `json:"user"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return "", fmt.Errorf("decode user display name: %w", err)
	}
	if decoded.Code != 0 {
		return "", fmt.Errorf("lark user display name: code=%d msg=%q", decoded.Code, decoded.Msg)
	}
	return strings.TrimSpace(decoded.Data.User.Name), nil
}

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (result uvim.SendResult, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("lark send", err)
	}()
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("lark send: %w", err)
	}
	if err := uvim.ValidateOutboundResources(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("lark send: %w", err)
	}
	if hasNonTextElements(msg.Elements) {
		return uvim.SendResult{}, fmt.Errorf("lark send: rich elements are not supported")
	}
	text := uvim.NormalizeOutboundText(msg.Text)
	if text == "" && len(msg.Elements) > 0 {
		text = uvim.NormalizeOutboundText(textFromElements(msg.Elements))
	}
	if len(msg.Resources) > 1 || (text != "" && len(msg.Resources) > 0) {
		return uvim.SendResourceSequence(ctx, msg, p.Send)
	}
	if text == "" && len(msg.Resources) == 0 {
		return uvim.SendResult{}, fmt.Errorf("lark send: text or resource is required")
	}
	msgType := "text"
	content := map[string]string{"text": text}
	if len(msg.Resources) == 1 {
		var err error
		msgType, content, err = p.uploadResource(ctx, msg.Resources[0])
		if err != nil {
			return uvim.SendResult{}, err
		}
	}
	contentRaw, _ := json.Marshal(content)
	body := map[string]any{"msg_type": msgType, "content": string(contentRaw)}
	endpoint := ""
	base := p.baseURL()
	if strings.TrimSpace(msg.Referrer.MessageID) != "" {
		endpoint = base + "/open-apis/im/v1/messages/" + url.PathEscape(msg.Referrer.MessageID) + "/reply"
	} else {
		target := msg.ResolvedTarget()
		if target.ID == "" {
			return uvim.SendResult{}, fmt.Errorf("lark send: message_id or target id is required")
		}
		receiveIDType := "chat_id"
		typedTarget := msg.Target != nil || msg.Referrer.Target != nil
		if typedTarget && target.Kind == uvim.TargetUser && !strings.HasPrefix(target.ID, "ou_") {
			sendErr := fmt.Errorf("lark send: user target id must be an Open ID")
			return uvim.SendResult{}, uvim.NewProviderSendFailure(uvim.SendFailure{
				Category:      uvim.SendFailureInvalidRequest,
				DeliveryState: uvim.DeliveryNotAttempted,
			}, sendErr.Error(), sendErr)
		}
		if (typedTarget && target.Kind == uvim.TargetUser) || strings.HasPrefix(target.ID, "ou_") {
			receiveIDType = "open_id"
		}
		endpoint = base + "/open-apis/im/v1/messages?receive_id_type=" + receiveIDType
		body["receive_id"] = target.ID
	}
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return uvim.SendResult{}, err
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return uvim.SendResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return uvim.SendResult{}, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	respRaw, err := p.doJSON(req, "lark send")
	if err != nil {
		return uvim.SendResult{}, err
	}
	var decoded struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respRaw, &decoded); err != nil {
		sendErr := fmt.Errorf("decode lark send response: %w", err)
		return uvim.SendResult{}, uvim.NewProviderSendLogError(sendErr.Error(), sendErr)
	}
	if decoded.Code != 0 {
		sendErr := fmt.Errorf("lark send: code=%d msg=%q", decoded.Code, decoded.Msg)
		return uvim.SendResult{}, uvim.NewProviderResponseError(respRaw, sendErr.Error(), sendErr)
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: decoded.Data.MessageID, Time: time.Now().UTC()}, nil
}

func (p *Provider) uploadResource(ctx context.Context, ref uvim.ResourceRef) (kind string, content map[string]string, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("lark upload", err)
	}()
	if p.config.ResourceStore == nil {
		return "", nil, fmt.Errorf("lark upload: resource store is not configured")
	}
	if !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return "", nil, fmt.Errorf("lark upload: internal resource is required")
	}
	file, _, err := p.config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return "", nil, uvim.NewProviderSendError("lark resource is unavailable", err)
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil {
		return "", nil, uvim.NewProviderSendError("lark resource read failed", readErr)
	}
	if closeErr != nil {
		return "", nil, uvim.NewProviderSendError("lark resource close failed", closeErr)
	}
	name := uvim.ResourceUploadName(0, ref, ref.MIME)
	if strings.EqualFold(strings.TrimSpace(ref.Kind), uvim.ElementImage) && larkNativeImageMIME(ref.MIME) {
		key, err := p.uploadMultipart(ctx, "/open-apis/im/v1/images", map[string]string{"image_type": "message"}, "image", name, ref.MIME, data, "image_key")
		if err != nil {
			return "", nil, err
		}
		return uvim.ElementImage, map[string]string{"image_key": key}, nil
	}
	key, err := p.uploadMultipart(ctx, "/open-apis/im/v1/files", map[string]string{"file_type": "stream", "file_name": name}, "file", name, ref.MIME, data, "file_key")
	if err != nil {
		return "", nil, err
	}
	return uvim.ElementFile, map[string]string{"file_key": key}, nil
}

func larkNativeImageMIME(mimeType string) bool {
	switch strings.ToLower(strings.TrimSpace(strings.SplitN(mimeType, ";", 2)[0])) {
	case "image/jpeg", "image/png", "image/webp", "image/gif", "image/bmp", "image/x-icon", "image/tiff", "image/heic":
		return true
	default:
		return false
	}
}

func (p *Provider) uploadMultipart(ctx context.Context, path string, fields map[string]string, fileField, name, mimeType string, data []byte, responseKey string) (key string, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("lark upload", err)
	}()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			return "", err
		}
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", fmt.Sprintf(`form-data; name=%q; filename=%q`, fileField, name))
	if strings.TrimSpace(mimeType) != "" {
		header.Set("Content-Type", mimeType)
	}
	part, err := writer.CreatePart(header)
	if err != nil {
		return "", err
	}
	if _, err := part.Write(data); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL()+path, &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	respRaw, err := p.doJSON(req, "lark upload")
	if err != nil {
		return "", err
	}
	var decoded struct {
		Code int               `json:"code"`
		Msg  string            `json:"msg"`
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(respRaw, &decoded); err != nil {
		return "", fmt.Errorf("decode lark upload response: %w", err)
	}
	if decoded.Code != 0 {
		sendErr := fmt.Errorf("lark upload: code=%d msg=%q", decoded.Code, decoded.Msg)
		return "", uvim.NewProviderResponseError(respRaw, sendErr.Error(), sendErr)
	}
	key = strings.TrimSpace(decoded.Data[responseKey])
	if key == "" {
		missingErr := fmt.Errorf("lark upload: response missing %s", responseKey)
		return "", uvim.NewProviderSendLogError("lark upload: response key missing", missingErr)
	}
	return key, nil
}

func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	ref := req.Resource
	if strings.TrimSpace(ref.Key) == "" {
		return ref, fmt.Errorf("lark download: resource key is required")
	}
	messageID := uvim.FirstNonEmpty(ref.Metadata["message_id"], req.Event.Message.ID, req.Message.ID)
	if strings.TrimSpace(messageID) == "" {
		return ref, fmt.Errorf("lark download: message id is required")
	}
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return ref, err
	}
	endpoint := p.baseURL() + "/open-apis/im/v1/messages/" + url.PathEscape(messageID) + "/resources/" + url.PathEscape(ref.Key)
	reqURL, err := url.Parse(endpoint)
	if err != nil {
		return ref, err
	}
	query := reqURL.Query()
	query.Set("type", resourceType(ref.Kind))
	reqURL.RawQuery = query.Encode()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
	if err != nil {
		return ref, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	return p.store(req.Dir).SaveHTTP(ctx, httpReq, ref)
}

type endpoint struct {
	URL          string
	Headers      http.Header
	ServiceID    int32
	PingInterval time.Duration
}

func (p *Provider) endpoint(ctx context.Context) (endpoint, error) {
	body := map[string]string{"AppID": p.config.AppID, "AppSecret": p.config.AppSecret}
	raw, err := json.Marshal(body)
	if err != nil {
		return endpoint{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.callbackBaseURL()+"/callback/ws/endpoint", bytes.NewReader(raw))
	if err != nil {
		return endpoint{}, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("locale", "zh")
	respRaw, err := p.doJSON(req, "lark ws endpoint")
	if err != nil {
		return endpoint{}, err
	}
	var decoded struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			URL          string `json:"URL"`
			ClientConfig struct {
				PingInterval int `json:"PingInterval"`
			} `json:"ClientConfig"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respRaw, &decoded); err != nil {
		return endpoint{}, fmt.Errorf("decode endpoint response: %w", err)
	}
	if decoded.Code != 0 || decoded.Data.URL == "" {
		return endpoint{}, fmt.Errorf("lark ws endpoint: code=%d msg=%q", decoded.Code, decoded.Msg)
	}
	serviceID, err := parseServiceID(decoded.Data.URL)
	if err != nil {
		return endpoint{}, err
	}
	return endpoint{URL: decoded.Data.URL, Headers: http.Header{}, ServiceID: serviceID, PingInterval: time.Duration(decoded.Data.ClientConfig.PingInterval) * time.Second}, nil
}

func (p *Provider) tenantAccessToken(ctx context.Context) (string, error) {
	p.tokenMu.Lock()
	if p.token != "" && p.now().Before(p.tokenExp.Add(-5*time.Minute)) {
		token := p.token
		p.tokenMu.Unlock()
		return token, nil
	}
	p.tokenMu.Unlock()
	body := map[string]string{"app_id": p.config.AppID, "app_secret": p.config.AppSecret}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL()+"/open-apis/auth/v3/tenant_access_token/internal", bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	respRaw, err := p.doJSON(req, "lark tenant access token")
	if err != nil {
		return "", err
	}
	var decoded struct {
		Code              int    `json:"code"`
		Msg               string `json:"msg"`
		TenantAccessToken string `json:"tenant_access_token"`
		Expire            int64  `json:"expire"`
	}
	if err := json.Unmarshal(respRaw, &decoded); err != nil {
		return "", fmt.Errorf("decode tenant_access_token response: %w", err)
	}
	if decoded.Code != 0 || decoded.TenantAccessToken == "" {
		return "", fmt.Errorf("lark tenant_access_token: code=%d msg=%q", decoded.Code, decoded.Msg)
	}
	p.tokenMu.Lock()
	p.token = decoded.TenantAccessToken
	p.tokenExp = p.now().Add(time.Duration(decoded.Expire) * time.Second)
	p.tokenMu.Unlock()
	return decoded.TenantAccessToken, nil
}

func (p *Provider) doJSON(req *http.Request, operation string) (json.RawMessage, error) {
	resp, err := p.config.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return httpchannel.ReadPrivateSendResponse(resp, operation)
}

func (p *Provider) writeFrame(mu *sync.Mutex, conn WSConn, frame *wsFrame) error {
	raw := frame.marshal()
	mu.Lock()
	defer mu.Unlock()
	if err := conn.SetWriteDeadline(uvim.OptionalDeadline(p.now(), p.config.WriteTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, raw)
}

func (p *Provider) pingLoop(ctx context.Context, conn WSConn, writeMu *sync.Mutex, serviceID int32, interval time.Duration, done chan<- struct{}) {
	defer close(done)
	if interval <= 0 {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.writeFrame(writeMu, conn, newPingFrame(serviceID)); err != nil {
				p.config.Logger.Warn("lark ping failed", "err", err.Error())
				_ = conn.Close()
				return
			}
		}
	}
}

func (p *Provider) baseURL() string {
	if base := strings.TrimRight(strings.TrimSpace(p.config.BaseURL), "/"); base != "" {
		return base
	}
	if strings.EqualFold(strings.TrimSpace(p.config.Region), RegionLark) {
		return defaultLarkURL
	}
	return defaultFeishuURL
}

func (p *Provider) callbackBaseURL() string {
	if base := strings.TrimRight(strings.TrimSpace(p.config.CallbackBaseURL), "/"); base != "" {
		return base
	}
	return p.baseURL()
}

func (p *Provider) store(dir string) *uvim.ResourceStore {
	if p.config.ResourceStore != nil && dir == "" {
		return p.config.ResourceStore
	}
	store := &uvim.ResourceStore{Dir: dir, HTTPClient: p.config.HTTPClient}
	if store.Dir == "" && p.config.ResourceStore != nil {
		store.Dir = p.config.ResourceStore.Dir
	}
	return store
}

func (p *Provider) setState(state string) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	p.state = state
}

func parseServiceID(rawURL string) (int32, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return 0, err
	}
	value := u.Query().Get("service_id")
	if value == "" {
		return 0, errors.New("missing service_id query parameter")
	}
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid service_id %q: %w", value, err)
	}
	return int32(n), nil
}

func resourceType(kind string) string {
	if strings.EqualFold(strings.TrimSpace(kind), uvim.ElementImage) {
		return "image"
	}
	return "file"
}

func hasNonTextElements(elements []uvim.Element) bool {
	for _, element := range elements {
		if element.Type != "" && element.Type != uvim.ElementText {
			return true
		}
		if element.Resource != nil {
			return true
		}
		if hasNonTextElements(element.Children) {
			return true
		}
	}
	return false
}

func textFromElements(elements []uvim.Element) string {
	parts := make([]string, 0, len(elements))
	for _, element := range elements {
		if element.Type == uvim.ElementText && strings.TrimSpace(element.Text) != "" {
			parts = append(parts, element.Text)
		}
		if childText := textFromElements(element.Children); childText != "" {
			parts = append(parts, childText)
		}
	}
	return strings.Join(parts, "\n")
}
