package wecom

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
)

const (
	defaultWSURL     = "wss://openws.work.weixin.qq.com"
	cmdSubscribe     = "aibot_subscribe"
	cmdHeartbeat     = "ping"
	cmdCallback      = "aibot_msg_callback"
	cmdEventCallback = "aibot_event_callback"
	cmdRespond       = "aibot_respond_msg"
	cmdSend          = "aibot_send_msg"
	cmdUploadInit    = "aibot_upload_media_init"
	cmdUploadChunk   = "aibot_upload_media_chunk"
	cmdUploadFinish  = "aibot_upload_media_finish"
	mediaVoice       = "voice"
	uploadChunkSize  = 512 * 1024
)

type Config struct {
	ConnectorID       string
	BotID             string
	Secret            string
	UserNames         map[string]string
	ConversationNames map[string]string
	WSURL             string
	Dialer            WSDialer
	HTTPClient        *http.Client
	ResourceStore     *uvim.ResourceStore
	HeartbeatInterval time.Duration
	ReadDeadline      time.Duration
	WriteTimeout      time.Duration
	AckTimeout        time.Duration
	Now               func() time.Time
	Logger            *slog.Logger
}

type Provider struct {
	config Config
	now    func() time.Time

	mu           sync.Mutex
	pending      map[string]chan frame
	replyLocks   map[string]*replyLock
	replyLocksMu sync.Mutex

	activeMu    sync.Mutex
	activeConn  WSConn
	activeWrite *sync.Mutex
	state       string
}

type replyLock struct {
	ch   chan struct{}
	refs int
}

type frame struct {
	Cmd     string         `json:"cmd,omitempty"`
	Headers headers        `json:"headers,omitempty"`
	Body    map[string]any `json:"body,omitempty"`
	ErrCode *int           `json:"errcode,omitempty"`
	ErrMsg  string         `json:"errmsg,omitempty"`
}

type headers struct {
	ReqID string `json:"req_id,omitempty"`
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
	if strings.TrimSpace(config.BotID) == "" || strings.TrimSpace(config.Secret) == "" {
		return nil, fmt.Errorf("wecom provider: bot_id and secret are required")
	}
	if config.ConnectorID == "" {
		config.ConnectorID = "wecom"
	}
	if config.WSURL == "" {
		config.WSURL = defaultWSURL
	}
	if config.Dialer == nil {
		config.Dialer = GorillaDialer{Dialer: &websocket.Dialer{}}
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{}
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 30 * time.Second
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Provider{config: config, now: config.Now, pending: map[string]chan frame{}, replyLocks: map[string]*replyLock{}, state: "configured"}, nil
}

func (p *Provider) ID() string          { return "wecom" }
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
	p.activeMu.Lock()
	state := p.state
	p.activeMu.Unlock()
	return uvim.Health{Provider: p.ID(), Connector: p.ConnectorID(), State: state, CheckedAt: time.Now().UTC(), Capabilities: p.Capabilities()}
}

func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error {
	conn, _, err := p.config.Dialer.DialContext(ctx, p.config.WSURL, http.Header{})
	if err != nil {
		p.setState("error")
		return fmt.Errorf("dial ws: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer conn.Close()
	stopClose := context.AfterFunc(runCtx, func() { _ = conn.Close() })
	defer stopClose()
	var writeMu sync.Mutex

	authReqID := p.reqID(cmdSubscribe)
	if err := p.writeFrame(conn, &writeMu, frame{
		Cmd:     cmdSubscribe,
		Headers: headers{ReqID: authReqID},
		Body: map[string]any{
			"bot_id": p.config.BotID,
			"secret": p.config.Secret,
		},
	}); err != nil {
		p.setState("error")
		return fmt.Errorf("send auth: %w", err)
	}
	if err := p.waitAuth(ctx, conn, authReqID); err != nil {
		p.setState("error")
		return err
	}
	p.setActive(conn, &writeMu)
	defer p.clearActive(conn)
	p.setState("connected")

	transportCtx, stopTransport := context.WithCancel(runCtx)
	defer stopTransport()
	heartbeatDone := make(chan struct{})
	go p.heartbeat(transportCtx, conn, &writeMu, heartbeatDone)
	frames := make(chan frame)
	events := make(chan uvim.Event)
	readErr := make(chan error, 1)
	emitErr := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(2)
	go func(frames chan<- frame, readErr chan<- error) {
		defer workers.Done()
		readErr <- p.readFrames(transportCtx, conn, frames)
	}(frames, readErr)
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
		<-heartbeatDone
		p.failAllPending("connection closed")
	}()

	// Queue only event metadata and emit serially to preserve arrival order.
	// Waiting on the sink (including attachment downloads) must never stop ACK
	// dispatch, even when more callbacks arrive while an event is being emitted.
	var queued []uvim.Event
	var connectionErr error
	finishReading := func(err error) {
		connectionErr = err
		readErr, frames = nil, nil
		stopTransport()
		_ = conn.Close()
		p.failAllPending("connection closed")
	}
	for {
		if readErr == nil && len(queued) == 0 && events != nil {
			// Transport loss does not cancel independent downloads or discard
			// received callbacks. Drain them before returning the connection error.
			close(events)
			events = nil
		}
		var ready chan uvim.Event
		var next uvim.Event
		if len(queued) > 0 {
			ready, next = events, queued[0]
		}
		var inbound frame
		select {
		case <-runCtx.Done():
			return nil
		case err := <-readErr:
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				p.setState("error")
			}
			finishReading(err)
			continue
		case err := <-emitErr:
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return errors.Join(connectionErr, fmt.Errorf("emit event: %w", err))
			}
			return connectionErr
		case ready <- next:
			queued[0] = uvim.Event{}
			queued = queued[1:]
			continue
		case inbound = <-frames:
		}
		if inbound.Cmd == "" {
			p.resolvePending(inbound.Headers.ReqID, inbound)
			continue
		}
		if inbound.Cmd == cmdEventCallback {
			if eventType := uvim.StringValue(uvim.MapStringAny(inbound.Body["event"])["eventtype"]); eventType == "disconnected_event" {
				p.setState("disconnected")
				finishReading(fmt.Errorf("wecom disconnected_event: another connector is active"))
			}
			continue
		}
		if inbound.Cmd != cmdCallback {
			continue
		}
		event, ok := p.decodeMessage(inbound)
		if !ok {
			continue
		}
		p.setState("event")
		queued = append(queued, event)
	}
}

func (p *Provider) readFrames(ctx context.Context, conn WSConn, frames chan<- frame) error {
	for {
		if err := conn.SetReadDeadline(uvim.OptionalDeadline(p.now(), p.config.ReadDeadline)); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}
		msgType, raw, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}
			return fmt.Errorf("read message: %w", err)
		}
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		var inbound frame
		if err := json.Unmarshal(raw, &inbound); err != nil {
			p.config.Logger.Warn("wecom frame decode failed", "err", err.Error(), "raw_len", len(raw))
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frames <- inbound:
		}
	}
}

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (result uvim.SendResult, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("wecom send", err)
	}()
	if err := uvim.ValidateOutboundTarget(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("wecom send: %w", err)
	}
	if err := uvim.ValidateOutboundResources(msg, p.Capabilities()); err != nil {
		return uvim.SendResult{}, fmt.Errorf("wecom send: %w", err)
	}
	if hasNonTextElements(msg.Elements) {
		sendErr := fmt.Errorf("wecom send: rich elements are not supported")
		return uvim.SendResult{}, uvim.NewProviderSendLogError("wecom send: rich elements are not supported", sendErr)
	}
	text := uvim.NormalizeOutboundText(msg.Text)
	if text == "" && len(msg.Elements) > 0 {
		text = uvim.NormalizeOutboundText(textFromElements(msg.Elements))
	}
	if len(msg.Resources) > 1 || (text != "" && len(msg.Resources) > 0) {
		return uvim.SendResourceSequence(ctx, msg, p.Send)
	}
	if text == "" && len(msg.Resources) == 0 {
		return uvim.SendResult{}, fmt.Errorf("wecom send: text or resource is required")
	}
	conn, writeMu, err := p.waitActive(ctx)
	if err != nil {
		return uvim.SendResult{}, err
	}
	if len(msg.Resources) == 1 {
		media, err := p.uploadResource(ctx, conn, writeMu, msg.Resources[0])
		if err != nil {
			return uvim.SendResult{}, err
		}
		return p.sendMedia(ctx, conn, writeMu, msg, media)
	}
	reqID := uvim.FirstNonEmpty(msg.Referrer.ReplyToken, p.reqID(cmdSend))
	out := frame{Headers: headers{ReqID: reqID}}
	if msg.Referrer.ReplyToken != "" {
		streamID := "uv-" + uvim.SafeSegment(uvim.FirstNonEmpty(msg.Referrer.MessageID, msg.ID, reqID))
		out.Cmd = cmdRespond
		out.Body = map[string]any{
			"msgtype": "stream",
			"stream": map[string]any{
				"id":      streamID,
				"content": text,
				"finish":  msg.Final,
			},
		}
		if err := p.withReplyLock(ctx, reqID, func() error {
			return p.sendWithAck(ctx, conn, writeMu, reqID, out)
		}); err != nil {
			return uvim.SendResult{}, err
		}
		return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: streamID, Time: time.Now().UTC()}, nil
	}
	target := msg.ResolvedTarget()
	recipient := target.ID
	if recipient == "" {
		return uvim.SendResult{}, fmt.Errorf("wecom send: target id is required")
	}
	out.Cmd = cmdSend
	out.Body = map[string]any{
		"chatid":  recipient,
		"msgtype": "markdown",
		"markdown": map[string]any{
			"content": text,
		},
	}
	if err := p.sendWithAck(ctx, conn, writeMu, reqID, out); err != nil {
		return uvim.SendResult{}, err
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: reqID, Time: time.Now().UTC()}, nil
}

type uploadedMedia struct {
	kind    string
	mediaID string
}

func (p *Provider) uploadResource(ctx context.Context, conn WSConn, writeMu *sync.Mutex, ref uvim.ResourceRef) (media uploadedMedia, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("wecom upload", err)
	}()
	if p.config.ResourceStore == nil {
		return uploadedMedia{}, fmt.Errorf("wecom upload: resource store is not configured")
	}
	if !strings.HasPrefix(strings.TrimSpace(ref.InternalURL), "internal://") {
		return uploadedMedia{}, fmt.Errorf("wecom upload: internal resource is required")
	}
	file, _, err := p.config.ResourceStore.Open(ref.InternalURL)
	if err != nil {
		return uploadedMedia{}, uvim.NewProviderSendError("wecom resource is unavailable", err)
	}
	data, readErr := io.ReadAll(file)
	closeErr := file.Close()
	if readErr != nil {
		return uploadedMedia{}, uvim.NewProviderSendError("wecom resource read failed", readErr)
	}
	if closeErr != nil {
		return uploadedMedia{}, uvim.NewProviderSendError("wecom resource close failed", closeErr)
	}
	totalChunks, err := wecomUploadChunkCount(len(data))
	if err != nil {
		return uploadedMedia{}, err
	}
	kind := wecomMediaKind(ref.Kind)
	filename := uvim.ResourceUploadName(0, ref, ref.MIME)
	// WeCom requires MD5 as a transport checksum; it is not used for security.
	digest := md5.Sum(data) // #nosec G401 -- required by the WeCom upload protocol
	initReqID := p.reqID(cmdUploadInit)
	initAck, err := p.requestWithAck(ctx, conn, writeMu, initReqID, frame{
		Cmd:     cmdUploadInit,
		Headers: headers{ReqID: initReqID},
		Body: map[string]any{
			"type":         kind,
			"filename":     filename,
			"total_size":   len(data),
			"total_chunks": totalChunks,
			"md5":          fmt.Sprintf("%x", digest),
		},
	})
	if err != nil {
		return uploadedMedia{}, err
	}
	uploadID := uvim.StringValue(initAck.Body["upload_id"])
	if uploadID == "" {
		missingErr := fmt.Errorf("wecom upload: init response missing upload_id")
		return uploadedMedia{}, uvim.NewProviderSendLogError("wecom upload: upload ID missing", missingErr)
	}
	for index := 0; index < totalChunks; index++ {
		start := index * uploadChunkSize
		end := min(start+uploadChunkSize, len(data))
		chunkReqID := p.reqID(cmdUploadChunk)
		if _, err := p.requestWithAck(ctx, conn, writeMu, chunkReqID, frame{
			Cmd:     cmdUploadChunk,
			Headers: headers{ReqID: chunkReqID},
			Body: map[string]any{
				"upload_id":   uploadID,
				"chunk_index": index,
				"base64_data": base64.StdEncoding.EncodeToString(data[start:end]),
			},
		}); err != nil {
			return uploadedMedia{}, err
		}
	}
	finishReqID := p.reqID(cmdUploadFinish)
	finishAck, err := p.requestWithAck(ctx, conn, writeMu, finishReqID, frame{
		Cmd:     cmdUploadFinish,
		Headers: headers{ReqID: finishReqID},
		Body:    map[string]any{"upload_id": uploadID},
	})
	if err != nil {
		return uploadedMedia{}, err
	}
	mediaID := uvim.StringValue(finishAck.Body["media_id"])
	if mediaID == "" {
		missingErr := fmt.Errorf("wecom upload: finish response missing media_id")
		return uploadedMedia{}, uvim.NewProviderSendLogError("wecom upload: media ID missing", missingErr)
	}
	return uploadedMedia{kind: kind, mediaID: mediaID}, nil
}

func wecomUploadChunkCount(size int) (int, error) {
	if size < 0 {
		return 0, fmt.Errorf("wecom upload: negative resource size")
	}
	if size == 0 {
		return 0, nil
	}
	return 1 + (size-1)/uploadChunkSize, nil
}

func (p *Provider) sendMedia(ctx context.Context, conn WSConn, writeMu *sync.Mutex, msg uvim.OutboundMessage, media uploadedMedia) (result uvim.SendResult, err error) {
	defer func() {
		err = uvim.NewProviderSendOperationError("wecom send", err)
	}()
	content := map[string]any{"media_id": media.mediaID}
	body := map[string]any{"msgtype": media.kind, media.kind: content}
	if msg.Referrer.ReplyToken != "" {
		reqID := msg.Referrer.ReplyToken
		err := p.withReplyLock(ctx, reqID, func() error {
			return p.sendWithAck(ctx, conn, writeMu, reqID, frame{Cmd: cmdRespond, Headers: headers{ReqID: reqID}, Body: body})
		})
		if err != nil {
			return uvim.SendResult{}, err
		}
		return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: reqID, Time: time.Now().UTC()}, nil
	}
	target := msg.ResolvedTarget()
	if target.ID == "" {
		return uvim.SendResult{}, fmt.Errorf("wecom send: target id is required")
	}
	reqID := p.reqID(cmdSend)
	body["chatid"] = target.ID
	if err := p.sendWithAck(ctx, conn, writeMu, reqID, frame{Cmd: cmdSend, Headers: headers{ReqID: reqID}, Body: body}); err != nil {
		return uvim.SendResult{}, err
	}
	return uvim.SendResult{Provider: p.ID(), Connector: p.ConnectorID(), MessageID: reqID, Time: time.Now().UTC()}, nil
}

func wecomMediaKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case uvim.ElementImage:
		return uvim.ElementImage
	case uvim.ElementVideo:
		return uvim.ElementVideo
	case uvim.ElementAudio:
		return mediaVoice
	default:
		return uvim.ElementFile
	}
}

func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	ref := req.Resource
	if strings.TrimSpace(ref.URL) == "" {
		return ref, fmt.Errorf("wecom download: resource url is required")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, ref.URL, nil)
	if err != nil {
		return ref, err
	}
	store := p.store(req.Dir)
	if ref.Secret == "" {
		return store.SaveHTTP(ctx, httpReq, ref)
	}
	resp, err := p.config.HTTPClient.Do(httpReq)
	if err != nil {
		return ref, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ref, fmt.Errorf("http %d", resp.StatusCode)
	}
	encrypted, err := io.ReadAll(resp.Body)
	if err != nil {
		return ref, err
	}
	plain, err := DecryptAttachment(encrypted, ref.Secret)
	if err != nil {
		return ref, err
	}
	return store.Save(ctx, bytes.NewReader(plain), ref)
}

func (p *Provider) decodeMessage(in frame) (uvim.Event, bool) {
	body := in.Body
	if len(body) == 0 {
		return uvim.Event{}, false
	}
	msgType := uvim.StringValue(body["msgtype"])
	text := p.messageText(body, msgType)
	resources := p.messageResources(body, msgType)
	if quote := uvim.MapStringAny(body["quote"]); len(quote) > 0 {
		quoteType := uvim.StringValue(quote["msgtype"])
		resources = append(resources, p.messageResources(quote, quoteType)...)
		if quoteText := quotedMessageText(quote, quoteType); quoteText != "" {
			resources = append(resources, p.quotedTextResource(quoteType, quoteText))
		}
	}
	if strings.TrimSpace(text) == "" && len(resources) == 0 {
		return uvim.Event{}, false
	}
	chatType := uvim.StringValue(body["chattype"])
	channelType := uvim.ChannelDirect
	if strings.EqualFold(chatType, "group") {
		channelType = uvim.ChannelGroup
	}
	from := uvim.MapStringAny(body["from"])
	userID := uvim.StringValue(from["userid"])
	channelID := uvim.FirstNonEmpty(uvim.StringValue(body["chatid"]), userID)
	userName := uvim.FirstNonEmpty(uvim.StringValue(from["display_name"]), uvim.StringValue(from["name"]), p.config.UserNames[userID])
	channelName := uvim.FirstNonEmpty(uvim.StringValue(body["chat_name"]), uvim.StringValue(body["chatname"]), p.config.ConversationNames[channelID])
	messageID := uvim.FirstNonEmpty(uvim.StringValue(body["msgid"]), in.Headers.ReqID)
	now := p.now().UTC()
	expiresAt := now.Add(10 * time.Minute)
	targetKind := uvim.TargetUser
	if channelType == uvim.ChannelGroup {
		targetKind = uvim.TargetGroup
	}
	return uvim.Event{
		ID:        uvim.FirstNonEmpty(uvim.StringValue(body["msgid"]), in.Headers.ReqID),
		Type:      uvim.EventMessageCreate,
		Provider:  p.ID(),
		Connector: p.ConnectorID(),
		Time:      now,
		Login:     uvim.Login{Platform: p.ID(), Connector: p.ConnectorID(), ID: p.config.BotID},
		Channel:   uvim.Channel{ID: channelID, Type: channelType, Name: channelName},
		User:      uvim.User{ID: userID, DisplayName: userName},
		Message: uvim.Message{
			ID:        messageID,
			Type:      msgType,
			Text:      text,
			Elements:  elementsFromTextAndResources(text, resources),
			Resources: resources,
		},
		Referrer: uvim.Referrer{MessageID: messageID, ChannelID: channelID, ReplyToken: in.Headers.ReqID, ExpiresAt: &expiresAt, Target: &uvim.OutboundTarget{ID: channelID, Kind: targetKind}},
		// AI Bot callbacks are interactions delivered to this bot, including group @mentions.
		Addressed: true,
	}, true
}

func quotedMessageText(body map[string]any, msgType string) string {
	switch msgType {
	case "text":
		return strings.TrimSpace(uvim.StringValue(uvim.MapStringAny(body["text"])["content"]))
	case "voice":
		return strings.TrimSpace(uvim.StringValue(uvim.MapStringAny(body["voice"])["content"]))
	case "mixed":
		items, _ := uvim.MapStringAny(body["mixed"])["msg_item"].([]any)
		var parts []string
		for _, itemValue := range items {
			item := uvim.MapStringAny(itemValue)
			switch uvim.StringValue(item["msgtype"]) {
			case "text":
				if content := strings.TrimSpace(uvim.StringValue(uvim.MapStringAny(item["text"])["content"])); content != "" {
					parts = append(parts, content)
				}
			case "voice":
				if content := strings.TrimSpace(uvim.StringValue(uvim.MapStringAny(item["voice"])["content"])); content != "" {
					parts = append(parts, content)
				}
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func (p *Provider) quotedTextResource(msgType, text string) uvim.ResourceRef {
	ref := uvim.ResourceRef{
		Provider:  p.ID(),
		Connector: p.ConnectorID(),
		Kind:      uvim.ElementFile,
		Name:      "quoted-message.txt",
		MIME:      "text/plain; charset=utf-8",
	}
	if p.config.ResourceStore == nil {
		ref.Error = "quoted_message_context_store_unavailable"
		return ref
	}
	body := fmt.Sprintf("Message type: %s\n\n%s\n", uvim.FirstNonEmpty(msgType, "text"), text)
	saved, err := p.config.ResourceStore.Save(context.Background(), strings.NewReader(body), ref)
	if err != nil {
		ref.Error = "quoted_message_context_store_failed"
		return ref
	}
	return saved
}

func (p *Provider) messageText(body map[string]any, msgType string) string {
	switch msgType {
	case "text":
		return uvim.StringValue(uvim.MapStringAny(body["text"])["content"])
	case "voice":
		return uvim.StringValue(uvim.MapStringAny(body["voice"])["content"])
	case "mixed":
		items, _ := uvim.MapStringAny(body["mixed"])["msg_item"].([]any)
		var parts []string
		for _, itemValue := range items {
			item := uvim.MapStringAny(itemValue)
			switch uvim.StringValue(item["msgtype"]) {
			case "text":
				if content := uvim.StringValue(uvim.MapStringAny(item["text"])["content"]); content != "" {
					parts = append(parts, content)
				}
			case "image":
				parts = append(parts, "[Image]")
			case "file":
				parts = append(parts, "[File]")
			case "video":
				parts = append(parts, "[Video]")
			}
		}
		return strings.Join(parts, "\n")
	case "image":
		return "[Image]"
	case "file":
		return "[File]"
	case "video":
		return "[Video]"
	default:
		return ""
	}
}

func (p *Provider) messageResources(body map[string]any, msgType string) []uvim.ResourceRef {
	switch msgType {
	case "mixed":
		items, _ := uvim.MapStringAny(body["mixed"])["msg_item"].([]any)
		var refs []uvim.ResourceRef
		for _, itemValue := range items {
			item := uvim.MapStringAny(itemValue)
			if ref, ok := resourceFromBody(item, uvim.StringValue(item["msgtype"]), p.ID(), p.ConnectorID()); ok {
				refs = append(refs, ref)
			}
		}
		return refs
	case "image", "file", "video":
		if ref, ok := resourceFromBody(body, msgType, p.ID(), p.ConnectorID()); ok {
			return []uvim.ResourceRef{ref}
		}
	}
	return nil
}

func resourceFromBody(body map[string]any, msgType, provider, connector string) (uvim.ResourceRef, bool) {
	payload := uvim.MapStringAny(body[msgType])
	if len(payload) == 0 {
		return uvim.ResourceRef{}, false
	}
	ref := uvim.ResourceRef{
		Provider:  provider,
		Connector: connector,
		Kind:      msgType,
		Name:      uvim.FirstNonEmpty(uvim.StringValue(payload["file_name"]), uvim.StringValue(payload["filename"]), uvim.StringValue(payload["name"])),
		Key:       uvim.FirstNonEmpty(uvim.StringValue(payload["media_id"]), uvim.StringValue(payload["file_id"]), uvim.StringValue(payload["fileid"])),
		URL:       uvim.FirstNonEmpty(uvim.StringValue(payload["url"]), uvim.StringValue(payload["download_url"])),
		Secret:    uvim.StringValue(payload["aeskey"]),
	}
	return ref, ref.URL != ""
}

func elementsFromTextAndResources(text string, refs []uvim.ResourceRef) []uvim.Element {
	var out []uvim.Element
	if strings.TrimSpace(text) != "" {
		out = append(out, uvim.Text(text))
	}
	for _, ref := range refs {
		out = append(out, uvim.File(ref.Sanitized()))
	}
	return out
}

func (p *Provider) waitAuth(ctx context.Context, conn WSConn, reqID string) error {
	deadline := uvim.OptionalDeadline(p.now(), p.config.AckTimeout)
	for {
		if err := conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		msgType, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("auth read: %w", err)
		}
		if msgType != websocket.TextMessage && msgType != websocket.BinaryMessage {
			continue
		}
		var ack frame
		if err := json.Unmarshal(raw, &ack); err != nil {
			continue
		}
		if ack.Headers.ReqID != reqID {
			continue
		}
		if ack.ErrCode != nil && *ack.ErrCode != 0 {
			return fmt.Errorf("wecom auth failed: errcode=%d errmsg=%q", *ack.ErrCode, ack.ErrMsg)
		}
		return nil
	}
}

func (p *Provider) heartbeat(ctx context.Context, conn WSConn, writeMu *sync.Mutex, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(p.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reqID := p.reqID(cmdHeartbeat)
			err := p.sendWithAck(ctx, conn, writeMu, reqID, frame{Cmd: cmdHeartbeat, Headers: headers{ReqID: reqID}})
			if err != nil {
				if ctx.Err() == nil {
					p.config.Logger.Warn("wecom heartbeat failed", "err", err.Error())
				}
				_ = conn.Close()
				return
			}
		}
	}
}

func (p *Provider) sendWithAck(ctx context.Context, conn WSConn, writeMu *sync.Mutex, reqID string, out frame) error {
	_, err := p.requestWithAck(ctx, conn, writeMu, reqID, out)
	return err
}

func (p *Provider) requestWithAck(ctx context.Context, conn WSConn, writeMu *sync.Mutex, reqID string, out frame) (frame, error) {
	if p.config.AckTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.config.AckTimeout)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return frame{}, err
	}
	ch := p.registerPending(reqID)
	defer p.unregisterPending(reqID)
	// An unbounded socket write still has to honor the caller's cancellation.
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	err := p.writeFrame(conn, writeMu, out)
	stopClose()
	if err != nil {
		return frame{}, err
	}
	select {
	case <-ctx.Done():
		return frame{}, ctx.Err()
	case ack := <-ch:
		if ack.ErrCode != nil && *ack.ErrCode != 0 {
			sendErr := fmt.Errorf("wecom ack error: errcode=%d errmsg=%q", *ack.ErrCode, ack.ErrMsg)
			return frame{}, uvim.NewProviderSendFailure(uvim.SendFailure{
				Category:      uvim.SendFailureProviderRejected,
				DeliveryState: uvim.DeliveryRejected,
				ProviderCode:  strconv.Itoa(*ack.ErrCode),
			}, sendErr.Error(), sendErr)
		}
		return ack, nil
	}
}

func (p *Provider) writeFrame(conn WSConn, writeMu *sync.Mutex, out frame) error {
	raw, err := json.Marshal(out)
	if err != nil {
		return err
	}
	writeMu.Lock()
	defer writeMu.Unlock()
	if err := conn.SetWriteDeadline(uvim.OptionalDeadline(p.now(), p.config.WriteTimeout)); err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, raw)
}

func (p *Provider) waitActive(ctx context.Context) (WSConn, *sync.Mutex, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if conn, writeMu := p.active(); conn != nil && writeMu != nil {
		return conn, writeMu, nil
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("wecom active connection unavailable: %w", ctx.Err())
		case <-ticker.C:
			if conn, writeMu := p.active(); conn != nil && writeMu != nil {
				return conn, writeMu, nil
			}
		}
	}
}

func (p *Provider) active() (WSConn, *sync.Mutex) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	return p.activeConn, p.activeWrite
}

func (p *Provider) setActive(conn WSConn, writeMu *sync.Mutex) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	p.activeConn = conn
	p.activeWrite = writeMu
}

func (p *Provider) clearActive(conn WSConn) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	if p.activeConn == conn {
		p.activeConn = nil
		p.activeWrite = nil
	}
	p.state = "stopped"
}

func (p *Provider) setState(state string) {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	p.state = state
}

func (p *Provider) registerPending(reqID string) chan frame {
	ch := make(chan frame, 1)
	p.mu.Lock()
	p.pending[reqID] = ch
	p.mu.Unlock()
	return ch
}

func (p *Provider) unregisterPending(reqID string) {
	p.mu.Lock()
	delete(p.pending, reqID)
	p.mu.Unlock()
}

func (p *Provider) resolvePending(reqID string, in frame) {
	if reqID == "" {
		return
	}
	p.mu.Lock()
	ch := p.pending[reqID]
	p.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- in:
	default:
	}
}

func (p *Provider) failAllPending(reason string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	code := -1
	for reqID, ch := range p.pending {
		select {
		case ch <- frame{Headers: headers{ReqID: reqID}, ErrCode: &code, ErrMsg: reason}:
		default:
		}
		delete(p.pending, reqID)
	}
}

func (p *Provider) withReplyLock(ctx context.Context, reqID string, fn func() error) error {
	lock, err := p.acquireReplyLock(ctx, reqID)
	if err != nil {
		return err
	}
	defer p.releaseReplyLock(reqID, lock)
	return fn()
}

func (p *Provider) acquireReplyLock(ctx context.Context, reqID string) (*replyLock, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if reqID == "" {
		reqID = "default"
	}
	p.replyLocksMu.Lock()
	lock := p.replyLocks[reqID]
	if lock == nil {
		lock = &replyLock{ch: make(chan struct{}, 1)}
		lock.ch <- struct{}{}
		p.replyLocks[reqID] = lock
	}
	lock.refs++
	p.replyLocksMu.Unlock()
	select {
	case <-lock.ch:
		return lock, nil
	case <-ctx.Done():
		p.releaseReplyLockRef(reqID, lock)
		return nil, ctx.Err()
	}
}

func (p *Provider) releaseReplyLock(reqID string, lock *replyLock) {
	if reqID == "" {
		reqID = "default"
	}
	if lock == nil {
		return
	}
	select {
	case lock.ch <- struct{}{}:
	default:
	}
	p.releaseReplyLockRef(reqID, lock)
}

func (p *Provider) releaseReplyLockRef(reqID string, lock *replyLock) {
	if reqID == "" {
		reqID = "default"
	}
	if lock == nil {
		return
	}
	p.replyLocksMu.Lock()
	lock.refs--
	if lock.refs == 0 {
		delete(p.replyLocks, reqID)
	}
	p.replyLocksMu.Unlock()
}

func (p *Provider) reqID(prefix string) string {
	return uvim.NewID(prefix)
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
