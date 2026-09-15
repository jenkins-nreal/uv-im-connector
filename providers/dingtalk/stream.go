package dingtalk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	streamsdk "github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
)

type streamClient interface {
	RegisterChatBotCallbackRouter(chatbot.IChatBotMessageHandler)
	Start(context.Context) error
	Close()
}

type Provider struct {
	config Config
	base   *httpchannel.Provider

	stateMu sync.Mutex
	state   string

	newStreamClient func(clientID, clientSecret string) streamClient
}

var _ uvim.WebhookProvider = (*Provider)(nil)

func newProvider(config Config, base *httpchannel.Provider) *Provider {
	return &Provider{
		config: config,
		base:   base,
		state:  "configured",
		newStreamClient: func(clientID, clientSecret string) streamClient {
			return streamsdk.NewStreamClient(streamsdk.WithAppCredential(
				streamsdk.NewAppCredentialConfig(clientID, clientSecret),
			))
		},
	}
}

func (p *Provider) ID() string { return p.base.ID() }

func (p *Provider) ConnectorID() string { return p.base.ConnectorID() }

func (p *Provider) Capabilities() uvim.Capabilities { return p.base.Capabilities() }

func (p *Provider) streamEnabled() bool { return p.config.ClientID != "" }

func (p *Provider) Run(ctx context.Context, sink uvim.EventSink) error {
	if !p.streamEnabled() {
		return p.base.Run(ctx, sink)
	}

	client := p.newStreamClient(p.config.ClientID, p.config.ClientSecret)
	client.RegisterChatBotCallbackRouter(func(handlerCtx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("dingtalk stream: encode callback: %w", err)
		}
		event, ok, err := Decode(raw, httpchannel.Config{
			ProviderID:  p.ID(),
			ConnectorID: p.ConnectorID(),
			BaseURL:     p.config.BaseURL,
		})
		if err != nil {
			return nil, fmt.Errorf("dingtalk stream: decode callback: %w", err)
		}
		if !ok {
			return nil, nil
		}
		if err := sink.Emit(handlerCtx, event); err != nil {
			return nil, fmt.Errorf("dingtalk stream: emit callback: %w", err)
		}
		p.setState("event")
		return nil, nil
	})

	p.setState("connecting")
	if err := client.Start(ctx); err != nil {
		p.setState("error")
		return fmt.Errorf("dingtalk stream: start: %w", err)
	}
	defer client.Close()
	p.setState("connected")

	<-ctx.Done()
	return nil
}

func (p *Provider) Send(ctx context.Context, msg uvim.OutboundMessage) (uvim.SendResult, error) {
	return p.base.Send(ctx, msg)
}

func (p *Provider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	if strings.TrimSpace(req.Resource.Key) == "" {
		return p.base.Download(ctx, req)
	}
	robotCode := strings.TrimSpace(req.Resource.Private["robot_code"])
	if robotCode == "" {
		return req.Resource, fmt.Errorf("dingtalk download: robot code is required for downloadCode")
	}
	if strings.TrimSpace(p.config.ClientID) == "" || strings.TrimSpace(p.config.ClientSecret) == "" {
		return req.Resource, fmt.Errorf("dingtalk download: stream credentials are required for downloadCode")
	}
	token, err := p.accessToken(ctx)
	if err != nil {
		return req.Resource, err
	}
	downloadURL, err := p.downloadURL(ctx, token, robotCode, req.Resource.Key)
	if err != nil {
		return req.Resource, err
	}
	req.Resource.URL = downloadURL
	req.Resource.Key = ""
	return p.base.Download(ctx, req)
}

func (p *Provider) accessToken(ctx context.Context) (string, error) {
	raw, err := json.Marshal(map[string]string{
		"appKey":    p.config.ClientID,
		"appSecret": p.config.ClientSecret,
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBaseURL()+"/v1.0/oauth2/accessToken", bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	body, err := doDingTalkJSON(req)
	if err != nil {
		return "", fmt.Errorf("dingtalk access token: %w", err)
	}
	var response struct {
		AccessToken string `json:"accessToken"`
		Code        string `json:"code"`
		Message     string `json:"message"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("dingtalk access token: decode response: %w", err)
	}
	if response.AccessToken == "" {
		return "", fmt.Errorf("dingtalk access token: empty token code=%q message=%q", response.Code, response.Message)
	}
	return response.AccessToken, nil
}

func (p *Provider) downloadURL(ctx context.Context, token, robotCode, downloadCode string) (string, error) {
	raw, err := json.Marshal(map[string]string{
		"downloadCode": downloadCode,
		"robotCode":    robotCode,
	})
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.apiBaseURL()+"/v1.0/robot/messageFiles/download", bytes.NewReader(raw))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	body, err := doDingTalkJSON(req)
	if err != nil {
		return "", fmt.Errorf("dingtalk message file: %w", err)
	}
	var response struct {
		DownloadURL string `json:"downloadUrl"`
		Code        string `json:"code"`
		Message     string `json:"message"`
		Result      struct {
			DownloadURL string `json:"downloadUrl"`
		} `json:"result"`
		Data struct {
			DownloadURL string `json:"downloadUrl"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("dingtalk message file: decode response: %w", err)
	}
	downloadURL := firstNonEmpty(response.DownloadURL, response.Result.DownloadURL, response.Data.DownloadURL)
	if downloadURL == "" {
		return "", fmt.Errorf("dingtalk message file: empty download URL code=%q message=%q", response.Code, response.Message)
	}
	return downloadURL, nil
}

func (p *Provider) apiBaseURL() string {
	base := strings.TrimRight(strings.TrimSpace(p.config.BaseURL), "/")
	if base == "" || strings.Contains(base, "oapi.dingtalk.com") {
		return "https://api.dingtalk.com"
	}
	return base
}

func doDingTalkJSON(req *http.Request) ([]byte, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (p *Provider) Health(ctx context.Context) uvim.Health {
	if !p.streamEnabled() {
		return p.base.Health(ctx)
	}
	p.stateMu.Lock()
	state := p.state
	p.stateMu.Unlock()
	return uvim.Health{
		Provider:     p.ID(),
		Connector:    p.ConnectorID(),
		State:        state,
		CheckedAt:    time.Now().UTC(),
		Capabilities: p.Capabilities(),
	}
}

func (p *Provider) ServeWebhook(w http.ResponseWriter, req *http.Request, sink uvim.EventSink) {
	p.base.ServeWebhook(w, req, sink)
}

func (p *Provider) setState(state string) {
	p.stateMu.Lock()
	p.state = state
	p.stateMu.Unlock()
}
