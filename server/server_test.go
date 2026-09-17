package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/client"
	"github.com/hengshi/uv-im-connector/providers/lark"
	"github.com/hengshi/uv-im-connector/providers/line"
	"github.com/hengshi/uv-im-connector/providers/memory"
	"github.com/hengshi/uv-im-connector/providers/slack"
	"github.com/hengshi/uv-im-connector/providers/wecom"
)

func TestUploadValidLongFilename(t *testing.T) {
	for _, tc := range []struct {
		label, name, mime, content, contentType string
	}{
		{"234-txt", strings.Repeat("a", 230) + ".txt", "", "hello", "text/plain"},
		{"255-txt", strings.Repeat("a", 251) + ".txt", "", "hello", "text/plain"},
		{"255-noext-plain", strings.Repeat("a", 255), "text/plain", "hello", "text/plain"},
		{"short-noext-json", "document", "application/json", `{}`, "application/json"},
		{"255-noext-json", strings.Repeat("a", 255), "application/json", `{}`, "application/json"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			name, data := tc.name, []byte(tc.content)
			// Establish that the original name is legal on this filesystem.
			if err := os.WriteFile(filepath.Join(t.TempDir(), name), data, 0o600); err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			hub := NewHub(nil, nil, &uvim.ResourceStore{Dir: dir})
			api := httptest.NewServer(hub.Handler())
			defer api.Close()
			c := client.New(api.URL)
			ref, err := c.Upload(context.Background(), uvim.ResourceRef{Name: name, MIME: tc.mime}, data)
			if err != nil {
				t.Fatal(err)
			}
			// Reopen using a fresh store: lookup must not depend on in-memory metadata.
			api.Close()
			restarted := httptest.NewServer(NewHub(nil, nil, &uvim.ResourceStore{Dir: dir}).Handler())
			defer restarted.Close()
			c = client.New(restarted.URL)
			resp, err := c.ResolveInternalURL(context.Background(), ref.InternalURL)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil || !bytes.Equal(got, data) || ref.Name != name || ref.MIME != tc.mime || !strings.HasPrefix(resp.Header.Get("Content-Type"), tc.contentType) {
				t.Fatalf("round trip: name=%q body=%q type=%q err=%v", ref.Name, got, resp.Header.Get("Content-Type"), err)
			}
		})
	}
}

func TestWebSocketHandshakeCancellation(t *testing.T) {
	for _, entry := range []string{"wecom", "lark", "client"} {
		t.Run(entry, func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/callback/ws/endpoint" {
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": "ws://" + r.Host + "/blocked?service_id=1"}})
					return
				}
				close(started)
				<-release // Deliberately never complete the upgrade until cleanup.
				http.Error(w, "closed", http.StatusServiceUnavailable)
			}))
			defer api.Close()
			defer close(release)
			ctx, cancel := context.WithCancel(context.Background()) // No deadline.
			defer cancel()
			var run func(context.Context) error
			if entry == "client" {
				run = func(ctx context.Context) error { return client.New(api.URL).WatchEvents(ctx, 0, nil) }
			} else {
				var p uvim.Provider
				var err error
				if entry == "wecom" {
					p, err = wecom.New(wecom.Config{BotID: "bot", Secret: "secret", WSURL: strings.Replace(api.URL, "http", "ws", 1)})
				} else {
					p, err = lark.New(lark.Config{AppID: "app", AppSecret: "secret", BotOpenID: "bot", CallbackBaseURL: api.URL})
				}
				if err != nil {
					t.Fatal(err)
				}
				run = NewHub(uvim.NewProviderRegistry(p), nil, nil).RunProviders
			}
			done := make(chan error, 1)
			go func() { done <- run(ctx) }()
			select {
			case <-started:
			case err := <-done:
				t.Fatalf("returned before handshake: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("handshake did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancel result: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancellation did not interrupt pending upgrade")
			}
		})
	}
}

func TestUploadBeyondLegacyResourceSizeLimit(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	hub := NewHub(nil, nil, store)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	c := client.New(server.URL)
	data := bytes.Repeat([]byte("x"), 100*1024*1024+1)
	ref, err := c.Upload(context.Background(), uvim.ResourceRef{Name: "report.txt"}, data)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.ResolveInternalURL(context.Background(), ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(got, data) || ref.SizeBytes != int64(len(data)) {
		t.Fatalf("upload round trip: size=%d metadata=%d err=%v", len(got), ref.SizeBytes, err)
	}
}

func TestHubEventsAndOutbound(t *testing.T) {
	dir := t.TempDir()
	log, err := uvim.NewEventLog(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-1", Type: uvim.EventMessageCreate, Provider: "test", Message: uvim.Message{ID: "m1", Text: "hello"}}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var events struct {
		Events []uvim.Event `json:"events"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events.Events) != 1 || events.Events[0].Message.Text != "hello" {
		t.Fatalf("events = %+v", events.Events)
	}

	raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "test", ChannelID: "c1", Text: "reply"})
	resp, err = http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 1 || provider.Sent()[0].Text != "reply" {
		t.Fatalf("sent = %+v", provider.Sent())
	}
}

func TestHubRoutesOutboundByConnector(t *testing.T) {
	first := memory.NewConnector("lark", "main")
	second := memory.NewConnector("lark", "sandbox")
	hub := NewHub(uvim.NewProviderRegistry(first, second), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "lark", Connector: "sandbox", ChannelID: "c1", Text: "reply"})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(first.Sent()) != 0 {
		t.Fatalf("main connector sent = %+v", first.Sent())
	}
	if len(second.Sent()) != 1 {
		t.Fatalf("sandbox connector sent = %+v", second.Sent())
	}
}

func TestHubRejectsInvalidExplicitTarget(t *testing.T) {
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{
		Provider: "test",
		Target:   &uvim.OutboundTarget{ID: "x", Kind: "invalid"},
		Text:     "hello",
	})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 0 {
		t.Fatalf("provider sent = %+v", provider.Sent())
	}
}

func TestHubRejectsInvalidLegacyChannelType(t *testing.T) {
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{
		Provider:    "test",
		ChannelID:   "legacy",
		ChannelType: "bogus",
		Text:        "hello",
	})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 0 {
		t.Fatalf("provider sent = %+v", provider.Sent())
	}
}

func TestHubRejectsLegacyTargetWithoutRecipient(t *testing.T) {
	provider := memory.New("test")
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{
		Provider:    "test",
		ChannelType: uvim.ChannelDirect,
		Text:        "hello",
	})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if len(provider.Sent()) != 0 {
		t.Fatalf("provider sent = %+v", provider.Sent())
	}
}

func TestHubReturnsProviderSendFailureDetail(t *testing.T) {
	provider := &downloadProvider{
		id:        "test",
		connector: "main",
		sendErr: uvim.NewProviderSendError(
			"provider rejected message: invalid recipient",
			errors.New("internal request failed at a URL containing a secret"),
		),
	}
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "test", ChannelID: "c1", Text: "hello"})
	resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body struct {
		Error   string           `json:"error"`
		Detail  string           `json:"detail"`
		Failure uvim.SendFailure `json:"failure"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error != "provider_send_failed" || body.Detail != "provider rejected message: invalid recipient" {
		t.Fatalf("body = %+v", body)
	}
	if body.Failure.Category != uvim.SendFailureUnknown || body.Failure.DeliveryState != uvim.DeliveryUnknown {
		t.Fatalf("failure = %+v", body.Failure)
	}
}

func TestHubLogsCredentialSafeProviderSendFailureDetail(t *testing.T) {
	tests := []struct {
		name       string
		sendErr    error
		wantReason string
	}{
		{
			name:       "marked private diagnostic",
			sendErr:    uvim.NewProviderSendLogError("decode lark send response: unexpected EOF", errors.New("access_token=secret")),
			wantReason: "decode lark send response: unexpected EOF",
		},
		{
			name:       "unmarked error",
			sendErr:    errors.New("POST https://user:password@example.test/private?access_token=secret failed"),
			wantReason: "unmarked provider error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs bytes.Buffer
			previousLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(previousLogger) })

			provider := &downloadProvider{id: "lark", connector: "main", sendErr: tt.sendErr}
			hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
			server := httptest.NewServer(hub.Handler())
			defer server.Close()

			raw, _ := json.Marshal(uvim.OutboundMessage{Provider: "lark", Connector: "main", ChannelID: "c1", Text: "hello"})
			resp, err := http.Post(server.URL+"/v1/message.create", "application/json", bytes.NewReader(raw))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("status = %d", resp.StatusCode)
			}
			var body struct {
				Error  string `json:"error"`
				Detail string `json:"detail"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Error != "provider_send_failed" || body.Detail != "" {
				t.Fatalf("body = %+v", body)
			}
			got := logs.String()
			if !strings.Contains(got, `provider=lark connector=main reason="`+tt.wantReason+`"`) || strings.Contains(got, "internal_error") || strings.Contains(got, "secret") {
				t.Fatalf("logs = %q", got)
			}
		})
	}
}

func TestHubDownloadsInboundResourcesBeforeEventLog(t *testing.T) {
	dir := t.TempDir()
	provider := &downloadProvider{id: "test", connector: "main"}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	err := hub.Emit(context.Background(), uvim.Event{
		ID:        "evt-1",
		Type:      uvim.EventMessageCreate,
		Provider:  "test",
		Connector: "main",
		Message: uvim.Message{
			ID:   "m1",
			Text: "file",
			Resources: []uvim.ResourceRef{{
				Provider:  "test",
				Connector: "main",
				Kind:      uvim.ElementFile,
				Name:      "secret.txt",
				Key:       "provider-key",
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || len(events[0].Message.Resources) != 1 {
		t.Fatalf("events = %+v", events)
	}
	ref := events[0].Message.Resources[0]
	if ref.InternalURL == "" || ref.Key != "" || ref.Metadata != nil {
		t.Fatalf("resource was not resolved and sanitized: %+v", ref)
	}
}

func TestHubAuthProtectsAPIs(t *testing.T) {
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	hub.SetAuthToken("token")
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	resp, err := http.Get(server.URL + "/v1/meta")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without token = %d", resp.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/meta", nil)
	req.Header.Set("Authorization", "Bearer token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with token = %d", resp.StatusCode)
	}
}

func TestHubMetaIncludesServiceAndProtocolVersion(t *testing.T) {
	provider := memory.NewConnector("memory", "main")
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/meta")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var meta uvim.ServiceMeta
	if err := json.NewDecoder(resp.Body).Decode(&meta); err != nil {
		t.Fatal(err)
	}
	if meta.Service != uvim.ServiceName || meta.ProtocolVersion != uvim.ProtocolVersion || meta.ConnectorVersion == "" {
		t.Fatalf("meta = %+v", meta)
	}
	if len(meta.Providers) != 1 || meta.Providers[0].Provider != "memory" || meta.Providers[0].Connector != "main" {
		t.Fatalf("providers = %+v", meta.Providers)
	}
}

func TestHubWebhookRoutesToProviderAndStoresEvent(t *testing.T) {
	dir := t.TempDir()
	provider, err := slack.New(slack.Config{WebhookSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	hub.SetAuthToken("api-token")
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"type":"event_callback","event":{"type":"message","user":"u1","channel":"c1","text":"hello","ts":"m1"}}`
	resp, err := http.Post(server.URL+"/v1/webhook/slack/slack", "application/json", strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without provider secret = %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/webhook/slack/slack", strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-UV-Webhook-Secret", "secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with provider secret = %d", resp.StatusCode)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Provider != "slack" || events[0].Message.Text != "hello" {
		t.Fatalf("events = %+v", events)
	}
}

func TestHubWebhookRejectsMissingProviderSecret(t *testing.T) {
	provider, err := slack.New(slack.Config{})
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(uvim.NewProviderRegistry(provider), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"type":"event_callback","event":{"type":"message","user":"u1","channel":"c1","text":"hello","ts":"m1"}}`
	resp, err := http.Post(server.URL+"/v1/webhook/slack/slack", "application/json", strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without configured provider secret = %d", resp.StatusCode)
	}
}

func TestHubWebhookEmitsBatchedProviderEvents(t *testing.T) {
	dir := t.TempDir()
	provider, err := line.New(line.Config{WebhookSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	log := mustEventLog(t, filepath.Join(dir, "events.jsonl"))
	hub := NewHub(uvim.NewProviderRegistry(provider), log, &uvim.ResourceStore{Dir: filepath.Join(dir, "resources")})
	server := httptest.NewServer(hub.Handler())
	defer server.Close()

	raw := `{"events":[{"replyToken":"r1","source":{"type":"user","userId":"u1"},"message":{"id":"m1","type":"text","text":"one"}},{"replyToken":"r2","source":{"type":"user","userId":"u2"},"message":{"id":"m2","type":"text","text":"two"}}]}`
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/webhook/line/line", strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-UV-Webhook-Secret", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	events, err := log.ReadAfter(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Message.Text != "one" || events[1].Message.Text != "two" {
		t.Fatalf("events = %+v", events)
	}
}

func TestHubClosesSubscriberOnOverflow(t *testing.T) {
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), &uvim.ResourceStore{Dir: t.TempDir()})
	ch := make(chan uvim.Event, 1)
	hub.mu.Lock()
	hub.subscribers[ch] = struct{}{}
	hub.mu.Unlock()
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-1", Type: uvim.EventMessageCreate, Provider: "test", Message: uvim.Message{ID: "m1"}}); err != nil {
		t.Fatal(err)
	}
	if err := hub.Emit(context.Background(), uvim.Event{ID: "evt-2", Type: uvim.EventMessageCreate, Provider: "test", Message: uvim.Message{ID: "m2"}}); err != nil {
		t.Fatal(err)
	}
	hub.mu.Lock()
	_, stillSubscribed := hub.subscribers[ch]
	hub.mu.Unlock()
	if stillSubscribed {
		t.Fatal("subscriber remained registered after overflow")
	}
	<-ch
	if _, ok := <-ch; ok {
		t.Fatal("subscriber channel is not closed after overflow")
	}
}

func TestClientResolveInternalURL(t *testing.T) {
	dir := t.TempDir()
	store := &uvim.ResourceStore{Dir: dir}
	ref, err := store.Save(context.Background(), strings.NewReader("hello"), uvim.ResourceRef{ID: "r1", Kind: uvim.ElementFile, Name: "hello.txt"})
	if err != nil {
		t.Fatal(err)
	}
	hub := NewHub(uvim.NewProviderRegistry(memory.New("test")), mustEventLog(t, ""), store)
	server := httptest.NewServer(hub.Handler())
	defer server.Close()
	resp, err := client.New(server.URL).ResolveInternalURL(context.Background(), ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("body = %q", string(data))
	}
}

func mustEventLog(t *testing.T, path string) *uvim.EventLog {
	t.Helper()
	log, err := uvim.NewEventLog(path)
	if err != nil {
		t.Fatal(err)
	}
	return log
}

type downloadProvider struct {
	id        string
	connector string
	sendErr   error
}

func (p *downloadProvider) ID() string          { return p.id }
func (p *downloadProvider) ConnectorID() string { return p.connector }
func (p *downloadProvider) Capabilities() uvim.Capabilities {
	return uvim.Capabilities{
		Inbound:          true,
		Outbound:         true,
		ReplyMessage:     true,
		ProactiveGroup:   true,
		TargetKinds:      []string{uvim.TargetConversation},
		DownloadResource: true,
		ResourceKinds:    []string{uvim.ElementFile},
	}
}
func (p *downloadProvider) Run(ctx context.Context, sink uvim.EventSink) error {
	<-ctx.Done()
	return ctx.Err()
}
func (p *downloadProvider) Send(context.Context, uvim.OutboundMessage) (uvim.SendResult, error) {
	if p.sendErr != nil {
		return uvim.SendResult{}, p.sendErr
	}
	return uvim.SendResult{Provider: p.id, Connector: p.connector, Time: time.Now().UTC()}, nil
}
func (p *downloadProvider) Download(ctx context.Context, req uvim.ResourceDownloadRequest) (uvim.ResourceRef, error) {
	store := &uvim.ResourceStore{Dir: req.Dir}
	ref := req.Resource
	return store.Save(ctx, strings.NewReader("downloaded"), ref)
}
func (p *downloadProvider) Health(context.Context) uvim.Health {
	return uvim.Health{Provider: p.id, Connector: p.connector, State: "ok", CheckedAt: time.Now().UTC(), Capabilities: p.Capabilities()}
}
