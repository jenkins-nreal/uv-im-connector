package dingtalk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/providers/httpchannel"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
)

func TestDecodeUsesSessionWebhookExpiry(t *testing.T) {
	expiresAt := time.Date(2026, 7, 16, 11, 30, 0, 0, time.UTC)
	event, ok, err := Decode([]byte(`{
  "msgId": "m1",
  "msgtype": "text",
  "senderStaffId": "u1",
  "conversationId": "g1",
  "conversationType": "2",
  "sessionWebhook": "https://oapi.dingtalk.com/robot/sendBySession?session=secret",
  "sessionWebhookExpiredTime": `+formatUnixMilli(expiresAt)+`,
  "text": {"content": "hello"}
}`), httpchannel.Config{BaseURL: "https://oapi.dingtalk.com", ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Referrer.ExpiresAt == nil || !event.Referrer.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("reply expiry = %v", event.Referrer.ExpiresAt)
	}
}

func formatUnixMilli(value time.Time) string {
	return fmt.Sprintf("%d", value.UnixMilli())
}

func TestNewRejectsPartialStreamCredentials(t *testing.T) {
	for _, config := range []Config{
		{ClientID: "client-id"},
		{ClientSecret: "client-secret"},
	} {
		if _, err := New(config); err == nil {
			t.Fatal("New() accepted a partial DingTalk Stream credential pair")
		}
	}
}

func TestDecodeContentRichTextAndDownloadCodes(t *testing.T) {
	event, ok, err := Decode([]byte(`{
  "msgId": "m-rich",
  "msgtype": "richText",
  "robotCode": "robot-1",
  "senderStaffId": "u1",
  "senderNick": "Ada",
  "conversationId": "g1",
  "conversationType": "2",
  "content": {
    "richText": [
      {"text": "hello"},
      {"type": "picture", "downloadCode": "pic-code", "fileName": "pic.png"},
      {"text": "world"},
      {"type": "file", "downloadCode": "file-code", "fileName": "report.pdf", "fileSize": 12}
    ]
  }
}`), httpchannel.Config{ConnectorID: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Message.Text != "hello\nworld" {
		t.Fatalf("text = %q", event.Message.Text)
	}
	if len(event.Message.Resources) != 2 {
		t.Fatalf("resources = %+v", event.Message.Resources)
	}
	if event.Message.Resources[0].Kind != uvim.ElementImage || event.Message.Resources[0].Key != "pic-code" {
		t.Fatalf("image resource = %+v", event.Message.Resources[0])
	}
	if event.Message.Resources[1].Kind != uvim.ElementFile || event.Message.Resources[1].Key != "file-code" || event.Message.Resources[1].SizeBytes != 12 {
		t.Fatalf("file resource = %+v", event.Message.Resources[1])
	}
	if event.Message.Resources[0].Private["robot_code"] != "robot-1" {
		t.Fatalf("private metadata = %+v", event.Message.Resources[0].Private)
	}
}

func TestDownloadExchangesDownloadCodeForTemporaryURL(t *testing.T) {
	var sawTokenRequest, sawDownloadRequest bool
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			sawTokenRequest = true
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["appKey"] != "client-id" || body["appSecret"] != "client-secret" {
				t.Fatalf("token body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": "access-token"})
		case "/v1.0/robot/messageFiles/download":
			sawDownloadRequest = true
			if got := r.Header.Get("x-acs-dingtalk-access-token"); got != "access-token" {
				t.Fatalf("download token = %q", got)
			}
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body["robotCode"] != "robot-1" || body["downloadCode"] != "download-code" {
				t.Fatalf("download body = %+v", body)
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"downloadUrl": apiURL(r) + "/file"})
		case "/file":
			_, _ = io.WriteString(w, "file bytes")
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer api.Close()

	provider, err := New(Config{BaseURL: api.URL, ClientID: "client-id", ClientSecret: "client-secret"})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := provider.Download(context.Background(), uvim.ResourceDownloadRequest{
		Dir: t.TempDir(),
		Resource: uvim.ResourceRef{
			Provider: "dingtalk",
			Kind:     uvim.ElementImage,
			Name:     "pic.png",
			Key:      "download-code",
			URL:      api.URL + "/legacy-file",
			Private:  map[string]string{"robot_code": "robot-1"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sawTokenRequest || !sawDownloadRequest {
		t.Fatalf("token=%t download=%t", sawTokenRequest, sawDownloadRequest)
	}
	if ref.InternalURL == "" || ref.Key != "" {
		t.Fatalf("downloaded ref = %+v", ref)
	}
}

func apiURL(r *http.Request) string {
	return "http://" + r.Host
}

func TestStreamRunEmitsNormalizedEvent(t *testing.T) {
	provider, err := New(Config{
		ConnectorID:  "main",
		ClientID:     "client-id",
		ClientSecret: "client-secret",
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeStreamClient{
		message: &chatbot.BotCallbackDataModel{
			MsgId:            "m-stream",
			Msgtype:          "text",
			SenderStaffId:    "u1",
			SenderNick:       "Actor",
			ConversationId:   "c1",
			ConversationType: "1",
			SessionWebhook:   "https://oapi.dingtalk.com/robot/sendBySession?session=secret",
			Text:             chatbot.BotCallbackDataTextModel{Content: "/start JARVIS-IM-REAL-E2E-1"},
		},
		afterMessage: cancel,
	}
	provider.newStreamClient = func(string, string) streamClient { return fake }

	var events []uvim.Event
	err = provider.Run(ctx, uvim.EventSinkFunc(func(_ context.Context, event uvim.Event) error {
		events = append(events, event)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	event := events[0]
	if event.Provider != "dingtalk" || event.Connector != "main" || event.ID != "m-stream" {
		t.Fatalf("unexpected event identity: %#v", event)
	}
	if event.User.ID != "u1" || event.Channel.ID != "c1" || event.Message.Text != "/start JARVIS-IM-REAL-E2E-1" {
		t.Fatalf("unexpected normalized event: %#v", event)
	}
	if event.Referrer.ReplyToken == "" {
		t.Fatal("stream event lost session webhook reply token")
	}
}

type fakeStreamClient struct {
	handler      chatbot.IChatBotMessageHandler
	message      *chatbot.BotCallbackDataModel
	afterMessage func()
}

func (f *fakeStreamClient) RegisterChatBotCallbackRouter(handler chatbot.IChatBotMessageHandler) {
	f.handler = handler
}

func (f *fakeStreamClient) Start(ctx context.Context) error {
	if f.handler == nil {
		return fmt.Errorf("chatbot handler was not registered")
	}
	if _, err := f.handler(ctx, f.message); err != nil {
		return err
	}
	if f.afterMessage != nil {
		f.afterMessage()
	}
	return nil
}

func (f *fakeStreamClient) Close() {}
