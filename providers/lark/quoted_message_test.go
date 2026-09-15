package lark

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/server"
)

// Runs the real WebSocket adapter and Hub with a local Feishu API stub.
func TestQuotedMessageResourcesThroughLarkTransportStub(t *testing.T) {
	for _, scenario := range []string{"file", "folder", "folder-failed", "text", "denied", "deleted", "wrong-chat", "wrong-message", "download-failed", "unaddressed"} {
		t.Run(scenario, func(t *testing.T) {
			var api *httptest.Server
			api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/callback/ws/endpoint":
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(api.URL, "http:", "ws:", 1) + "/ws?service_id=1"}})
				case "/ws":
					conn, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer conn.Close()
					mentions := []any{map[string]any{"key": "@bot", "id": map[string]any{"open_id": "bot"}}}
					if scenario == "unaddressed" {
						mentions = nil
					}
					raw, _ := json.Marshal(map[string]any{"header": map[string]any{"event_type": "im.message.receive_v1"}, "event": map[string]any{"message": map[string]any{
						"message_id": "current", "parent_id": "parent", "root_id": "unrelated-root", "chat_id": "chat", "chat_type": "group", "message_type": "text", "content": `{"text":"analyze this log"}`, "mentions": mentions,
					}}})
					frame := &wsFrame{Method: frameMethodData, Headers: []frameHeader{{Key: frameHeaderMessageID, Value: "current"}}, Payload: raw}
					if err := conn.WriteMessage(websocket.BinaryMessage, frame.marshal()); err != nil {
						t.Error(err)
						return
					}
					conn.SetReadDeadline(time.Now().Add(3 * time.Second))
					if _, _, err := conn.ReadMessage(); err != nil {
						t.Error(err)
					}
				case "/open-apis/auth/v3/tenant_access_token/internal":
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
				case "/open-apis/im/v1/chats/chat":
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"name": "group"}})
				case "/open-apis/im/v1/messages/parent":
					if scenario == "unaddressed" {
						t.Error("fetched quote for an unaddressed group message")
					}
					if req.Header.Get("Authorization") != "Bearer token" {
						t.Error("missing bot identity")
					}
					if scenario == "denied" {
						json.NewEncoder(w).Encode(map[string]any{"code": 99991672, "msg": "private diagnostic"})
						return
					}
					kind, content := "file", `{"file_key":"log-key","file_name":"device.log"}`
					if strings.HasPrefix(scenario, "folder") {
						kind = "folder"
					}
					if scenario == "text" {
						kind, content = "text", `{"text":"fatal: tracking failed"}`
					}
					chat, id := "chat", "parent"
					if scenario == "wrong-chat" {
						chat = "other-chat"
					}
					if scenario == "wrong-message" {
						id = "other-message"
					}
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{
						"message_id": id, "chat_id": chat, "msg_type": kind, "deleted": scenario == "deleted", "body": map[string]any{"content": content},
					}}}})
				case "/open-apis/im/v1/messages/parent/resources/log-key":
					if scenario != "file" && scenario != "folder" && scenario != "folder-failed" && scenario != "download-failed" {
						t.Error("downloaded an unverified quote")
					}
					if scenario == "folder-failed" {
						w.WriteHeader(http.StatusInternalServerError)
						json.NewEncoder(w).Encode(map[string]any{"code": 40009, "msg": "internal server error"})
						return
					}
					if scenario == "download-failed" {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					io.WriteString(w, "fatal: tracking failed\n")
				default:
					t.Errorf("unexpected request %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer api.Close()
			store := &uvim.ResourceStore{Dir: t.TempDir()}
			provider, err := New(Config{AppID: "app", AppSecret: "secret", BotOpenID: "bot", BaseURL: api.URL, CallbackBaseURL: api.URL, ResourceStore: store})
			if err != nil {
				t.Fatal(err)
			}
			log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			hub := server.NewHub(uvim.NewProviderRegistry(provider), log, store)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_ = provider.Run(ctx, hub) // Peer disconnect drains the queued event.
			events, err := log.ReadAfter(t.Context(), 0)
			if err != nil || len(events) != 1 {
				t.Fatalf("events=%+v err=%v", events, err)
			}
			event := events[0]
			if event.Message.Text != "analyze this log" || event.Referrer.MessageID != "current" || event.Referrer.ParentMessageID != "parent" {
				t.Fatalf("changed instruction/reply authority: %+v", event)
			}
			if scenario == "unaddressed" {
				if len(event.Message.Resources) != 0 {
					t.Fatal("unexpected quote resources")
				}
				return
			}
			if len(event.Message.Resources) == 0 {
				t.Fatal("quoted message context missing from emitted resources")
			}
			var contents string
			var failures []string
			for _, ref := range event.Message.Resources {
				if ref.Error != "" {
					failures = append(failures, ref.Error)
					continue
				}
				file, _, err := store.Open(ref.InternalURL)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(file)
				file.Close()
				if err != nil {
					t.Fatal(err)
				}
				contents += ref.Name + "\n" + string(data)
			}
			if strings.HasPrefix(scenario, "folder") && (!strings.Contains(contents, "Message type: folder") || !strings.Contains(contents, "ZIP archive")) {
				t.Fatalf("folder context missing: %q", contents)
			}
			switch scenario {
			case "file", "folder", "text":
				if !strings.Contains(contents, "fatal: tracking failed") || !strings.Contains(contents, "parent") || len(failures) != 0 {
					t.Fatalf("contents=%q failures=%v", contents, failures)
				}
				if scenario == "file" && !strings.Contains(contents, "device.log") {
					t.Fatal("log attachment missing")
				}
			default:
				if len(failures) == 0 || strings.Contains(contents, "fatal:") || strings.Contains(strings.Join(failures, ""), "private diagnostic") {
					t.Fatalf("contents=%q failures=%v", contents, failures)
				}
				want := map[string]string{"denied": "quoted_message_lookup_failed: code=99991672", "deleted": "quoted_message_deleted", "wrong-chat": "quoted_message_identity_mismatch", "wrong-message": "quoted_message_identity_mismatch", "download-failed": "download_failed", "folder-failed": "download_failed"}[scenario]
				if len(failures) != 1 || failures[0] != want {
					t.Fatalf("failure=%v want=%q", failures, want)
				}
			}
		})
	}
}
