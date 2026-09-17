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
	for _, scenario := range []string{"auto-file", "auto-other", "file", "folder", "folder-failed", "text", "interactive", "share-chat", "share-user", "system", "merge-forward", "denied", "deleted", "wrong-chat", "wrong-message", "download-failed", "unaddressed"} {
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
					if scenario == "auto-other" {
						mentions = []any{map[string]any{"key": "@bot", "id": map[string]any{"open_id": "other-bot"}}}
					}
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
				case "/open-apis/bot/v3/info":
					if req.Header.Get("Authorization") != "Bearer token" {
						t.Error("missing bot identity")
					}
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "bot": map[string]any{"open_id": "bot"}})
				case "/open-apis/im/v1/chats/chat":
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"name": "group"}})
				case "/open-apis/im/v1/messages/parent":
					if scenario == "unaddressed" || scenario == "auto-other" {
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
					if scenario == "interactive" {
						kind, content = "interactive", `{"elements":[{"tag":"img","img_key":"card-image"}]}`
					}
					if scenario == "share-chat" {
						kind, content = "share_chat", `{"chat_id":"shared-chat"}`
					}
					if scenario == "share-user" {
						kind, content = "share_user", `{"user_id":"shared-user"}`
					}
					if scenario == "system" {
						kind, content = "system", `{}`
					}
					if scenario == "merge-forward" {
						kind, content = "merge_forward", `{}`
					}
					chat, id := "chat", "parent"
					if scenario == "wrong-chat" {
						chat = "other-chat"
					}
					if scenario == "wrong-message" {
						id = "other-message"
					}
					items := []any{map[string]any{
						"message_id": id, "chat_id": chat, "msg_type": kind, "deleted": scenario == "deleted", "body": map[string]any{"content": content},
					}}
					if scenario == "merge-forward" {
						items = append(items, map[string]any{"message_id": "child", "chat_id": "chat", "msg_type": "file", "body": map[string]any{"content": `{"file_key":"forwarded-file","file_name":"forwarded.log"}`}})
					}
					json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": items}})
				case "/open-apis/im/v1/messages/parent/resources/log-key":
					if scenario != "auto-file" && scenario != "file" && scenario != "folder" && scenario != "folder-failed" && scenario != "download-failed" {
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
			botID := "bot"
			if strings.HasPrefix(scenario, "auto-") {
				botID = ""
			}
			provider, err := New(Config{AppID: "app", AppSecret: "secret", BotOpenID: botID, BaseURL: api.URL, CallbackBaseURL: api.URL, ResourceStore: store})
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
			if scenario == "unaddressed" || scenario == "auto-other" {
				if event.Addressed {
					t.Fatal("another mention admitted as bot")
				}
				if len(event.Message.Resources) != 0 {
					t.Fatal("unexpected quote resources")
				}
				return
			}
			if (scenario == "interactive" || scenario == "merge-forward") && len(event.Message.Resources) != 1 {
				t.Fatalf("child resources leaked into event: %+v", event.Message.Resources)
			}
			if !event.Addressed {
				t.Fatal("bot mention was not recognized")
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
			case "auto-file", "file", "folder", "text", "interactive", "share-chat", "share-user", "system", "merge-forward":
				if !strings.Contains(contents, "parent") || len(failures) != 0 {
					t.Fatalf("contents=%q failures=%v", contents, failures)
				}
				if scenario == "auto-file" || scenario == "file" || scenario == "folder" || scenario == "text" {
					if !strings.Contains(contents, "fatal: tracking failed") {
						t.Fatal("message content missing")
					}
				}
				if (scenario == "auto-file" || scenario == "file") && !strings.Contains(contents, "device.log") {
					t.Fatal("log attachment missing")
				}
				if scenario == "interactive" && !strings.Contains(contents, "Interactive card") {
					t.Fatal("interactive context missing")
				}
				if scenario == "share-chat" && !strings.Contains(contents, "shared-chat") {
					t.Fatal("shared chat context missing")
				}
				if scenario == "share-user" && !strings.Contains(contents, "shared-user") {
					t.Fatal("shared user context missing")
				}
				if scenario == "system" && !strings.Contains(contents, "System message") {
					t.Fatal("system context missing")
				}
				if scenario == "merge-forward" && (!strings.Contains(contents, "forwarded messages") || !strings.Contains(contents, "Forwarded message 1 (file): [File]")) {
					t.Fatal("merge-forward context missing")
				}
			default:
				if len(failures) == 0 || strings.Contains(contents, "fatal:") || strings.Contains(strings.Join(failures, ""), "private diagnostic") {
					t.Fatalf("contents=%q failures=%v", contents, failures)
				}
				want := map[string]string{"denied": "quoted_message_lookup_failed: code=99991672", "deleted": "quoted_message_deleted", "wrong-chat": "quoted_message_identity_mismatch", "wrong-message": "quoted_message_not_found", "download-failed": "download_failed", "folder-failed": "download_failed"}[scenario]
				if len(failures) != 1 || failures[0] != want {
					t.Fatalf("failure=%v want=%q", failures, want)
				}
			}
		})
	}
}
