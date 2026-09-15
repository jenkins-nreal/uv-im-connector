package lark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/conformance"
	"github.com/hengshi/uv-im-connector/server"
)

func TestPostDownloadDoesNotBlockFollowingACKOrDisconnectDrain(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	ackDone := make(chan error, 1)
	var api *httptest.Server
	api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/callback/ws/endpoint":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"URL": strings.Replace(api.URL, "http:", "ws:", 1) + "/ws?service_id=1"}})
		case "/ws":
			conn, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
			if err != nil {
				ackDone <- err
				return
			}
			defer conn.Close()
			for i, content := range []string{`{"content":[[{"tag":"img","image_key":"image-1"}]]}`, `{"text":"next"}`} {
				if i == 1 {
					<-started
				}
				kind := []string{"post", "text"}[i]
				raw, _ := json.Marshal(map[string]any{"header": map[string]any{"event_type": "im.message.receive_v1"}, "event": map[string]any{"message": map[string]any{
					"message_id": kind, "chat_id": "chat", "chat_type": "group", "message_type": kind, "content": content,
					"mentions": []any{map[string]any{"key": "@bot", "id": map[string]any{"open_id": "bot"}}},
				}}})
				out := &wsFrame{Method: frameMethodData, Headers: []frameHeader{{Key: frameHeaderMessageID, Value: kind}}, Payload: raw}
				if err := conn.WriteMessage(websocket.BinaryMessage, out.marshal()); err != nil {
					ackDone <- err
					return
				}
				conn.SetReadDeadline(time.Now().Add(time.Second))
				_, raw, err = conn.ReadMessage()
				if err != nil {
					ackDone <- err
					return
				}
				ack, err := unmarshalFrame(raw)
				if err != nil || ack.headerValue(frameHeaderMessageID) != kind {
					ackDone <- errors.New("incorrect ACK")
					return
				}
			}
			ackDone <- nil
		case "/open-apis/auth/v3/tenant_access_token/internal":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/chats/chat":
			json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"name": "group"}})
		case "/open-apis/im/v1/messages/post/resources/image-1":
			close(started)
			select {
			case <-release:
				w.Write([]byte("image bytes"))
			case <-req.Context().Done():
			}
		default:
			t.Errorf("unexpected path %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()
	defer close(release)
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
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- provider.Run(ctx, hub) }()
	if err := <-ackDone; err != nil {
		t.Fatalf("ACK blocked by download: %v", err)
	}
	release <- struct{}{}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("TCP disconnect error missing")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not drain")
	}
	events, err := log.ReadAfter(t.Context(), 0)
	if err != nil || len(events) != 2 || events[0].Message.ID != "post" || events[1].Message.ID != "text" {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	ref := events[0].Message.Resources[0]
	file, _, err := store.Open(ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil || string(data) != "image bytes" || ref.Error != "" {
		t.Fatalf("resource=%+v data=%q err=%v", ref, data, err)
	}
}

func TestChunkAssemblerWithoutCountOrDefaultExpiry(t *testing.T) {
	now := time.Now()
	assembler := newChunkAssembler(0, func() time.Time { return now })
	// A declaration alone must not allocate an array proportional to sum.
	assembler.admit("sparse", int(^uint(0)>>1), 0, []byte("x"))
	for seq := 256; seq >= 0; seq-- {
		if seq == 0 {
			now = now.Add(time.Hour)
		}
		data, complete := assembler.admit("message", 257, seq, []byte{byte(seq)})
		if seq != 0 && complete {
			t.Fatal("completed before all chunks arrived")
		}
		if seq == 0 {
			if !complete || len(data) != 257 {
				t.Fatalf("complete=%v length=%d", complete, len(data))
			}
			for i, b := range data {
				if b != byte(i) {
					t.Fatalf("chunk %d = %d", i, b)
				}
			}
		}
	}
	assembler.admit("empty", 2, 0, nil)
	assembler.admit("empty", 2, 0, nil)
	if _, complete := assembler.admit("empty", 3, 2, nil); complete {
		t.Fatal("accepted inconsistent sum")
	}
	if _, complete := assembler.admit("empty", 2, 1, nil); !complete {
		t.Fatal("empty chunks never completed")
	}
}

func TestDecodePayloadPostResources(t *testing.T) {
	for _, chatType := range []string{"p2p", "group"} {
		t.Run(chatType, func(t *testing.T) {
			content := `{"title":"Review","content":[[{"tag":"text","text":"analyze this"},{"tag":"img","image_key":"image-1"}],[{"tag":"media","file_key":"video-1","image_key":"preview-1"},{"tag":"img"}]]}`
			payload, err := json.Marshal(map[string]any{
				"header": map[string]any{"event_type": "im.message.receive_v1", "event_id": "event-1"},
				"event": map[string]any{"message": map[string]any{
					"message_id": "message-1", "chat_id": "chat-1", "chat_type": chatType,
					"message_type": "post", "content": content,
					"mentions": []any{map[string]any{"key": "@_user_1", "id": map[string]any{"open_id": "bot"}}},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			event, ok, err := DecodePayload(payload, DecoderConfig{Connector: "main", BotOpenID: "bot"})
			if err != nil || !ok || !event.Addressed || len(event.Message.Resources) != 2 {
				t.Fatalf("ok=%t err=%v event=%+v", ok, err, event)
			}
			for i, want := range []uvim.ResourceRef{{Kind: "image", Key: "image-1"}, {Kind: "video", Key: "video-1"}} {
				ref := event.Message.Resources[i]
				if ref.Kind != want.Kind || ref.Key != want.Key || ref.Metadata["message_id"] != "message-1" || ref.Provider != "lark" || ref.Connector != "main" {
					t.Fatalf("resource %d = %+v", i, ref)
				}
			}
			ambient, _, err := DecodePayload(payload, DecoderConfig{Connector: "main", BotOpenID: "another-bot"})
			if err != nil || ambient.Addressed != (chatType == "p2p") || len(ambient.Message.Resources) != 2 {
				t.Fatalf("ambient addressed=%t resources=%d err=%v", ambient.Addressed, len(ambient.Message.Resources), err)
			}
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/open-apis/auth/v3/tenant_access_token/internal":
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
				case "/open-apis/im/v1/messages/message-1/resources/image-1", "/open-apis/im/v1/messages/message-1/resources/video-1":
					wantType := "file"
					if req.URL.Path == "/open-apis/im/v1/messages/message-1/resources/image-1" {
						wantType = "image"
					}
					if req.URL.Query().Get("type") != wantType || req.Header.Get("Authorization") != "Bearer token" {
						t.Errorf("download query=%v authorization=%q", req.URL.Query(), req.Header.Get("Authorization"))
					}
					_, _ = w.Write([]byte("resource bytes"))
				default:
					t.Errorf("unexpected download path %q", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer api.Close()
			store := &uvim.ResourceStore{Dir: t.TempDir()}
			p, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: api.URL, ResourceStore: store})
			if err != nil {
				t.Fatal(err)
			}
			for _, ref := range event.Message.Resources {
				downloaded, err := p.Download(context.Background(), uvim.ResourceDownloadRequest{Resource: ref, Event: event, Message: event.Message})
				if err != nil {
					t.Fatal(err)
				}
				file, _, err := store.Open(downloaded.InternalURL)
				if err != nil {
					t.Fatal(err)
				}
				data, err := io.ReadAll(file)
				file.Close()
				if err != nil || string(data) != "resource bytes" {
					t.Fatalf("downloaded=%q err=%v", data, err)
				}
			}
		})
	}
}

func TestDecodePayloadTextMention(t *testing.T) {
	event := map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   "evt-1",
			"event_type": "im.message.receive_v1",
			"app_id":     "app",
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_id": map[string]any{"open_id": "ou_sender"},
			},
			"message": map[string]any{
				"message_id":   "om_msg",
				"parent_id":    "om_parent",
				"root_id":      "om_root",
				"chat_id":      "oc_chat",
				"chat_type":    "group",
				"message_type": "text",
				"content":      `{"text":"@_user_1 帮我看一下"}`,
				"mentions": []any{
					map[string]any{
						"key":  "@_user_1",
						"name": "Bot",
						"id":   map[string]any{"open_id": "ou_bot"},
					},
				},
				"create_time": "1700000000000",
			},
		},
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := DecodePayload(raw, DecoderConfig{BotOpenID: "ou_bot", Connector: "main"})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ok = false")
	}
	if got.Provider != "lark" || got.Connector != "main" || got.Message.ID != "om_msg" {
		t.Fatalf("event = %+v", got)
	}
	if got.Channel.Type != uvim.ChannelGroup {
		t.Fatalf("channel = %+v", got.Channel)
	}
	if got.Message.Text != "帮我看一下" {
		t.Fatalf("text = %q", got.Message.Text)
	}
	if got.Referrer.MessageID != "om_msg" || got.Referrer.ParentMessageID != "om_parent" || got.Referrer.RootMessageID != "om_root" {
		t.Fatalf("referrer = %+v", got.Referrer)
	}
}

func TestEnrichEventDisplayNamesUsesLarkAPIsAndCache(t *testing.T) {
	var tokenRequests, chatRequests, userRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			tokenRequests++
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/chats/oc_chat":
			chatRequests++
			if req.Header.Get("Authorization") != "Bearer token" {
				t.Errorf("authorization = %q", req.Header.Get("Authorization"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"name": "研发群"}})
		case "/open-apis/contact/v3/users/ou_sender":
			userRequests++
			if req.URL.Query().Get("user_id_type") != "open_id" {
				t.Errorf("user_id_type = %q", req.URL.Query().Get("user_id_type"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"user": map[string]any{"name": "张三"}}})
		default:
			t.Errorf("unexpected path %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		event := uvim.Event{Channel: uvim.Channel{ID: "oc_chat", Type: uvim.ChannelGroup}, User: uvim.User{ID: "ou_sender"}}
		provider.enrichEventDisplayNames(context.Background(), &event)
		if event.Channel.Name != "研发群" || event.User.DisplayName != "张三" {
			t.Fatalf("event = %+v", event)
		}
	}
	direct := uvim.Event{Channel: uvim.Channel{ID: "oc_direct", Type: uvim.ChannelDirect}, User: uvim.User{ID: "ou_sender"}}
	provider.enrichEventDisplayNames(context.Background(), &direct)
	direct = direct.Sanitized()
	if direct.Channel.Name != "张三" || direct.User.DisplayName != "张三" {
		t.Fatalf("direct event = %+v", direct)
	}
	if tokenRequests != 1 || chatRequests != 1 || userRequests != 1 {
		t.Fatalf("requests token=%d chat=%d user=%d", tokenRequests, chatRequests, userRequests)
	}
}

func TestEnrichEventDisplayNamesRetriesAfterLarkLookupFails(t *testing.T) {
	requests := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
			return
		}
		requests[req.URL.Path]++
		if requests[req.URL.Path] == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 999, "msg": "temporary failure"})
			return
		}
		switch req.URL.Path {
		case "/open-apis/im/v1/chats/oc_chat":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"name": "恢复群"}})
		case "/open-apis/contact/v3/users/ou_sender":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"user": map[string]any{"name": "恢复用户"}}})
		default:
			t.Errorf("unexpected path %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	event := uvim.Event{Channel: uvim.Channel{ID: "oc_chat", Type: uvim.ChannelGroup}, User: uvim.User{ID: "ou_sender"}}
	provider.enrichEventDisplayNames(context.Background(), &event)
	if event.Channel.ID != "oc_chat" || event.User.ID != "ou_sender" || event.Channel.Name != "" || event.User.DisplayName != "" {
		t.Fatalf("event after temporary failure = %+v", event)
	}
	provider.enrichEventDisplayNames(context.Background(), &event)
	if event.Channel.Name != "恢复群" || event.User.DisplayName != "恢复用户" {
		t.Fatalf("event after recovery = %+v", event)
	}
	for path, count := range requests {
		if count != 2 {
			t.Fatalf("requests[%q] = %d, want 2", path, count)
		}
	}
}

func TestDecodePayloadFileResource(t *testing.T) {
	event := map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":   "evt-1",
			"event_type": "im.message.receive_v1",
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_id": map[string]any{"open_id": "ou_sender"},
			},
			"message": map[string]any{
				"message_id":   "om_msg",
				"chat_id":      "oc_chat",
				"chat_type":    "p2p",
				"message_type": "file",
				"content":      `{"file_key":"file_v3_abc","file_name":"log.txt"}`,
			},
		},
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := DecodePayload(raw, DecoderConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("ok = false")
	}
	if len(got.Message.Resources) != 1 {
		t.Fatalf("resources = %+v", got.Message.Resources)
	}
	ref := got.Message.Resources[0]
	if ref.Kind != uvim.ElementFile || ref.Key != "file_v3_abc" || ref.Name != "log.txt" {
		t.Fatalf("resource = %+v", ref)
	}
	if ref.Metadata["message_id"] != "om_msg" {
		t.Fatalf("metadata = %+v", ref.Metadata)
	}
}

func TestDecodePayloadCoversAdditionalMessageTypes(t *testing.T) {
	tests := []struct {
		name        string
		messageType string
		content     string
		wantText    string
		wantKind    string
		wantKey     string
	}{
		{name: "sticker", messageType: "sticker", content: `{"file_key":"sticker-1"}`, wantText: "[Sticker]"},
		{name: "interactive", messageType: "interactive", content: `{}`, wantText: "[Interactive card]"},
		{name: "shared chat", messageType: "share_chat", content: `{"chat_id":"chat-1"}`, wantText: "[Shared chat: chat-1]"},
		{name: "shared user", messageType: "share_user", content: `{"user_id":"user-1"}`, wantText: "[Shared user: user-1]"},
		{name: "system", messageType: "system", content: `{}`, wantText: "[System message]"},
		{name: "forwarded", messageType: "merge_forward", content: `{}`, wantText: "[forwarded messages]"},
		{name: "unknown", messageType: "future_type", content: `{}`, wantText: "[Unsupported message: future_type]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]any{
				"header": map[string]any{"event_id": "evt-1", "event_type": "im.message.receive_v1"},
				"event": map[string]any{"message": map[string]any{
					"message_id": "msg-1", "chat_id": "chat-1", "chat_type": "p2p",
					"message_type": tt.messageType, "content": tt.content,
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			got, ok, err := DecodePayload(raw, DecoderConfig{})
			if err != nil || !ok {
				t.Fatalf("decode ok=%t err=%v", ok, err)
			}
			if got.Message.Text != tt.wantText {
				t.Fatalf("text = %q, want %q", got.Message.Text, tt.wantText)
			}
			if tt.wantKind == "" {
				return
			}
			if len(got.Message.Resources) != 1 || got.Message.Resources[0].Kind != tt.wantKind || got.Message.Resources[0].Key != tt.wantKey {
				t.Fatalf("resources = %+v", got.Message.Resources)
			}
		})
	}
}

func TestFrameRoundTrip(t *testing.T) {
	in := &wsFrame{SeqID: 1, Service: 2, Method: frameMethodData, Headers: []frameHeader{{Key: "message_id", Value: "m1"}}, Payload: []byte("payload")}
	out, err := unmarshalFrame(in.marshal())
	if err != nil {
		t.Fatal(err)
	}
	if out.SeqID != 1 || out.Service != 2 || out.Method != frameMethodData || string(out.Payload) != "payload" {
		t.Fatalf("frame = %+v", out)
	}
	if out.headerValue("message_id") != "m1" {
		t.Fatalf("headers = %+v", out.Headers)
	}
}

func TestProviderMetadataConformance(t *testing.T) {
	provider, err := New(Config{AppID: "app", AppSecret: "secret", ResourceStore: &uvim.ResourceStore{Dir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	conformance.AssertProviderMetadata(t, provider)
	if !provider.Capabilities().UploadResource {
		t.Fatal("UploadResource = false")
	}
}

func TestSendRejectsMixedTextAndResource(t *testing.T) {
	provider, err := New(Config{AppID: "app", AppSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(nil, uvim.OutboundMessage{
		Provider:  "lark",
		ChannelID: "chat",
		Text:      "file",
		Resources: []uvim.ResourceRef{{Kind: uvim.ElementFile, InternalURL: "internal://r1"}},
	})
	if err == nil {
		t.Fatal("Send() error = nil, want unsupported resource error")
	}
}

func TestSendResourceUploadsThenReplies(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	var sentBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/files":
			if err := req.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
			}
			file, header, err := req.FormFile("file")
			if err != nil {
				t.Error(err)
			} else {
				defer file.Close()
				var data bytes.Buffer
				_, _ = data.ReadFrom(file)
				if header.Filename != "report.txt" || data.String() != "report" || req.FormValue("file_type") != "stream" {
					t.Errorf("upload filename=%q data=%q type=%q", header.Filename, data.String(), req.FormValue("file_type"))
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"file_key": "file-1"}})
		case "/open-apis/im/v1/messages/om_in/reply":
			if err := json.NewDecoder(req.Body).Decode(&sentBody); err != nil {
				t.Error(err)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"message_id": "om_out"}})
		default:
			t.Errorf("unexpected path %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL, ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Send(context.Background(), uvim.OutboundMessage{Resources: []uvim.ResourceRef{ref}, Referrer: uvim.Referrer{MessageID: "om_in"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "om_out" || sentBody["msg_type"] != uvim.ElementFile {
		t.Fatalf("result=%+v body=%+v", result, sentBody)
	}
	var content map[string]string
	if err := json.Unmarshal([]byte(sentBody["content"].(string)), &content); err != nil {
		t.Fatal(err)
	}
	if content["file_key"] != "file-1" || len(content) != 1 {
		t.Fatalf("content = %+v", content)
	}
}

func TestLargeResourceReachesProviderSizePolicy(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	data := bytes.Repeat([]byte("x"), 30*1024*1024+1)
	ref, err := store.Save(context.Background(), bytes.NewReader(data), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "large.bin"})
	if err != nil {
		t.Fatal(err)
	}
	uploaded := int64(0)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
			return
		}
		if req.URL.Path != "/open-apis/im/v1/files" {
			t.Errorf("unexpected path %q", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		reader, err := req.MultipartReader()
		if err != nil {
			t.Error(err)
			return
		}
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Error(err)
				return
			}
			if part.FormName() == "file" {
				uploaded, err = io.Copy(io.Discard, part)
				if err != nil {
					t.Error(err)
				}
			}
			part.Close()
		}
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	defer api.Close()
	p, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: api.URL, ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Send(context.Background(), uvim.OutboundMessage{ChannelID: "chat", Resources: []uvim.ResourceRef{ref}})
	failure, ok := uvim.ProviderSendFailure(err)
	if uploaded != int64(len(data)) || !ok || failure.Category != uvim.SendFailurePayloadTooLarge || failure.HTTPStatus != http.StatusRequestEntityTooLarge {
		t.Fatalf("uploaded=%d failure=%+v err=%v", uploaded, failure, err)
	}
}

func TestSendResourceMarksMissingUploadKeyForPrivateLogs(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewBufferString("report"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/files":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{}})
		default:
			t.Errorf("unexpected path %s", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL, ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{Resources: []uvim.ResourceRef{ref}, Referrer: uvim.Referrer{MessageID: "om_in"}})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "lark upload: response key missing" {
		t.Fatalf("private log detail = %q", got)
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "" {
		t.Fatalf("public detail = %q", got)
	}
}

func TestSendImageUsesImageUpload(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewReader([]byte("png")), uvim.ResourceRef{Kind: uvim.ElementImage, Name: "chart.png", MIME: "image/png"})
	if err != nil {
		t.Fatal(err)
	}
	var sentType string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/images":
			if err := req.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
			}
			if req.FormValue("image_type") != "message" {
				t.Errorf("image_type = %q", req.FormValue("image_type"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"image_key": "img-1"}})
		case "/open-apis/im/v1/messages":
			var body map[string]any
			_ = json.NewDecoder(req.Body).Decode(&body)
			sentType, _ = body["msg_type"].(string)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"message_id": "om_out"}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL, ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "oc_chat", Resources: []uvim.ResourceRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	if sentType != uvim.ElementImage {
		t.Fatalf("msg_type = %q", sentType)
	}
}

func TestProactiveSendSelectsRecipientIDType(t *testing.T) {
	tests := []struct {
		name       string
		target     uvim.OutboundTarget
		wantIDType string
	}{
		{name: "user open id", target: uvim.OutboundTarget{ID: "ou_user", Kind: uvim.TargetUser}, wantIDType: "open_id"},
		{name: "conversation", target: uvim.OutboundTarget{ID: "oc_chat", Kind: uvim.TargetConversation}, wantIDType: "chat_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotBody map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/open-apis/auth/v3/tenant_access_token/internal":
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
				case "/open-apis/im/v1/messages":
					if got := req.URL.Query().Get("receive_id_type"); got != tt.wantIDType {
						t.Errorf("receive_id_type = %q, want %q", got, tt.wantIDType)
					}
					if err := json.NewDecoder(req.Body).Decode(&gotBody); err != nil {
						t.Error(err)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "msg": "ok", "data": map[string]any{"message_id": "om_sent"}})
				default:
					t.Errorf("unexpected path %s", req.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			result, err := provider.Send(context.Background(), uvim.OutboundMessage{Target: &tt.target, Text: "hello"})
			if err != nil {
				t.Fatal(err)
			}
			if gotBody["receive_id"] != tt.target.ID || result.MessageID != "om_sent" {
				t.Fatalf("body=%+v result=%+v", gotBody, result)
			}
		})
	}
}

func TestSendBusinessFailureExposesOnlyNormalizedFacts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 230020, "msg": "access_token=secret"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "oc_chat", Text: "hello"})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	failure, ok := uvim.ProviderSendFailure(err)
	if !ok || failure.Category != uvim.SendFailureProviderRejected || failure.Retryable || failure.DeliveryState != uvim.DeliveryRejected || failure.ProviderCode != "230020" {
		t.Fatalf("failure = %+v, ok=%v", failure, ok)
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "" {
		t.Fatalf("public detail leaked provider message: %q", got)
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "provider rejected request: code 230020" {
		t.Fatalf("private log detail = %q", got)
	}
}

func TestLegacyDirectChannelRemainsChatID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages":
			if got := req.URL.Query().Get("receive_id_type"); got != "chat_id" {
				t.Errorf("receive_id_type = %q, want chat_id", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"message_id": "om_sent"}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "oc_chat", ChannelType: uvim.ChannelDirect, Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSendMarksDecodeFailureForPrivateLogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages":
			_, _ = w.Write([]byte(`{"code":`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "oc_chat", Text: "hello"})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "" {
		t.Fatalf("public detail = %q", got)
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "decode lark send response: unexpected end of JSON input" {
		t.Fatalf("private log detail = %q", got)
	}
}

func TestSendMarksHTTPFailureForPrivateLogs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "tenant_access_token": "token", "expire": 3600})
		case "/open-apis/im/v1/messages":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"access_token=secret"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "oc_chat", Text: "hello"})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorDetail(err); got != "" {
		t.Fatalf("public detail = %q", got)
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "lark send: http 502" {
		t.Fatalf("private log detail = %q", got)
	}
}

func TestDoJSONPreservesStatusAndBodyReadCause(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		readErr    error
		want       string
	}{
		{
			name:       "bad gateway before unreadable body",
			statusCode: http.StatusBadGateway,
			readErr:    errors.New("access_token=secret"),
			want:       "lark send: http 502",
		},
		{
			name:       "successful status with reset body",
			statusCode: http.StatusOK,
			readErr:    &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
			want:       "lark send: read response: read tcp: connection reset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, err := New(Config{
				AppID:     "app",
				AppSecret: "secret",
				HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{
						StatusCode: tt.statusCode,
						Body:       io.NopCloser(errorReader{err: tt.readErr}),
						Header:     make(http.Header),
					}, nil
				})},
			})
			if err != nil {
				t.Fatal(err)
			}
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://api.example.test/private?access_token=secret", nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = provider.doJSON(req, "lark send")
			if err == nil {
				t.Fatal("doJSON() error = nil")
			}
			if got := uvim.ProviderSendErrorLogDetail(err); got != tt.want {
				t.Fatalf("private log detail = %q, want %q", got, tt.want)
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestProactiveSendRejectsNonOpenIDUserTarget(t *testing.T) {
	provider, err := New(Config{AppID: "app", AppSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target: &uvim.OutboundTarget{ID: "user_123", Kind: uvim.TargetUser},
		Text:   "hello",
	})
	if err == nil || err.Error() != "lark send: user target id must be an Open ID" {
		t.Fatalf("Send() error = %v", err)
	}
}
