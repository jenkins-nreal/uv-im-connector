package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
	"github.com/hengshi/uv-im-connector/client"
	"github.com/hengshi/uv-im-connector/conformance"
	"github.com/hengshi/uv-im-connector/server"
)

func TestInboundAttachmentsReachCaller(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	plain := []byte("attachment content")
	padding := 32 - len(plain)%32
	encrypted := append(bytes.Clone(plain), bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, encrypted)
	media := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(encrypted)
	}))
	defer media.Close()
	for _, chatType := range []string{"single", "group"} {
		for _, kind := range []string{"file", "image", "video", "mixed"} {
			for _, quoted := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/quoted=%t", chatType, kind, quoted), func(t *testing.T) {
					payload := map[string]any{"url": media.URL, "aeskey": base64.StdEncoding.EncodeToString(key), "file_name": "report.bin"}
					content := map[string]any{"msgtype": kind, kind: payload}
					wantKind := kind
					if kind == "mixed" {
						wantKind = "image"
						content["mixed"] = map[string]any{"msg_item": []any{
							map[string]any{"msgtype": "text", "text": map[string]any{"content": "analyze this"}},
							map[string]any{"msgtype": "image", "image": payload},
						}}
					}
					body := content
					if quoted {
						body = map[string]any{"msgtype": "text", "text": map[string]any{"content": "analyze this"}, "quote": content}
					}
					body["msgid"], body["chattype"], body["chatid"] = "message-1", chatType, "chat-1"
					body["from"] = map[string]any{"userid": "user-1"}
					store := &uvim.ResourceStore{Dir: t.TempDir()}
					provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store, HTTPClient: media.Client()})
					if err != nil {
						t.Fatal(err)
					}
					event, ok := provider.decodeMessage(frame{Cmd: cmdCallback, Headers: headers{ReqID: "request-1"}, Body: body})
					wantResources := 1
					if quoted && kind == "mixed" {
						wantResources = 2
					}
					if !ok || !event.Addressed || len(event.Message.Resources) != wantResources {
						t.Fatalf("decoded ok=%t addressed=%t resources=%d", ok, event.Addressed, len(event.Message.Resources))
					}
					wantChannel, wantTarget := uvim.ChannelDirect, uvim.TargetUser
					if chatType == "group" {
						wantChannel, wantTarget = uvim.ChannelGroup, uvim.TargetGroup
					}
					if event.Channel.Type != wantChannel || event.Referrer.Target.Kind != wantTarget || event.Referrer.Target.ID != "chat-1" {
						t.Fatalf("channel=%+v target=%+v", event.Channel, event.Referrer.Target)
					}
					log, err := uvim.NewEventLog(t.TempDir() + "/events.jsonl")
					if err != nil {
						t.Fatal(err)
					}
					hub := server.NewHub(uvim.NewProviderRegistry(provider), log, store)
					if err := hub.Emit(context.Background(), event); err != nil {
						t.Fatal(err)
					}
					api := httptest.NewServer(hub.Handler())
					defer api.Close()
					c := client.New(api.URL)
					events, err := c.Events(context.Background(), 0)
					if err != nil || len(events) != 1 {
						t.Fatalf("events=%d err=%v", len(events), err)
					}
					ref := events[0].Message.Resources[0]
					if !events[0].Addressed || ref.Kind != wantKind || ref.InternalURL == "" || ref.Error != "" || ref.URL != "" || ref.Secret != "" {
						t.Fatalf("public event = %+v", events[0])
					}
					resp, err := c.ResolveInternalURL(context.Background(), ref.InternalURL)
					if err != nil {
						t.Fatal(err)
					}
					defer resp.Body.Close()
					got, err := io.ReadAll(resp.Body)
					if err != nil || !bytes.Equal(got, plain) {
						t.Fatalf("downloaded=%q err=%v", got, err)
					}
				})
			}
		}
	}
}

func TestDecodeMessagePreservesQuotedTextAsContextResource(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := provider.decodeMessage(frame{Cmd: cmdCallback, Headers: headers{ReqID: "req-1"}, Body: map[string]any{
		"msgid":    "msg-1",
		"msgtype":  "text",
		"chattype": "group",
		"chatid":   "chat-1",
		"from":     map[string]any{"userid": "u1"},
		"text":     map[string]any{"content": "analyze this"},
		"quote": map[string]any{
			"msgtype": "text",
			"text":    map[string]any{"content": "quoted context"},
		},
	}})
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Message.Text != "analyze this" {
		t.Fatalf("message text = %q", event.Message.Text)
	}
	if len(event.Message.Resources) != 1 {
		t.Fatalf("resources = %+v", event.Message.Resources)
	}
	ref := event.Message.Resources[0]
	if ref.Name != "quoted-message.txt" || ref.InternalURL == "" || ref.Error != "" {
		t.Fatalf("quote resource = %+v", ref)
	}
	file, _, err := store.Open(ref.InternalURL)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	raw, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "quoted context") {
		t.Fatalf("quote body = %q", raw)
	}
}

func TestProviderConformanceShape(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: &uvim.ResourceStore{Dir: t.TempDir()}, Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	caps := provider.Capabilities()
	if !caps.Inbound || !caps.Outbound || !caps.UploadResource || !caps.DownloadResource {
		t.Fatalf("capabilities = %+v", caps)
	}
	if provider.ID() != "wecom" {
		t.Fatalf("ID = %q", provider.ID())
	}
	conformance.AssertProviderMetadata(t, provider)
}

func TestDecodeMessageFile(t *testing.T) {
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	provider, err := New(Config{BotID: "bot", Secret: "secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := provider.decodeMessage(frame{
		Cmd:     cmdCallback,
		Headers: headers{ReqID: "req-1"},
		Body: map[string]any{
			"msgid":    "msg-1",
			"msgtype":  "file",
			"chattype": "group",
			"chatid":   "chat-1",
			"from":     map[string]any{"userid": "u1"},
			"file":     map[string]any{"file_name": "log.txt", "url": "https://download.test/file", "aeskey": "secret"},
		},
	})
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.Type != uvim.EventMessageCreate || event.Channel.Type != uvim.ChannelGroup {
		t.Fatalf("event = %+v", event)
	}
	if event.Referrer.Target == nil || event.Referrer.Target.ID != "chat-1" || event.Referrer.Target.Kind != uvim.TargetGroup {
		t.Fatalf("reply target = %+v", event.Referrer.Target)
	}
	if event.Referrer.ExpiresAt == nil || !event.Referrer.ExpiresAt.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("reply expiry = %v", event.Referrer.ExpiresAt)
	}
	if len(event.Message.Resources) != 1 || event.Message.Resources[0].Name != "log.txt" {
		t.Fatalf("resources = %+v", event.Message.Resources)
	}
	if got := event.Sanitized().Message.Resources[0].URL; got != "" {
		t.Fatalf("sanitized URL = %q", got)
	}
}

func TestDecodeMessageAddsConfiguredDisplayNames(t *testing.T) {
	provider, err := New(Config{
		BotID:             "bot",
		Secret:            "secret",
		UserNames:         map[string]string{"u1": "张三"},
		ConversationNames: map[string]string{"chat-1": "研发群"},
	})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := provider.decodeMessage(frame{Cmd: cmdCallback, Headers: headers{ReqID: "req-1"}, Body: map[string]any{
		"msgid": "msg-1", "msgtype": "text", "chattype": "group", "chatid": "chat-1",
		"from": map[string]any{"userid": "u1"}, "text": map[string]any{"content": "hello"},
	}})
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.User.DisplayName != "张三" || event.Channel.Name != "研发群" {
		t.Fatalf("event = %+v", event)
	}
}

func TestDecodeMessageCallbackDisplayNamesOverrideConfiguredMaps(t *testing.T) {
	provider, err := New(Config{
		BotID:             "bot",
		Secret:            "secret",
		UserNames:         map[string]string{"u1": "旧用户名"},
		ConversationNames: map[string]string{"chat-1": "旧群名"},
	})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := provider.decodeMessage(frame{Cmd: cmdCallback, Headers: headers{ReqID: "req-1"}, Body: map[string]any{
		"msgid": "msg-1", "msgtype": "text", "chattype": "group", "chatid": "chat-1", "chat_name": "新群名",
		"from": map[string]any{"userid": "u1", "display_name": "新用户名"}, "text": map[string]any{"content": "hello"},
	}})
	if !ok {
		t.Fatal("decode ok = false")
	}
	if event.User.DisplayName != "新用户名" || event.Channel.Name != "新群名" {
		t.Fatalf("event = %+v", event)
	}
}

func TestDecodeMessageIgnoresKeyOnlyAttachment(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	event, ok := provider.decodeMessage(frame{
		Cmd:     cmdCallback,
		Headers: headers{ReqID: "req-1"},
		Body: map[string]any{
			"msgid":    "msg-1",
			"msgtype":  "file",
			"chattype": "single",
			"from":     map[string]any{"userid": "u1"},
			"file":     map[string]any{"file_name": "log.txt", "media_id": "media-key"},
		},
	})
	if !ok {
		t.Fatal("decode ok = false")
	}
	if len(event.Message.Resources) != 0 {
		t.Fatalf("key-only resource should not be emitted: %+v", event.Message.Resources)
	}
}

func TestDecodeAESKeyAcceptsRawBase64(t *testing.T) {
	key := "0123456789abcdef0123456789abcdef"
	encoded := "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY"
	got, err := DecodeAESKey(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != key {
		t.Fatalf("key = %q", string(got))
	}
}

func TestConformanceNonNetworkParts(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	health := provider.Health(nil)
	if health.Provider != "wecom" {
		t.Fatalf("health = %+v", health)
	}
	if provider.Capabilities().Inbound == false {
		t.Fatal("provider must declare inbound")
	}
	conformance.AssertProviderMetadata(t, provider)
}

func TestSendRejectsMixedTextAndResource(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(nil, uvim.OutboundMessage{
		Provider:  "wecom",
		ChannelID: "chat",
		Text:      "file",
		Resources: []uvim.ResourceRef{{Kind: uvim.ElementFile, InternalURL: "internal://r1"}},
	})
	if err == nil {
		t.Fatal("Send() error = nil, want unsupported resource error")
	}
}

func TestSendResourceReplyUploadsChunksAndSendsMedia(t *testing.T) {
	data := bytes.Repeat([]byte("x"), uploadChunkSize+1)
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewReader(data), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "report.txt", MIME: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	var frames []frame
	conn.onWrite = func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		frames = append(frames, sent)
		code := 0
		ack := frame{Headers: sent.Headers, ErrCode: &code}
		switch sent.Cmd {
		case cmdUploadInit:
			ack.Body = map[string]any{"upload_id": "upload-1"}
		case cmdUploadFinish:
			ack.Body = map[string]any{"media_id": "media-1"}
		}
		provider.resolvePending(sent.Headers.ReqID, ack)
	}
	activateSendTestConn(provider, conn)

	result, err := provider.Send(context.Background(), uvim.OutboundMessage{
		Provider:  "wecom",
		ChannelID: "chat-1",
		Resources: []uvim.ResourceRef{ref},
		Referrer:  uvim.Referrer{MessageID: "msg-1", ReplyToken: "reply-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.MessageID != "reply-1" {
		t.Fatalf("result = %+v", result)
	}
	wantCommands := []string{cmdUploadInit, cmdUploadChunk, cmdUploadChunk, cmdUploadFinish, cmdRespond}
	if len(frames) != len(wantCommands) {
		t.Fatalf("frames = %+v", frames)
	}
	for i, command := range wantCommands {
		if frames[i].Cmd != command {
			t.Fatalf("frame %d command = %q, want %q", i, frames[i].Cmd, command)
		}
	}
	if frames[0].Body["filename"] != "report.txt" || int(frames[0].Body["total_chunks"].(float64)) != 2 {
		t.Fatalf("init body = %+v", frames[0].Body)
	}
	if frames[0].Body["md5"] != fmt.Sprintf("%x", md5.Sum(data)) {
		t.Fatalf("init md5 = %v", frames[0].Body["md5"])
	}
	var uploaded []byte
	for index, sent := range frames[1:3] {
		if got := int(sent.Body["chunk_index"].(float64)); got != index {
			t.Fatalf("chunk index = %d, want %d", got, index)
		}
		decoded, err := base64.StdEncoding.DecodeString(sent.Body["base64_data"].(string))
		if err != nil {
			t.Fatal(err)
		}
		uploaded = append(uploaded, decoded...)
	}
	if !bytes.Equal(uploaded, data) {
		t.Fatal("uploaded chunks do not match source data")
	}
	media := frames[4].Body[uvim.ElementFile].(map[string]any)
	if frames[4].Headers.ReqID != "reply-1" || frames[4].Body["msgtype"] != uvim.ElementFile || media["media_id"] != "media-1" {
		t.Fatalf("media frame = %+v", frames[4])
	}
}

func TestWeComUploadChunkCountBoundaries(t *testing.T) {
	for _, test := range []struct {
		size int
		want int
		ok   bool
	}{
		{size: 0, want: 0, ok: true},
		{size: 1, want: 1, ok: true},
		{size: uploadChunkSize, want: 1, ok: true},
		{size: uploadChunkSize + 1, want: 2, ok: true},
		{size: uploadChunkSize * 100, want: 100, ok: true},
		{size: uploadChunkSize*100 + 1, want: 101, ok: true},
	} {
		got, err := wecomUploadChunkCount(test.size)
		if test.ok && (err != nil || got != test.want) {
			t.Fatalf("size %d: chunks=%d error=%v, want %d", test.size, got, err, test.want)
		}
		if !test.ok && err == nil {
			t.Fatalf("size %d: error=nil", test.size)
		}
	}
}

func TestWeComMediaKindMapsProtocolAudioToVoice(t *testing.T) {
	if got := wecomMediaKind(uvim.ElementAudio); got != mediaVoice {
		t.Fatalf("wecomMediaKind(audio) = %q, want %q", got, mediaVoice)
	}
}

func TestSendResourceReturnsUploadStageErrors(t *testing.T) {
	for _, failCommand := range []string{cmdUploadInit, cmdUploadChunk, cmdUploadFinish, cmdRespond} {
		t.Run(failCommand, func(t *testing.T) {
			store := &uvim.ResourceStore{Dir: t.TempDir()}
			ref, err := store.Save(context.Background(), strings.NewReader("file"), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "file.txt"})
			if err != nil {
				t.Fatal(err)
			}
			provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
			if err != nil {
				t.Fatal(err)
			}
			conn := &sendTestConn{}
			conn.onWrite = func(raw []byte) {
				var sent frame
				if err := json.Unmarshal(raw, &sent); err != nil {
					t.Error(err)
					return
				}
				code := 0
				ack := frame{Headers: sent.Headers, ErrCode: &code}
				if sent.Cmd == cmdUploadInit {
					ack.Body = map[string]any{"upload_id": "upload-1"}
				}
				if sent.Cmd == cmdUploadFinish {
					ack.Body = map[string]any{"media_id": "media-1"}
				}
				if sent.Cmd == failCommand {
					failure := 50001
					ack.ErrCode = &failure
					ack.ErrMsg = "stage failed"
				}
				provider.resolvePending(sent.Headers.ReqID, ack)
			}
			activateSendTestConn(provider, conn)
			_, err = provider.Send(context.Background(), uvim.OutboundMessage{
				ChannelID: "chat-1",
				Resources: []uvim.ResourceRef{ref},
				Referrer:  uvim.Referrer{MessageID: "msg-1", ReplyToken: "reply-1"},
			})
			if err == nil || !strings.Contains(err.Error(), "stage failed") {
				t.Fatalf("Send() error = %v", err)
			}
		})
	}
}

func TestSendResourceProactivelyUsesTarget(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), strings.NewReader("file"), uvim.ResourceRef{Kind: uvim.ElementImage, Name: "chart.png"})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	var last frame
	conn.onWrite = func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		last = sent
		code := 0
		ack := frame{Headers: sent.Headers, ErrCode: &code}
		if sent.Cmd == cmdUploadInit {
			ack.Body = map[string]any{"upload_id": "upload-1"}
		}
		if sent.Cmd == cmdUploadFinish {
			ack.Body = map[string]any{"media_id": "media-1"}
		}
		provider.resolvePending(sent.Headers.ReqID, ack)
	}
	activateSendTestConn(provider, conn)

	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Provider:  "wecom",
		Target:    &uvim.OutboundTarget{ID: "user-1", Kind: uvim.TargetUser},
		Resources: []uvim.ResourceRef{ref},
	})
	if err != nil {
		t.Fatal(err)
	}
	image := last.Body[uvim.ElementImage].(map[string]any)
	if last.Cmd != cmdSend || last.Body["chatid"] != "user-1" || last.Body["msgtype"] != uvim.ElementImage || image["media_id"] != "media-1" {
		t.Fatalf("media frame = %+v", last)
	}
}

func TestUploadEmptyResourceDefersToWeCom(t *testing.T) {
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(t.Context(), bytes.NewReader(nil), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "empty.bin"})
	if err != nil {
		t.Fatal(err)
	}
	provider, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	seen := false
	conn := &sendTestConn{onWrite: func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Fatal(err)
		}
		if sent.Cmd != cmdUploadInit || sent.Body["total_size"] != float64(0) || sent.Body["total_chunks"] != float64(0) {
			t.Fatalf("frame = %+v", sent)
		}
		seen = true
		code := 400
		provider.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code})
	}}
	activateSendTestConn(provider, conn)
	_, err = provider.Send(t.Context(), uvim.OutboundMessage{ChannelID: "chat", Resources: []uvim.ResourceRef{ref}})
	failure, ok := uvim.ProviderSendFailure(err)
	if !seen || !ok || failure.ProviderCode != "400" {
		t.Fatalf("seen=%v failure=%+v err=%v", seen, failure, err)
	}
}

func TestLargeUploadDefersChunkLimitToWeCom(t *testing.T) {
	data := bytes.Repeat([]byte("x"), uploadChunkSize*100+1)
	store := &uvim.ResourceStore{Dir: t.TempDir()}
	ref, err := store.Save(context.Background(), bytes.NewReader(data), uvim.ResourceRef{Kind: uvim.ElementFile, Name: "large.bin"})
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Config{BotID: "bot", Secret: "secret", ResourceStore: store})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	requests := 0
	conn.onWrite = func(raw []byte) {
		requests++
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		if sent.Cmd != cmdUploadInit || sent.Body["total_chunks"] != float64(101) || sent.Body["total_size"] != float64(len(data)) {
			t.Errorf("unexpected upload init: %+v", sent)
		}
		code := 40005
		p.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code, ErrMsg: "media too large"})
	}
	activateSendTestConn(p, conn)
	_, err = p.Send(context.Background(), uvim.OutboundMessage{ChannelID: "chat", Resources: []uvim.ResourceRef{ref}})
	failure, ok := uvim.ProviderSendFailure(err)
	if requests != 1 || !ok || failure.ProviderCode != "40005" {
		t.Fatalf("requests=%d failure=%+v err=%v", requests, failure, err)
	}
}

func TestProactiveSendUsesMarkdownForDirectUser(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	conn.onWrite = func(raw []byte) {
		var sent frame
		if err := json.Unmarshal(raw, &sent); err != nil {
			t.Error(err)
			return
		}
		conn.sent = sent
		code := 0
		provider.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code})
	}
	provider.activeMu.Lock()
	provider.activeConn = conn
	provider.activeWrite = &sync.Mutex{}
	provider.activeMu.Unlock()

	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Provider: "wecom",
		Target:   &uvim.OutboundTarget{ID: "ChenJunHao", Kind: uvim.TargetUser},
		Text:     "upgrade complete",
	})
	if err != nil {
		t.Fatal(err)
	}
	if conn.sent.Cmd != cmdSend || conn.sent.Body["chatid"] != "ChenJunHao" || conn.sent.Body["msgtype"] != "markdown" {
		t.Fatalf("sent frame = %+v", conn.sent)
	}
	markdown, _ := conn.sent.Body["markdown"].(map[string]any)
	if markdown["content"] != "upgrade complete" {
		t.Fatalf("markdown = %+v", markdown)
	}
}

func TestSendReturnsProviderAckError(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{}
	conn.onWrite = func(raw []byte) {
		var sent frame
		_ = json.Unmarshal(raw, &sent)
		code := 40058
		provider.resolvePending(sent.Headers.ReqID, frame{Headers: sent.Headers, ErrCode: &code, ErrMsg: "invalid msgtype"})
	}
	provider.activeMu.Lock()
	provider.activeConn = conn
	provider.activeWrite = &sync.Mutex{}
	provider.activeMu.Unlock()

	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "u1", ChannelType: uvim.ChannelDirect, Text: "hello"})
	if err == nil || !strings.Contains(err.Error(), "errcode=40058") || !strings.Contains(err.Error(), "invalid msgtype") {
		t.Fatalf("Send() error = %v", err)
	}
}

func TestSendPreservesWebSocketTransportCause(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	conn := &sendTestConn{writeErr: &net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNRESET}}
	activateSendTestConn(provider, conn)
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{ChannelID: "u1", ChannelType: uvim.ChannelDirect, Text: "hello"})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "wecom send: write tcp: connection reset" {
		t.Fatalf("private log detail = %q", got)
	}
}

func TestSendMarksFixedLocalFailure(t *testing.T) {
	provider, err := New(Config{BotID: "bot", Secret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Send(context.Background(), uvim.OutboundMessage{
		Target:   &uvim.OutboundTarget{ID: "user-1", Kind: uvim.TargetUser},
		Elements: []uvim.Element{{Type: "button"}},
	})
	if err == nil {
		t.Fatal("Send() error = nil")
	}
	if got := uvim.ProviderSendErrorLogDetail(err); got != "wecom send: rich elements are not supported" {
		t.Fatalf("private log detail = %q", got)
	}
}

type sendTestConn struct {
	onWrite  func([]byte)
	sent     frame
	writeErr error
}

func activateSendTestConn(provider *Provider, conn *sendTestConn) {
	provider.activeMu.Lock()
	provider.activeConn = conn
	provider.activeWrite = &sync.Mutex{}
	provider.activeMu.Unlock()
}

func (c *sendTestConn) ReadMessage() (int, []byte, error) {
	return 0, nil, errors.New("not implemented")
}
func (c *sendTestConn) WriteMessage(_ int, raw []byte) error {
	if c.onWrite != nil {
		c.onWrite(raw)
	}
	return c.writeErr
}
func (c *sendTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *sendTestConn) SetWriteDeadline(time.Time) error { return nil }
func (c *sendTestConn) Close() error                     { return nil }
