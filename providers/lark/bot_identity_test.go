package lark

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Startup must fail explicitly before opening a socket with no mention identity.
// Private logs retain useful causes without leaking response text or credentials.
func TestBotIdentityFailureBeforeTransportStub(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		status           int
		auth, canceled   bool
	}{
		{name: "denied", body: `{"code":99991672,"msg":"private diagnostic"}`, want: "code=99991672"},
		{name: "missing-id", body: `{"code":0,"bot":{}}`, want: "missing open_id"},
		{name: "invalid-json", body: `{`, want: "invalid response"},
		{name: "auth-denied", body: `{"code":10014,"msg":"private diagnostic"}`, want: "code=10014", auth: true},
		{name: "http-unavailable", body: `{"code":40009,"msg":"private diagnostic"}`, want: "503", status: 503},
		{name: "canceled", want: "canceled", canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/open-apis/auth/v3/tenant_access_token/internal":
					if tc.auth {
						w.Write([]byte(tc.body))
						return
					}
					w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":3600}`))
				case "/open-apis/bot/v3/info":
					if tc.status != 0 {
						w.WriteHeader(tc.status)
					}
					w.Write([]byte(tc.body))
				default:
					t.Error("transport started without bot identity")
					w.WriteHeader(500)
				}
			}))
			defer api.Close()
			var logs bytes.Buffer
			p, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: api.URL, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceled {
				cancel()
			}
			err = p.Run(ctx, nil)
			if err == nil || !strings.Contains(err.Error(), "lark bot identity:") || strings.Contains(err.Error(), "private diagnostic") {
				t.Fatalf("unexpected startup error: %v", err)
			}
			if tc.canceled && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost cancellation cause: %v", err)
			}
			if !strings.Contains(logs.String(), tc.want) || strings.Contains(logs.String(), "private diagnostic") || strings.Contains(logs.String(), "secret") {
				t.Fatalf("incorrect private diagnostic: %s", logs.String())
			}
			if p.Health(context.Background()).State != "error" {
				t.Fatal("identity failure not reflected in health")
			}
		})
	}
}
