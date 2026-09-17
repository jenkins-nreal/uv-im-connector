package lark

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Startup must fail explicitly before opening a socket with no mention identity.
func TestBotIdentityFailureBeforeTransportStub(t *testing.T) {
	for _, body := range []string{`{"code":99991672,"msg":"private diagnostic"}`, `{"code":0,"bot":{}}`, `{`} {
		t.Run(body, func(t *testing.T) {
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/open-apis/auth/v3/tenant_access_token/internal":
					w.Write([]byte(`{"code":0,"tenant_access_token":"token","expire":3600}`))
				case "/open-apis/bot/v3/info":
					w.Write([]byte(body))
				default:
					t.Error("transport started without bot identity")
					w.WriteHeader(500)
				}
			}))
			defer api.Close()
			p, err := New(Config{AppID: "app", AppSecret: "secret", BaseURL: api.URL})
			if err != nil {
				t.Fatal(err)
			}
			err = p.Run(context.Background(), nil)
			if err == nil || !strings.Contains(err.Error(), "lark bot identity:") || strings.Contains(err.Error(), "private diagnostic") {
				t.Fatalf("unexpected startup error: %v", err)
			}
			if p.Health(context.Background()).State != "error" {
				t.Fatal("identity failure not reflected in health")
			}
		})
	}
}
