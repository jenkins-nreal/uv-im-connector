package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

// Resolve identity before opening the socket, so every event uses the same
// verified mention identity without delaying ACKs or mutating shared config.
func (p *Provider) decoderConfig(ctx context.Context) (DecoderConfig, error) {
	config := DecoderConfig{AppID: p.config.AppID, Connector: p.ConnectorID(),
		BotOpenID: strings.TrimSpace(p.config.BotOpenID), BotUnionID: strings.TrimSpace(p.config.BotUnionID)}
	if config.BotOpenID != "" || config.BotUnionID != "" {
		p.config.Logger.Info("lark bot identity resolved", "connector", p.ConnectorID(), "source", "configured", "open_id", config.BotOpenID, "union_id", config.BotUnionID)
		return config, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return config, p.botIdentityFailure("authentication failed", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL()+"/open-apis/bot/v3/info", nil)
	if err != nil {
		return config, p.botIdentityFailure("invalid endpoint", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	raw, err := p.doJSON(req, "lark bot identity")
	if err != nil {
		return config, p.botIdentityFailure("lookup failed", err)
	}
	var response struct {
		Code int `json:"code"`
		Bot  struct {
			OpenID string `json:"open_id"`
		} `json:"bot"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return config, p.botIdentityFailure("invalid response", err)
	}
	if response.Code != 0 {
		return config, p.botIdentityFailure(fmt.Sprintf("lookup failed (code=%d)", response.Code), nil)
	}
	config.BotOpenID = strings.TrimSpace(response.Bot.OpenID)
	if config.BotOpenID == "" {
		return config, p.botIdentityFailure("missing open_id", nil)
	}
	p.config.Logger.Info("lark bot identity resolved", "connector", p.ConnectorID(), "source", "discovered", "open_id", config.BotOpenID)
	return config, nil
}

// Keep public error text fixed while preserving errors.Is/As for callers.
type botIdentityError struct {
	stage string
	cause error
}

func (e *botIdentityError) Error() string { return "lark bot identity: " + e.stage }
func (e *botIdentityError) Unwrap() error { return e.cause }

func (p *Provider) botIdentityFailure(stage string, cause error) error {
	p.config.Logger.Warn("lark bot identity resolution failed", "connector", p.ConnectorID(), "stage", stage, "detail", uvim.ProviderSendErrorLogDetail(cause))
	return &botIdentityError{stage: stage, cause: cause}
}
