package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Resolve identity before opening the socket, so every event uses the same
// verified mention identity without delaying ACKs or mutating shared config.
func (p *Provider) decoderConfig(ctx context.Context) (DecoderConfig, error) {
	config := DecoderConfig{AppID: p.config.AppID, Connector: p.ConnectorID(),
		BotOpenID: strings.TrimSpace(p.config.BotOpenID), BotUnionID: strings.TrimSpace(p.config.BotUnionID)}
	if config.BotOpenID != "" || config.BotUnionID != "" {
		return config, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	token, err := p.tenantAccessToken(ctx)
	if err != nil {
		return config, fmt.Errorf("lark bot identity: authentication failed")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL()+"/open-apis/bot/v3/info", nil)
	if err != nil {
		return config, fmt.Errorf("lark bot identity: invalid endpoint")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	raw, err := p.doJSON(req, "lark bot identity")
	if err != nil {
		return config, fmt.Errorf("lark bot identity: lookup failed")
	}
	var response struct {
		Code int `json:"code"`
		Bot  struct {
			OpenID string `json:"open_id"`
		} `json:"bot"`
	}
	if json.Unmarshal(raw, &response) != nil {
		return config, fmt.Errorf("lark bot identity: invalid response")
	}
	if response.Code != 0 {
		return config, fmt.Errorf("lark bot identity: lookup failed (code=%d)", response.Code)
	}
	config.BotOpenID = strings.TrimSpace(response.Bot.OpenID)
	if config.BotOpenID == "" {
		return config, fmt.Errorf("lark bot identity: missing open_id")
	}
	return config, nil
}
