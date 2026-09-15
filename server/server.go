package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	uvim "github.com/hengshi/uv-im-connector"
)

type Hub struct {
	registry  *uvim.ProviderRegistry
	eventLog  *uvim.EventLog
	resources *uvim.ResourceStore
	authToken string

	mu          sync.Mutex
	subscribers map[chan uvim.Event]struct{}
	upgrader    websocket.Upgrader
}

func NewHub(registry *uvim.ProviderRegistry, eventLog *uvim.EventLog, resources *uvim.ResourceStore) *Hub {
	if registry == nil {
		registry = uvim.NewProviderRegistry()
	}
	if eventLog == nil {
		eventLog, _ = uvim.NewEventLog("")
	}
	if resources == nil {
		resources = &uvim.ResourceStore{}
	}
	hub := &Hub{
		registry:    registry,
		eventLog:    eventLog,
		resources:   resources,
		subscribers: map[chan uvim.Event]struct{}{},
		upgrader:    websocket.Upgrader{},
	}
	hub.upgrader.CheckOrigin = hub.checkOrigin
	return hub
}

func (h *Hub) SetAuthToken(token string) {
	if h != nil {
		h.authToken = strings.TrimSpace(token)
	}
}

func (h *Hub) Emit(ctx context.Context, event uvim.Event) error {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	event = h.resolveEventResources(ctx, event)
	written, fresh, err := h.eventLog.Append(ctx, event)
	if err != nil {
		return err
	}
	if !fresh {
		return nil
	}
	h.mu.Lock()
	for ch := range h.subscribers {
		select {
		case ch <- written.Sanitized():
		default:
			close(ch)
			delete(h.subscribers, ch)
		}
	}
	h.mu.Unlock()
	return nil
}

func (h *Hub) RunProviders(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(h.registry.List()))
	for _, provider := range h.registry.List() {
		provider := provider
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := provider.Run(ctx, h); err != nil && ctx.Err() == nil {
				errCh <- err
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		<-done
		return ctx.Err()
	case err := <-errCh:
		return err
	case <-done:
		return nil
	}
}

func (h *Hub) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/v1/meta", h.handleMeta)
	mux.HandleFunc("/v1/events", h.handleEvents)
	mux.HandleFunc("/v1/events/ws", h.handleEventsWS)
	mux.HandleFunc("/v1/message.create", h.handleMessageCreate)
	mux.HandleFunc("/v1/upload.create", h.handleUploadCreate)
	mux.HandleFunc("/v1/resource.download", h.handleResourceDownload)
	mux.HandleFunc("/v1/webhook/", h.handleWebhook)
	mux.HandleFunc("/v1/internal/", h.handleInternal)
	return h.authMiddleware(mux)
}

func (h *Hub) handleHealth(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *Hub) handleMeta(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var providers []uvim.ProviderMeta
	for _, provider := range h.registry.List() {
		providers = append(providers, uvim.ProviderMeta{
			Provider:     provider.ID(),
			Connector:    provider.ConnectorID(),
			Capabilities: provider.Capabilities(),
			Health:       provider.Health(req.Context()),
		})
	}
	writeJSON(w, http.StatusOK, uvim.NewServiceMeta(providers))
}

func (h *Hub) handleEvents(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	after, _ := strconv.ParseInt(req.URL.Query().Get("after"), 10, 64)
	events, err := h.eventLog.ReadAfter(req.Context(), after)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for i := range events {
		events[i] = events[i].Sanitized()
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (h *Hub) handleEventsWS(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	conn, err := h.upgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	after, _ := strconv.ParseInt(req.URL.Query().Get("after"), 10, 64)
	ch := make(chan uvim.Event, 32)
	h.mu.Lock()
	h.subscribers[ch] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.subscribers, ch)
		h.mu.Unlock()
	}()
	backlog, err := h.eventLog.ReadAfter(req.Context(), after)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = conn.WriteJSON(map[string]any{"error": err.Error()})
		return
	}
	lastSeq := after
	for _, event := range backlog {
		if err := conn.WriteJSON(event.Sanitized()); err != nil {
			return
		}
		if event.Sequence > lastSeq {
			lastSeq = event.Sequence
		}
	}
	for {
		select {
		case <-req.Context().Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			if event.Sequence <= lastSeq {
				continue
			}
			if err := conn.WriteJSON(event.Sanitized()); err != nil {
				return
			}
			lastSeq = event.Sequence
		}
	}
}

func (h *Hub) handleMessageCreate(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var msg uvim.OutboundMessage
	if err := json.NewDecoder(req.Body).Decode(&msg); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	provider := h.registry.Get(msg.Provider, msg.Connector)
	if provider == nil {
		writeError(w, http.StatusNotFound, "provider_not_found")
		return
	}
	if err := uvim.ValidateOutboundTarget(msg, provider.Capabilities()); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_target")
		return
	}
	result, err := provider.Send(req.Context(), msg)
	if err != nil {
		reason := uvim.ProviderSendErrorLogDetail(err)
		if reason == "" {
			reason = "provider send failed"
		}
		slog.Error("provider send failed", "provider", provider.ID(), "connector", provider.ConnectorID(), "reason", reason)
		writeProviderError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Hub) handleUploadCreate(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var input struct {
		Kind          string `json:"kind"`
		Name          string `json:"name"`
		MIME          string `json:"mime"`
		ContentBase64 string `json:"content_base64"`
	}
	if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	raw, err := base64.StdEncoding.DecodeString(input.ContentBase64)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	ref, err := h.resources.Save(req.Context(), bytes.NewReader(raw), uvim.ResourceRef{Kind: input.Kind, Name: input.Name, MIME: input.MIME})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ref.Sanitized())
}

func (h *Hub) handleResourceDownload(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var input uvim.ResourceDownloadRequest
	if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	provider := h.registry.Get(input.Resource.Provider, input.Resource.Connector)
	if provider == nil {
		writeError(w, http.StatusNotFound, "provider_not_found")
		return
	}
	input.Dir = h.resources.Dir
	ref, err := provider.Download(req.Context(), input)
	if err != nil {
		writeError(w, http.StatusBadGateway, "provider_download_failed")
		return
	}
	writeJSON(w, http.StatusOK, ref.Sanitized())
}

func (h *Hub) handleWebhook(w http.ResponseWriter, req *http.Request) {
	path := strings.TrimPrefix(req.URL.Path, "/v1/webhook/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusNotFound, "webhook_not_found")
		return
	}
	provider := h.registry.Get(parts[0], parts[1])
	if provider == nil {
		writeError(w, http.StatusNotFound, "provider_not_found")
		return
	}
	webhook, ok := provider.(uvim.WebhookProvider)
	if !ok {
		writeError(w, http.StatusNotFound, "webhook_not_supported")
		return
	}
	webhook.ServeWebhook(w, req, h)
}

func (h *Hub) handleInternal(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	id := strings.TrimPrefix(req.URL.Path, "/v1/internal/")
	file, ref, err := h.resources.Open("internal://" + id)
	if err != nil {
		writeError(w, http.StatusNotFound, "resource_not_found")
		return
	}
	defer file.Close()
	if ref.MIME != "" {
		w.Header().Set("Content-Type", ref.MIME)
	}
	http.ServeContent(w, req, ref.Name, time.Now(), file)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]any{"ok": false, "error": message})
}

func writeProviderError(w http.ResponseWriter, err error) {
	body := map[string]any{
		"ok":      false,
		"error":   "provider_send_failed",
		"failure": normalizedProviderFailure(err),
	}
	if detail := uvim.ProviderSendErrorDetail(err); detail != "" {
		body["detail"] = detail
	}
	writeJSON(w, http.StatusBadGateway, body)
}

func normalizedProviderFailure(err error) uvim.SendFailure {
	if failure, ok := uvim.ProviderSendFailure(err); ok {
		return failure
	}
	return uvim.SendFailure{Category: uvim.SendFailureUnknown, DeliveryState: uvim.DeliveryUnknown}
}

func (h *Hub) resolveEventResources(ctx context.Context, event uvim.Event) uvim.Event {
	if len(event.Message.Resources) == 0 {
		return event.Sanitized()
	}
	provider := h.registry.Get(event.Provider, event.Connector)
	resolved := make([]uvim.ResourceRef, len(event.Message.Resources))
	for i, ref := range event.Message.Resources {
		ref.Provider = uvim.FirstNonEmpty(ref.Provider, event.Provider)
		ref.Connector = uvim.FirstNonEmpty(ref.Connector, event.Connector)
		if provider != nil && provider.Capabilities().DownloadResource && ref.InternalURL == "" && ref.Error == "" {
			downloaded, err := provider.Download(ctx, uvim.ResourceDownloadRequest{
				Resource: ref,
				Message:  event.Message,
				Event:    event,
				Dir:      h.resources.Dir,
			})
			if err == nil {
				ref = downloaded
			} else {
				ref.Error = "download_failed"
			}
		}
		resolved[i] = ref.Sanitized()
	}
	event.Message.Resources = resolved
	event.Message.Elements = elementsFromTextAndResources(event.Message.Text, resolved)
	return event.Sanitized()
}

func elementsFromTextAndResources(text string, refs []uvim.ResourceRef) []uvim.Element {
	var out []uvim.Element
	if strings.TrimSpace(text) != "" {
		out = append(out, uvim.Text(text))
	}
	for _, ref := range refs {
		out = append(out, uvim.File(ref))
	}
	return out
}

func (h *Hub) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/health" || strings.HasPrefix(req.URL.Path, "/v1/webhook/") || h.authOK(req) {
			next.ServeHTTP(w, req)
			return
		}
		writeError(w, http.StatusUnauthorized, "unauthorized")
	})
}

func (h *Hub) authOK(req *http.Request) bool {
	if h == nil || h.authToken == "" {
		return true
	}
	token := strings.TrimSpace(req.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		token = strings.TrimSpace(token[7:])
	}
	if token == "" {
		token = strings.TrimSpace(req.URL.Query().Get("access_token"))
	}
	return token == h.authToken
}

func (h *Hub) checkOrigin(req *http.Request) bool {
	if h.authOK(req) {
		return true
	}
	origin := strings.TrimSpace(req.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	return strings.Contains(origin, "://"+req.Host)
}
