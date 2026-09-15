package lark

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	uvim "github.com/hengshi/uv-im-connector"
)

type DecoderConfig struct {
	AppID      string
	BotOpenID  string
	BotUnionID string
	Connector  string
}

func DecodePayload(payload []byte, config DecoderConfig) (uvim.Event, bool, error) {
	if len(payload) == 0 {
		return uvim.Event{}, false, nil
	}
	var env eventEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return uvim.Event{}, false, fmt.Errorf("envelope: %w", err)
	}
	if env.Type != "" && env.Type != "event_callback" {
		return uvim.Event{}, false, nil
	}
	if env.Header.EventType != "im.message.receive_v1" {
		return uvim.Event{}, false, nil
	}
	if len(env.Event) == 0 {
		return uvim.Event{}, false, fmt.Errorf("empty event payload")
	}
	var evt messageReceiveEvent
	if err := json.Unmarshal(env.Event, &evt); err != nil {
		return uvim.Event{}, false, fmt.Errorf("event: %w", err)
	}
	resources := messageResources(evt.Message.MessageType, evt.Message.Content)
	body := messageBody(evt.Message.MessageType, evt.Message.Content)
	body = resolveMentions(body, evt.Message.Mentions, config.BotOpenID, config.BotUnionID)
	channelType := normalizeChatType(evt.Message.ChatType)
	messageID := evt.Message.MessageID
	channelID := evt.Message.ChatID
	for i := range resources {
		resources[i].Provider = "lark"
		resources[i].Connector = config.Connector
		if resources[i].Metadata == nil {
			resources[i].Metadata = map[string]string{}
		}
		resources[i].Metadata["message_id"] = messageID
	}
	createdAt := parseMillis(evt.Message.CreateTime)
	return uvim.Event{
		ID:        env.Header.EventID,
		Type:      uvim.EventMessageCreate,
		Provider:  "lark",
		Connector: config.Connector,
		Time:      time.Now().UTC(),
		Login:     uvim.Login{Platform: "lark", Connector: config.Connector, ID: config.AppID},
		Channel:   uvim.Channel{ID: channelID, Type: channelType},
		User: uvim.User{
			ID: uvim.FirstNonEmpty(evt.Sender.SenderID.OpenID, evt.Sender.SenderID.UserID, evt.Sender.SenderID.UnionID),
		},
		Message: uvim.Message{
			ID:        messageID,
			Type:      evt.Message.MessageType,
			Text:      body,
			Elements:  elementsFromTextAndResources(body, resources),
			Resources: resources,
			CreatedAt: createdAt,
		},
		Referrer: uvim.Referrer{
			MessageID:       messageID,
			ParentMessageID: evt.Message.ParentID,
			RootMessageID:   evt.Message.RootID,
			ChannelID:       channelID,
			Target:          &uvim.OutboundTarget{ID: channelID, Kind: uvim.TargetConversation},
		},
		Addressed: channelType != uvim.ChannelGroup || containsBotMention(evt.Message.Mentions, config.BotOpenID, config.BotUnionID),
	}, true, nil
}

type eventEnvelope struct {
	Schema string          `json:"schema"`
	Type   string          `json:"type"`
	Header eventHeader     `json:"header"`
	Event  json.RawMessage `json:"event"`
}

type eventHeader struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	CreateTime string `json:"create_time"`
	AppID      string `json:"app_id"`
	TenantKey  string `json:"tenant_key"`
}

type messageReceiveEvent struct {
	Sender struct {
		SenderID struct {
			OpenID  string `json:"open_id"`
			UnionID string `json:"union_id"`
			UserID  string `json:"user_id"`
		} `json:"sender_id"`
		SenderType string `json:"sender_type"`
		TenantKey  string `json:"tenant_key"`
	} `json:"sender"`
	Message struct {
		MessageID   string    `json:"message_id"`
		ParentID    string    `json:"parent_id"`
		RootID      string    `json:"root_id"`
		ChatID      string    `json:"chat_id"`
		ChatType    string    `json:"chat_type"`
		MessageType string    `json:"message_type"`
		Content     string    `json:"content"`
		Mentions    []mention `json:"mentions"`
		CreateTime  string    `json:"create_time"`
	} `json:"message"`
}

type mention struct {
	Key string `json:"key"`
	ID  struct {
		OpenID  string `json:"open_id"`
		UnionID string `json:"union_id"`
		UserID  string `json:"user_id"`
	} `json:"id"`
	Name string `json:"name"`
}

func flattenContent(msgType, rawContent string) string {
	switch msgType {
	case "text":
		var doc struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal([]byte(rawContent), &doc); err != nil {
			return ""
		}
		return doc.Text
	case "post":
		return flattenPost(rawContent)
	default:
		return ""
	}
}

func messageBody(msgType, rawContent string) string {
	switch msgType {
	case "text", "post":
		return flattenContent(msgType, rawContent)
	case "image":
		return "[Image]"
	case "file":
		return "[File]"
	case "folder":
		return "[Folder]"
	case "audio":
		return "[Audio]"
	case "media":
		return "[Video]"
	case "sticker":
		return "[Sticker]"
	case "interactive":
		return "[Interactive card]"
	case "share_chat":
		return shareTargetBody("chat", rawContent)
	case "share_user":
		return shareTargetBody("user", rawContent)
	case "system":
		return "[System message]"
	case "merge_forward":
		return "[forwarded messages]"
	default:
		return unsupportedMessageBody(msgType)
	}
}

func messageResources(msgType, rawContent string) []uvim.ResourceRef {
	if strings.TrimSpace(rawContent) == "" {
		return nil
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(rawContent), &doc); err != nil {
		return nil
	}
	switch msgType {
	case "post":
		var post postContent
		if err := json.Unmarshal([]byte(rawContent), &post); err != nil {
			return nil
		}
		var refs []uvim.ResourceRef
		for _, paragraph := range post.Content {
			for _, span := range paragraph {
				switch span.Tag {
				case "img":
					if span.ImageKey != "" {
						refs = append(refs, uvim.ResourceRef{Kind: uvim.ElementImage, Key: span.ImageKey})
					}
				case "media":
					if span.FileKey != "" {
						refs = append(refs, uvim.ResourceRef{Kind: uvim.ElementVideo, Key: span.FileKey})
					}
				}
			}
		}
		return refs
	case "image":
		key := uvim.StringValue(doc["image_key"])
		if key == "" {
			return nil
		}
		return []uvim.ResourceRef{{Kind: uvim.ElementImage, Key: key}}
	case "file", "folder", "audio", "media":
		key := uvim.StringValue(doc["file_key"])
		if key == "" {
			return nil
		}
		kind := msgType
		if msgType == "folder" {
			kind = uvim.ElementFile
		}
		if msgType == "media" {
			kind = uvim.ElementVideo
		}
		return []uvim.ResourceRef{{Kind: kind, Name: uvim.FirstNonEmpty(uvim.StringValue(doc["file_name"]), uvim.StringValue(doc["name"])), Key: key}}
	default:
		return nil
	}
}

func shareTargetBody(kind, rawContent string) string {
	var doc map[string]any
	if err := json.Unmarshal([]byte(rawContent), &doc); err == nil {
		key := kind + "_id"
		if id := uvim.StringValue(doc[key]); id != "" {
			return fmt.Sprintf("[Shared %s: %s]", kind, id)
		}
	}
	return fmt.Sprintf("[Shared %s]", kind)
}

func unsupportedMessageBody(messageType string) string {
	messageType = strings.TrimSpace(messageType)
	if messageType == "" {
		return "[Unsupported message]"
	}
	return fmt.Sprintf("[Unsupported message: %s]", messageType)
}

type postContent struct {
	Title   string       `json:"title"`
	Content [][]postSpan `json:"content"`
}

type postSpan struct {
	ImageKey string `json:"image_key"`
	FileKey  string `json:"file_key"`
	Tag      string `json:"tag"`
	Text     string `json:"text"`
	Href     string `json:"href"`
	UserID   string `json:"user_id"`
	UserName string `json:"user_name"`
}

func flattenPost(raw string) string {
	var doc postContent
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return ""
	}
	var lines []string
	if strings.TrimSpace(doc.Title) != "" {
		lines = append(lines, doc.Title)
	}
	for _, para := range doc.Content {
		parts := make([]string, 0, len(para))
		for _, span := range para {
			switch span.Tag {
			case "text", "code_block":
				if span.Text != "" {
					parts = append(parts, span.Text)
				}
			case "a":
				if span.Text != "" && span.Href != "" {
					parts = append(parts, span.Text+" ("+span.Href+")")
				} else if span.Text != "" {
					parts = append(parts, span.Text)
				} else if span.Href != "" {
					parts = append(parts, span.Href)
				}
			case "at":
				if span.UserName != "" {
					parts = append(parts, "@"+span.UserName)
				} else if span.UserID != "" {
					parts = append(parts, span.UserID)
				}
			case "img":
				parts = append(parts, "[Image]")
			case "media":
				parts = append(parts, "[Video]")
			default:
				if span.Text != "" {
					parts = append(parts, span.Text)
				}
			}
		}
		lines = append(lines, strings.Join(parts, " "))
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func resolveMentions(text string, mentions []mention, botOpenID, botUnionID string) string {
	if text == "" || len(mentions) == 0 {
		return text
	}
	sorted := make([]mention, 0, len(mentions))
	for _, mention := range mentions {
		if mention.Key != "" {
			sorted = append(sorted, mention)
		}
	}
	sort.SliceStable(sorted, func(i, j int) bool { return len(sorted[i].Key) > len(sorted[j].Key) })
	var out strings.Builder
	for i := 0; i < len(text); {
		var matched *mention
		for idx := range sorted {
			if strings.HasPrefix(text[i:], sorted[idx].Key) {
				matched = &sorted[idx]
				break
			}
		}
		if matched == nil {
			out.WriteByte(text[i])
			i++
			continue
		}
		end := i + len(matched.Key)
		if isBotMention(*matched, botOpenID, botUnionID) {
			if end < len(text) && text[end] == ' ' {
				end++
			}
			i = end
			continue
		}
		if matched.Name != "" {
			out.WriteByte('@')
			out.WriteString(matched.Name)
		} else {
			out.WriteString(matched.Key)
		}
		i = end
	}
	return strings.TrimSpace(out.String())
}

func isBotMention(mention mention, botOpenID, botUnionID string) bool {
	if botUnionID != "" {
		return mention.ID.UnionID == botUnionID
	}
	if botOpenID != "" {
		return mention.ID.OpenID == botOpenID
	}
	return false
}

func containsBotMention(mentions []mention, botOpenID, botUnionID string) bool {
	for _, mention := range mentions {
		if isBotMention(mention, botOpenID, botUnionID) {
			return true
		}
	}
	return false
}

func normalizeChatType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "group":
		return uvim.ChannelGroup
	case "p2p":
		return uvim.ChannelDirect
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func elementsFromTextAndResources(text string, refs []uvim.ResourceRef) []uvim.Element {
	var out []uvim.Element
	if strings.TrimSpace(text) != "" {
		out = append(out, uvim.Text(text))
	}
	for _, ref := range refs {
		out = append(out, uvim.File(ref.Sanitized()))
	}
	return out
}

func parseMillis(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	var n int64
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return time.Time{}
		}
		n = n*10 + int64(ch-'0')
	}
	if n == 0 {
		return time.Time{}
	}
	return time.UnixMilli(n).UTC()
}
