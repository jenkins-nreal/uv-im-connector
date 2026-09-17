# Configuration

The standalone binary reads `UV_IM_*` variables.

| Variable | Purpose |
| --- | --- |
| `UV_IM_ADDR` | Listen address. Defaults to `127.0.0.1:8787`. |
| `UV_IM_STATE_DIR` | Event log and resource directory. Defaults to `.uv-im-connector`. |
| `UV_IM_PROVIDERS` | Comma-separated provider list. Supported values: `memory`, `wecom`, `lark`, `dingtalk`, `discord`, `kook`, `line`, `mail`, `matrix`, `onebot`, `qq`, `qqguild`, `slack`, `telegram`, `wechat-official`, `whatsapp`, `zulip`. |
| `UV_IM_AUTH_TOKEN` | Optional bearer token required by all HTTP/WS endpoints except `/health`. |
| `UV_WECOM_CONNECTOR_ID` | WeCom connector ID. Defaults to `wecom`. |
| `UV_WECOM_BOT_ID` | WeCom bot ID. |
| `UV_WECOM_BOT_SECRET` | WeCom bot secret. |
| `UV_WECOM_WS_URL` | Optional WeCom WebSocket endpoint override. |
| `UV_WECOM_USER_NAMES` | Optional JSON object mapping AI Bot callback `userid` values to display names, for example `{"zhangsan":"Zhang San"}`. It never changes routing or authorization. |
| `UV_WECOM_CONVERSATION_NAMES` | Optional JSON object mapping AI Bot group `chatid` values to display names, for example `{"wrxxxx":"Engineering"}`. It affects display metadata only. |
| `UV_LARK_CONNECTOR_ID` | Lark connector ID. Defaults to `lark`. |
| `UV_LARK_APP_ID` | Lark app ID. |
| `UV_LARK_APP_SECRET` | Lark app secret. |
| `UV_LARK_REGION` | `feishu` or `lark`. Defaults to `feishu`. |
| `UV_LARK_BOT_OPEN_ID` | Optional bot open ID for group mention admission (`addressed`) and mention stripping. An explicit value bypasses identity discovery. |
| `UV_LARK_BOT_UNION_ID` | Optional bot union ID for group mention admission and mention stripping. When both IDs are empty, startup discovers the bot open ID; lookup failure stops startup. |
| `UV_LARK_BASE_URL` | Optional OpenAPI base URL override. |
| `UV_LARK_CALLBACK_BASE_URL` | Optional callback WebSocket endpoint base URL override. |
| `UV_DINGTALK_CLIENT_ID` | DingTalk application Client ID. Must be configured with `UV_DINGTALK_CLIENT_SECRET`; enables Stream ingress without a public callback. |
| `UV_DINGTALK_CLIENT_SECRET` | DingTalk application Client Secret. Must be configured with `UV_DINGTALK_CLIENT_ID`. |
| `UV_<PROVIDER>_CONNECTOR_ID` | Connector ID for HTTP/webhook providers. Defaults to the provider ID. |
| `UV_<PROVIDER>_BASE_URL` | Provider API base URL override. |
| `UV_<PROVIDER>_TOKEN` | Provider API token. If the value starts with `Bearer `, `Bot `, or `Basic ` it is used as the full Authorization value. Otherwise it is sent as bearer token when the provider requires Authorization headers. |
| `UV_<PROVIDER>_WEBHOOK_SECRET` | Required shared secret for provider webhooks. Accepted on `X-UV-Webhook-Secret`, `X-Webhook-Secret`, or `?secret=`. Webhook requests are rejected when this is not configured. |
| `UV_WHATSAPP_PHONE_NUMBER_ID` | WhatsApp sender phone number ID used by outbound messages. |
| `UV_MAIL_SMTP_ADDR` | Mail outbound SMTP address, for example `smtp.example.com:587`. |
| `UV_MAIL_SMTP_USERNAME` | Mail outbound SMTP username. |
| `UV_MAIL_SMTP_PASSWORD` | Mail outbound SMTP password. |
| `UV_MAIL_FROM` | Mail outbound sender address. Defaults to `UV_MAIL_SMTP_USERNAME`. |
| `UV_MAIL_WEBHOOK_SECRET` | Mail inbound webhook secret. |

Provider credentials are exclusive deployment identity. Production, E2E, development, and temporary debug workers must not share the same provider credential set.

Lark identity discovery calls `GET /open-apis/bot/v3/info` with the existing application credentials before opening the WebSocket. Discovery and its token request share a 15-second startup deadline; a shorter caller context or HTTPClient timeout still applies. Success logs record the identity source and bot ID; failure logs retain safe stage, error-code or transport diagnostics.

Upgrade note: with both bot IDs empty, this API is a new startup prerequisite. Unknown identity does not fall back to receiving group messages. Under the standalone binary's current shared lifecycle, any provider failure stops the HTTP API and other providers in the same process; the service manager may repeatedly restart it. This path has no internal retry. To bypass identity discovery, set `UV_LARK_BOT_OPEN_ID` for the current application (or a verified union ID) and restart. This does not bypass credential validation or other startup APIs.

After acknowledging an inbound callback, Lark makes best-effort cached lookups for sender and group-chat names with the existing application credentials. The application needs OpenAPI permission to read basic user and chat information. Permission or API errors omit names. Lookups have no default timeout; an unresponsive request needs caller-context cancellation or an explicitly configured HTTPClient timeout. The WeCom AI Bot callback and Bot secret provide IDs but no contact/chat-name lookup, so deployments that need names should use the explicit maps above; without them, provider-native IDs remain the fallback.

For the generic provider variables, replace `<PROVIDER>` with one of:

```text
DINGTALK DISCORD KOOK LINE MATRIX ONEBOT QQ QQGUILD SLACK TELEGRAM WECHAT_OFFICIAL WHATSAPP ZULIP
```

When `UV_IM_PROVIDERS` is empty, the binary auto-loads only providers with detected credentials or webhook configuration. `memory` is never auto-loaded in production mode.

DingTalk supports two ingress modes. A complete `UV_DINGTALK_CLIENT_ID` and `UV_DINGTALK_CLIENT_SECRET` pair enables Stream mode and downloads inbound image, file, voice, video, and rich-text resources that carry `downloadCode`; when both are absent, the existing webhook mode remains available and validates ingress with `UV_DINGTALK_WEBHOOK_SECRET`. Supplying only one Stream credential fails startup instead of silently falling back. Both modes reply through the session webhook carried by the inbound message. `UV_DINGTALK_TOKEN` is only used for proactive messages to a configured group robot.

Connector-created HTTP clients have no total request timeout. WeCom/Lark WebSocket handshake, read/write/ACK timeouts and Lark chunk expiry are disabled by default; positive Go configuration values and caller-supplied clients remain explicit opt-ins. Heartbeat/ping scheduling and display-name cache eviction do not reject messages or attachments. Provide a cancellable context when embedding the connector.
