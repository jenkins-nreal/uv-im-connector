# Resources

Inbound files, images, audio, and video are normalized into `ResourceRef` values.

The connector imposes no additional attachment size limit on inbound downloads, local storage, HTTP uploads, or outbound delivery. The IM provider or mail server enforces its size policy.

Group and direct attachments use the same download path, before event persistence. WeCom extracts files, images, videos, and quoted text from the message and its `quote`, including a group-file quote sent with an @mention of the bot. DingTalk exchanges inbound `downloadCode` values for temporary file URLs when Stream credentials are configured. Lark extracts images and videos from rich-text posts. WeCom AI Bot callbacks have `addressed=true`; Lark group messages still derive that flag from bot mentions. The connector can only process events delivered by the platform; caller applications perform content recognition.

## Public Shape

```json
{
  "id": "res_xxx",
  "provider": "lark",
  "connector": "main",
  "kind": "file",
  "name": "report.pdf",
  "internal_url": "internal://res_xxx",
  "mime": "application/pdf",
  "size_bytes": 12043,
  "sha256": "..."
}
```

Provider-private fields are removed from public events:

- temporary download URLs;
- encrypted payload keys;
- provider resource IDs that should not be exposed;
- webhook secrets;
- raw provider payload metadata.

## Resolve an Internal Resource

Use the internal URL through the connector HTTP API:

```text
GET /v1/internal/<id>
```

The Go client exposes the same operation:

```go
resp, err := c.ResolveInternalURL(ctx, event.Message.Resources[0].InternalURL)
```

Callers should copy allowed files into caller-owned storage before starting long-running work. The connector resource store is infrastructure state, not the caller application's artifact store.

## Explicit Provider Download

Trusted callers can ask the provider adapter to resolve a provider-private resource:

```text
POST /v1/resource.download
```

The request uses `ResourceDownloadRequest` and returns a sanitized `ResourceRef`.

## Upload Local Bytes

Use `POST /v1/upload.create` to create an internal resource from local bytes before sending it through a provider that supports outbound resources.

```json
{
  "kind": "file",
  "name": "report.txt",
  "mime": "text/plain",
  "content_base64": "..."
}
```

Before sending, inspect the exact provider and connector in `GET /v1/meta`, require `upload_resource` and the desired `resource_kinds`, and pass the complete `ResourceRef` returned by `upload.create` in `OutboundMessage.resources`. Do not construct an `internal_url` or reuse one from another uv-im-connector process.

In the standalone binary, WeCom, Lark / Feishu, Discord, KOOK, Telegram, Matrix, Slack, WhatsApp, Zulip, WeChat Official Account, and Mail share the HTTP upload resource store and declare `upload_resource=true`. See the [provider capability matrix](/en/architecture.html#provider-capability-matrix) for every provider.

- WeCom: WebSocket upload uses 512 KiB chunks, and the provider validates total size and chunk count.
- Lark / Feishu: Supported image formats use the image API regardless of size; other resources use file upload. The provider validates size.
- Discord: Direct multipart message upload; the provider validates attachment size.
- KOOK: Asset upload followed by an image or attachment-card message.
- Telegram: Multipart Bot API upload; unsupported native formats fall back to documents.
- Matrix: Content-repository upload followed by an `mxc://` room message.
- Slack: External upload URL, raw upload, then completion and channel share.
- WhatsApp: Cloud API media upload followed by a message referencing the media ID.
- Zulip: Simple user upload followed by a Markdown attachment link.
- WeChat Official Account: Temporary-media upload plus customer-service send; no arbitrary file message.
- Mail: Sent as MIME attachments; the mail server validates size and count.

The remaining providers can currently receive and download resources but cannot send bytes from `internal://`; the matrix names the missing provider-native upload flow for each one. A send accepts text and multiple resources without a connector-imposed count limit. Discord, Mail, and Zulip combine them in one native message; the other upload-capable adapters send text first, then each resource in order (Slack also keeps its single-file caption form). Ordered sends return all IDs in `message_ids` and the last ID in `message_id`. On partial failure, `failure.delivered_count` and `failure.delivered_message_ids` identify completed messages; `retryable=false` and `delivery_state=unknown` prohibit replaying the whole sequence. The failed part may have an ambiguous outcome.

Providers that do not support a requested outbound resource kind should return explicit errors instead of silently dropping content.

The connector does not impose byte/count caps on resources, webhook bodies, Lark event assembly, event-log records, or provider-response reads. Empty resources reach the provider for validation. Text, safe filename components, and error metadata are not length-truncated. Authentication, path/header safety, supported message formats, and native protocol validation still apply. Memory, filesystem, standard-library transport, proxy, and provider constraints remain external boundaries.
