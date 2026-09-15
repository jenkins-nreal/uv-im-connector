# 资源与文件

入站文件、图片、音频和视频会被标准化为 `ResourceRef`。

Connector 不对附件大小设置额外上限：入站下载、本地存储、HTTP 上传和出站发送均不按字节数拦截，大小限制由 IM 平台或邮件服务器执行。

群聊和私聊使用相同的附件下载链路，资源会在事件持久化前下载。企业微信会提取消息本身以及 `quote` 中的文件、图片、视频和引用文本，支持引用群文件后 @ 机器人；DingTalk 在配置 Stream 凭据时会用入站 `downloadCode` 换取临时文件 URL；飞书会提取图文消息中的图片和视频，也会提取被引用父消息的可下载附件。飞书 `folder` 会作为文件资源识别，但平台资源 API 不保证可以下载文件夹；`merge_forward`（包括子消息）和 `interactive` 消息卡片只提供正文/摘要，不暴露子资源。企业微信 AI Bot 回调标记为 `addressed=true`，飞书群消息仍按是否 @ 机器人设置该字段。Connector 只能处理平台实际投递的消息；文件内容识别由调用方应用负责。

## 公开形态

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

公开事件会移除 provider-private 字段：

- 临时下载 URL；
- 加密 payload key；
- 不应暴露的 provider resource ID；
- webhook secret；
- provider 原始 payload metadata。

## 解析内部资源

通过 connector HTTP API 解析 internal URL：

```text
GET /v1/internal/<id>
```

Go client 暴露同等能力：

```go
resp, err := c.ResolveInternalURL(ctx, event.Message.Resources[0].InternalURL)
```

调用方应该在启动长耗时任务前，把允许的文件复制到调用方自己的存储。Connector 的 resource store 是基础设施状态，不是调用方应用的 artifact store。

## 显式下载 Provider 资源

可信调用方可以请求 provider adapter 把 provider-private resource 解析为内部资源：

```text
POST /v1/resource.download
```

请求使用 `ResourceDownloadRequest`，返回 sanitized `ResourceRef`。

## 上传本地字节

使用 `POST /v1/upload.create` 从本地字节创建内部资源，再交给支持 outbound resource 的 provider 发送。

```json
{
  "kind": "file",
  "name": "report.txt",
  "mime": "text/plain",
  "content_base64": "..."
}
```

发送前必须从 `GET /v1/meta` 校验目标 provider + connector 的 `upload_resource` 和 `resource_kinds`，并把 `upload.create` 返回的完整 `ResourceRef` 原样放进 `OutboundMessage.resources`。不能自行拼装 `internal_url`，也不能拿另一个 uv-im-connector 进程的内部 URL 来用。

standalone binary 中，WeCom、Lark / Feishu、Discord、KOOK、Telegram、Matrix、Slack、WhatsApp、Zulip、WeChat Official Account 和 Mail 与 HTTP upload endpoint 使用同一个 resource store，并真正声明 `upload_resource=true`。完整逐 provider 清单见 [Provider 能力矩阵](/architecture.html#provider-能力矩阵)。

- WeCom：WebSocket 上传每片 512 KiB，总大小和分片数量由平台校验。
- Lark / Feishu：支持格式的图片始终使用图片 API，不按大小转为文件；其他资源走文件上传，由平台校验大小。
- Discord：资源随消息直接 multipart 上传，由平台校验大小。
- KOOK：先上传 asset，再发送图片消息或附件卡片。
- Telegram：Bot API multipart 上传；不符合原生格式时降级为 document。
- Matrix：先上传 content repository，再以 `mxc://` room message 发送。
- Slack：申请 external upload URL、上传原始字节，再 complete 并分享到 channel。
- WhatsApp：Cloud API media upload 后再引用 media ID 发送消息。
- Zulip：simple user upload 后发送 Markdown 附件链接。
- WeChat Official Account：临时素材上传后通过客服消息发送；不支持任意文件。
- Mail：作为 MIME 附件发送；大小和数量由邮件服务器校验。

其余 provider 当前只能接收 / 下载资源，不能从 `internal://` 直接发送；矩阵逐项记录了缺失的 provider-native 上传环节。一次发送可同时提交 text 和多个 resources，connector 不设附件数量上限。Discord、Mail、Zulip 将它们合并为一条平台消息；其他支持上传的适配器先发送文本，再按顺序发送各附件（Slack 单附件仍可携带说明文字）。顺序发送的结果通过 `message_ids` 返回全部 ID，`message_id` 为最后一条 ID。中途失败时，`failure.delivered_count` 和 `failure.delivered_message_ids` 标明已完成部分，`retryable=false`、`delivery_state=unknown` 表示不能重放整批；失败的那一条仍可能已送达。

Provider 不支持某种 outbound resource 时，应该返回显式错误，而不是静默丢弃内容。

connector 不对资源、webhook 请求体、飞书事件拼装、事件日志记录或平台响应读取设置字节数/条数上限。空资源交由平台校验；文本、安全文件名片段和错误元数据不按长度截断。鉴权、路径/请求头安全、支持的消息格式和原生协议校验仍然有效。内存、文件系统、标准库传输、代理和平台自身的约束仍是外部边界。
