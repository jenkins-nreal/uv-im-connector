# jarvis-box Integration

jarvis-box should treat `uv-im-connector` as an external IM infrastructure dependency.

## Boundary

`uv-im-connector` owns:

- provider credentials and connection lifecycle;
- inbound IM event normalization;
- outbound IM messages;
- provider resource download and internal resource references;
- provider health and capabilities;
- connector service version and protocol version.

jarvis-box owns:

- Target, Task, Run, Workspace, and runtime-agent lifecycle;
- native resume and handoff;
- run artifacts such as `prompt.txt`, `run-context.json`, and `reply.md`;
- team visibility, redaction, status UI, and retry policy.

## Replacement Path

1. Configure and deploy `uv-im-connector` as an external service.
2. jarvis-box reads `/v1/meta` at startup and verifies `service == "uv-im-connector"` plus a supported `protocol_version`.
3. jarvis-box watches `/v1/events/ws`.
4. Each normalized `message.create` event maps to a jarvis-box Target using `provider + connector + channel.id`.
   Optional `channel.name`, `user.name`, and `user.display_name` are presentation snapshots only; jarvis-box must not use them for Target identity, ACL, dedupe, or reply routing.
5. jarvis-box resolves resources through `internal_url`, then copies allowed files into the Run attachment directory.
6. jarvis-box starts or continues a Task/Run using its existing runtime-agent model.
7. jarvis-box sends final replies through `POST /v1/message.create`.
8. On failure, jarvis-box persists the normalized `failure`, applies its own bounded retry policy, and keeps delivery state separate from agent Run state.

### Quoted Lark messages

For an addressed Lark message with a parent message ID, the connector reads that
one parent with its bot identity and verifies the returned message and chat IDs.
It exposes the quoted text and source ID as a `quoted-message.txt` resource, and
adds the parent's files to the same resource list. File downloads use each
resource's source message ID. The current command text, addressing decision and
reply target are unchanged; quoted text is reference material, not a new command.
The connector does not walk the root/thread history or recursively fetch quotes.

An unavailable, deleted or inaccessible parent produces a resource with a
normalized `quoted_message_*` error; a file download failure remains
`download_failed`. Consumers must preserve these errors in the Agent input rather
than reporting that the user supplied no attachment. Existing clients can consume
this through the resource protocol without gaining provider credentials.

Lark `folder` messages are recognized as file resources, including when quoted.
Recognition does not guarantee that Lark's resource API can download the folder.
If it fails, the context identifies the original folder and asks for a ZIP archive
or individual files. Do not claim that folder contents were retrieved based only
on its metadata or a successful local transport stub.

## Release Boundary

jarvis-box does not host, spawn, or auto-update `uv-im-connector`. A connector bugfix that keeps `protocol_version` compatible is deployed by upgrading the connector service. jarvis-box needs a dependency bump and release only when it consumes new Go client/API behavior or when the connector protocol becomes incompatible with the supported protocol set.

## Required E2E Coverage

- Direct conversation message creates a Run.
- Group/channel mention creates a Run under the same Target model.
- Optional sender/conversation display names survive event-log replay and appear in status without replacing provider-native IDs.
- Startup fails clearly when `/v1/meta` reports a wrong service or unsupported protocol version.
- File, image, audio, and video resources resolve through `internal_url`.
- Reply uses `OutboundMessage.Referrer`.
- Provider credentials and raw payload fields do not appear in public status or artifacts.
- Duplicate event IDs do not create duplicate Runs.
- Connector reconnect does not require jarvis-box to know provider-specific state.
- Every provider send failure reaches jarvis-box through the same normalized failure contract.
- Safe retryable rejection/not-attempted failures are retried without parsing text; ambiguous delivery is not replayed automatically.
- A failed final reply reuses the persisted provider-ready result and does not rerun the agent.

## Non-Goals

jarvis-box must not parse provider-native payloads after the replacement. If a provider needs special handling, that handling belongs in `uv-im-connector` and must be exposed through normalized fields or capabilities.
