# Standalone ChatGPT-plan daemon: deployment and verified behavior

This module provides a **headless daemon independent of ChatGPT Desktop**. It uses the officially documented Sign in with ChatGPT (SIWC) OAuth flow and direct `https://api.openai.com/v1/responses` requests. It does not use Codex, the ChatGPT desktop process, cookies, private ChatGPT APIs, browser impersonation, or the existing T3 ChatGPT provider implementation.

## Install and run

From this fork, install just the standalone dependencies:

```sh
python3 -m pip install -r requirements-siwc.txt
python3 -m siwc_gateway login
export MCP_SIWC_CONTROL_TOKEN="$(python3 -c 'import secrets;print(secrets.token_urlsafe(48))')"
python3 -m siwc_gateway serve --port 8768
```

Keep the control token securely for clients; do not print it in logs or put it in a command-line flag. The token must contain at least 24 characters. The server binds **127.0.0.1 only**, suppresses HTTP access logging, and refuses a public interface. For remote or Android access, use an authenticated, encrypted tunnel or a device-local deployment. It does not require ChatGPT Desktop during login or inference; initial OAuth authorization needs a regular browser.

The default local conversation store is `~/.config/terminal-mcp-t3/siwc/sessions.sqlite3`. Set `MCP_SIWC_SESSION_HOME` to a private directory to separate local chat history from the OAuth credential store. A one-process file lock prevents two daemons from independently recovering the same running session. Restart converts any unconfirmed in-flight turns to `interrupted` without appending a fictitious assistant response.

All endpoints require `Authorization: Bearer $MCP_SIWC_CONTROL_TOKEN`. The standalone daemon provides the original private `/v1/siwc/status`, `/capabilities`, `/models`, `/responses`, `/cancel/{request_id}` endpoints plus:

| Method | Path | Behavior |
| --- | --- | --- |
| POST | `/v1/siwc/sessions` | Create a local conversation with a model and optional instructions |
| GET | `/v1/siwc/sessions` | List locally persisted conversations (up to 100) |
| GET | `/v1/siwc/sessions/{id}` | Read the local conversation, including replayable history |
| DELETE | `/v1/siwc/sessions/{id}` | Delete an idle or stopped local conversation |
| POST | `/v1/siwc/sessions/{id}/turn` | Stream the next turn using saved history and commit only on `response.completed` |

For a tool turn, supply the namespaced function schema in `tools`, wait for `response.output_item.done` with a `function_call`, perform the tool **on the client side under the user's control**, and send `function_call_output` with its `call_id` as the next session input. The daemon never executes arbitrary tools suggested by model output.

For stateless GPT reasoning replay, callers may set `include: ["reasoning.encrypted_content"]` on a turn. Returned reasoning items are included in the local transcript for subsequent turns when available. The server forces `store=false, stream=true` for every upstream request, and rejects unsupported parameters and hosted tools.

## Repeatable live acceptance test

Run the daemon first, then in a separate shell with the **same** locally secured control token:

```sh
python3 scripts/siwc_smoke.py --base http://127.0.0.1:8768 --model gpt-6.1-sol
```

The script exits nonzero if any check fails and prints machine-readable booleans without printing OAuth tokens, access/refresh credentials, or user identity. This test consumes some of the connected ChatGPT plan's model usage.

### Verified on 2026-10-21

- Credential grant present (not printed); model catalog HTTP 200, seven eligible model choices including GPT-6.1 Sol.
- Real standalone OS process: private API rejected unauthorized access with HTTP 401.
- Two live streamed GPT-6.1 Sol turns returned exactly `FIRST_OK`, with `response.completed` both times; four replayable history entries stored.
- Stopping and restarting the daemon retained all four conversation history entries.
- Live namespaced `echo` function call emitted, local function output resubmitted, second inference completed, session returned from `waiting_tool` to `idle`.
- Live `/cancel` returned HTTP 200 and `gateway.interrupted`, never `response.completed`, and later returned 404 for the completed cancellation record.
- Closing a client stream early left the local conversation in `failed`, not `running`.
- Live inference using `include: ["reasoning.encrypted_content"]` succeeded.
- Focused offline unit/regression suite: 30 tests passed, including HTTP failure, cancellation, OAuth validation, token refresh, SSE framing, session persistence, tool output with empty terminal `response.output`, and interruption recovery.
- `scripts/siwc_smoke.py` completed its entire live test suite with all reported results `true`.

## Explicitly not verified or not supported

- **Not native ChatGPT conversation parity:** The OAuth permission cannot retrieve the user's ChatGPT conversation history, ChatGPT projects, memories, native hosted connectors, or private app UI semantics. The local history is separate. See https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations .
- **Not wired into T3 Code's existing ChatGPT provider yet:** To replace the provider in the T3 UI, an explicit T3 adapter must route model discovery, tool execution, conversation storage, and cancellation to this daemon. Existing T3 providers are unchanged.
- **Not an Android APK:** The daemon is a normal Python ASGI service suitable for Linux and potentially an Android Python environment, but APK packaging and device integration need independent testing.
- **Not a claim of exhaustive reliability:** Automated and live checks cover key paths, not every network failure, OS crash, token expiry, model, content type, or device.

Docs: https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference and https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations .
