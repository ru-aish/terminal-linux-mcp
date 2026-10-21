# Headless ChatGPT-plan inference: Terminal MCP experiment

This opt-in prototype uses the **official** Sign in with ChatGPT public-client OAuth flow and the public Responses API. It does not launch the ChatGPT desktop app, reuse `.codex/auth.json`, simulate browser fingerprints, solve anti-bot challenges, or access `chatgpt.com/backend-api`. A regular system browser is needed **only for initial authorization or reauthorization**.

## One-time sign-in

Install the additional Python requirement `PyJWT[crypto]`. From this fork checkout run:

```bash
python -m siwc_gateway login
# For terminals without a browser:
python -m siwc_gateway login --no-browser
python -m siwc_gateway status
```

The loopback OAuth callback must be opened in a browser **on the same machine** as the CLI (it binds `127.0.0.1` to a random port). For headless remote hosts, perform this locally and transfer the protected credential record securely according to OpenAI's documented self-hosted VM flow. Do not publish OAuth tokens or auth URLs containing ID-token hints. Files are under `~/.config/terminal-mcp-t3/siwc/`, owner-only `0600` with `0700` directory, and never printed by the CLI. Do not commit the files.

The authorization scope includes `chatgpt.tokens.use.direct`. Access tokens expire and rotate with refresh tokens. The daemon refreshes on demand; this initial prototype supports one local account/grant at a time. Account switching requires a deliberate reauthorization.

## Optional routes in the existing T3 daemon

Add `MCP_SIWC_ENABLED=1` **only to an isolated test deployment** of `t3_terminal_server.py`. This adds routes to the already-private `127.0.0.1` control port; it does **not** modify or expose the public MCP endpoint. Use the existing private control-plane bearer token for all calls. Do **not** expose the private control port to the public internet or to other users.

- `GET /v1/siwc/models` — account-specific visible model slugs.
- `POST /v1/siwc/responses` — text/multiturn Responses stream. Body: `{"model":"<chosen-slug>","input":[{"role":"user","content":"Hello"}]}`. The daemon forces `store=false`, `stream=true`. Optional `instructions` and `reasoning` may be supplied. Send the **full conversation input** for every turn.
- `POST /v1/siwc/cancel/{request_id}` — request cancellation. The request ID is returned in `X-Inference-Request-Id` and the first `gateway.started` event.

SSE streams preserve upstream `response.*` events and include gateway status events. **Only** `response.completed` indicates success; `response.failed`, `response.incomplete`, network interruption and explicit cancellation are not successes. A stream without a terminal event ends with `gateway.failed`.

This prototype deliberately does not implement a T3 provider adapter, ChatGPT conversation import/history, durable conversation persistence, hosted OpenAI tools or Android packaging. The first deployment should remain isolated until the T3 adapter supports these endpoints and lifecycle guarantees. Existing ChatGPT UI provider remains unchanged.

### Sources

- https://developers.openai.com/siwc/token-sharing-open-source/sign-in
- https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference
- https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations
- https://developers.openai.com/siwc/token-sharing-open-source/self-hosted-vms
