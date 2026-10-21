"""Private local chat APIs layered on the stateless SIWC inference route."""
from __future__ import annotations

import json
from typing import Any

from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

from .gateway import SIWCGateway, _validate_response_body, _event
from .sessions import SessionError, SessionStore


class SessionRoutes:
    def __init__(self, gateway: SIWCGateway, store: SessionStore):
        self.gateway = gateway
        self.store = store

    def routes(self):
        return [
            Route('/v1/siwc/sessions', self.sessions, methods=['GET', 'POST']),
            Route('/v1/siwc/sessions/{sid}/turn', self.turn, methods=['POST']),
            Route('/v1/siwc/sessions/{sid}', self.session, methods=['GET', 'DELETE']),
        ]

    def _deny(self, request):
        if not self.gateway._authorized(request):
            return JSONResponse({'error': 'Unauthorized'}, status_code=401)
        return None

    async def sessions(self, request):
        if denied := self._deny(request):
            return denied
        if request.method == 'GET':
            return JSONResponse({'sessions': self.store.list()})
        try:
            body = await request.json()
            if not isinstance(body, dict) or set(body) - {'model', 'instructions'}:
                raise SessionError('Only model and instructions are accepted')
            return JSONResponse(self.store.create(body.get('model'), body.get('instructions', '')), status_code=201)
        except (SessionError, ValueError, KeyError, TypeError) as exc:
            return JSONResponse({'error': str(exc)}, status_code=400)

    async def session(self, request):
        if denied := self._deny(request):
            return denied
        try:
            if request.method == 'DELETE':
                self.store.delete(request.path_params['sid'])
                return JSONResponse({'deleted': True})
            return JSONResponse(self.store.get(request.path_params['sid']))
        except SessionError as exc:
            return JSONResponse({'error': str(exc)}, status_code=409)

    async def turn(self, request):
        if denied := self._deny(request):
            return denied
        sid = request.path_params['sid']
        try:
            raw = await request.body()
            if len(raw) > 2_000_000:
                raise ValueError('Turn exceeds 2 MB')
            incoming = json.loads(raw)
            if not isinstance(incoming, dict) or 'input' not in incoming or not isinstance(incoming['input'], list):
                raise ValueError('Turn must include an input array')
            if set(incoming) - {'input', 'tools', 'tool_choice', 'reasoning', 'include'}:
                raise ValueError('Unsupported turn parameter')
            current = self.store.get(sid)
            composed = {'model': current['model'], 'input': current['history'] + incoming['input']}
            if current['instructions']:
                composed['instructions'] = current['instructions']
            composed.update({k: v for k, v in incoming.items() if k != 'input'})
            _validate_response_body(composed)
            if len(json.dumps(composed).encode()) > 2_000_000:
                raise ValueError('Entire conversation exceeds 2 MB')
            claim = self.store.begin(sid, incoming['input'], expected_revision=current['revision'])
        except SessionError as exc:
            return JSONResponse({'error': str(exc)}, status_code=409)
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return JSONResponse({'error': str(exc)}, status_code=400)

        class PreparedRequest:
            headers = request.headers
            async def body(self):
                return json.dumps(composed).encode()

        upstream = await self.gateway.responses(PreparedRequest())
        if not isinstance(upstream, StreamingResponse):
            self.store.finish(sid, claim['revision'], incoming['input'], [], False)
            return upstream

        async def frames():
            complete = False
            ended = False
            output: list[dict[str, Any]] = []
            text_parts: list[str] = []
            interrupted = False
            try:
                async for frame in upstream.body_iterator:
                    packet = None
                    try:
                        data = next((line[6:] for line in frame.decode().splitlines() if line.startswith('data: ')), '')
                        packet = json.loads(data)
                    except (ValueError, UnicodeError):
                        pass
                    if isinstance(packet, dict):
                        kind = packet.get('type')
                        if kind == 'response.output_item.done' and isinstance(packet.get('item'), dict):
                            item = packet['item']
                            if item.get('type', 'message') in ('message', 'reasoning', 'function_call', 'custom_tool_call'):
                                output.append(item)
                        elif kind == 'response.output_text.delta':
                            text_parts.append(packet.get('delta', ''))
                        elif kind == 'response.completed':
                            response = packet.get('response')
                            if isinstance(response, dict) and isinstance(response.get('output'), list):
                                completed_items = [item for item in response['output'] if isinstance(item, dict) and
                                          item.get('type', 'message') in ('message', 'reasoning', 'function_call', 'custom_tool_call')]
                                # SIWC store=false can leave response.completed.output empty
                                # even after response.output_item.done delivered a tool call.
                                if completed_items:
                                    output = completed_items
                            if not output and text_parts:
                                output = [{'role': 'assistant', 'content': [{'type': 'output_text', 'text': ''.join(text_parts)}]}]
                            self.store.finish(sid, claim['revision'], incoming['input'], output, True)
                            complete = ended = True
                        elif kind in ('response.failed', 'response.incomplete', 'gateway.failed', 'gateway.interrupted', 'error'):
                            interrupted = kind == 'gateway.interrupted'
                            ended = True
                    if ended and not complete:
                        self.store.finish(sid, claim['revision'], incoming['input'], [], False, interrupted)
                    yield frame
                    if ended:
                        return
            except SessionError:
                yield _event('gateway.failed', {'reason': 'session_persistence_failed'})
            finally:
                if not ended:
                    try:
                        self.store.finish(sid, claim['revision'], incoming['input'], [], False, interrupted)
                    except SessionError:
                        pass
                # Cancel the upstream iterator on client disconnect, closing HTTP.
                if hasattr(upstream.body_iterator, 'aclose'):
                    await upstream.body_iterator.aclose()

        return StreamingResponse(frames(), media_type='text/event-stream', headers={
            'Cache-Control': 'no-store', 'X-Accel-Buffering': 'no',
            'X-Inference-Request-Id': upstream.headers['X-Inference-Request-Id'],
            'X-Local-Session-Id': sid,
        })
