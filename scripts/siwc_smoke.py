#!/usr/bin/env python3
"""Opt-in, real-token acceptance test for a separately running SIWC daemon.

Reads only the local control bearer from the environment. No Desktop, Codex,
private endpoints, token output, or browser automation is involved.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os

import httpx


def packets(response: httpx.Response) -> list[dict]:
    result = []
    for line in response.text.splitlines():
        if line.startswith('data: '):
            try:
                result.append(json.loads(line[6:]))
            except ValueError:
                raise RuntimeError('Malformed SSE packet') from None
    return result


def complete(stream: list[dict]) -> bool:
    return any(packet.get('type') == 'response.completed' for packet in stream) and not any(
        packet.get('type') in ('response.failed', 'response.incomplete', 'gateway.failed', 'gateway.interrupted', 'error')
        for packet in stream)


async def run(base: str, token: str, model: str) -> dict:
    headers = {'Authorization': 'Bearer ' + token}
    async with httpx.AsyncClient(base_url=base, timeout=100) as client:
        assert (await client.get('/v1/siwc/models')).status_code == 401, 'Public model catalog access'
        models = await client.get('/v1/siwc/models', headers=headers)
        models.raise_for_status()
        assert model in [m['slug'] for m in models.json()['models']], 'Requested model not available'

        async def session():
            r = await client.post('/v1/siwc/sessions', headers=headers, json={'model': model})
            r.raise_for_status()
            return '/v1/siwc/sessions/' + r.json()['id']

        path = await session()
        for prompt in ('Say exactly STANDALONE_OK.', 'Repeat the exact token you said in your immediately preceding reply.'):
            r = await client.post(path + '/turn', headers=headers, json={'input': [{'role': 'user', 'content': prompt}]})
            r.raise_for_status()
            events = packets(r)
            assert complete(events), 'Multi-turn stream did not complete'
            text = ''.join(x.get('delta', '') for x in events if x.get('type') == 'response.output_text.delta').strip()
            assert text == 'STANDALONE_OK', 'Model did not retain the previous response'
        saved = (await client.get(path, headers=headers)).json()
        assert saved['state'] == 'idle' and len(saved['history']) == 4, 'Local history not saved'

        tool = {'type': 'namespace', 'name': 'local', 'description': 'Local echo tools', 'tools': [
            {'type': 'function', 'name': 'echo', 'description': 'Echo a string', 'strict': True,
             'parameters': {'type': 'object', 'properties': {'message': {'type': 'string'}},
                            'required': ['message'], 'additionalProperties': False}}]}
        tool_path = await session()
        r = await client.post(tool_path + '/turn', headers=headers,
                              json={'input': [{'role': 'user', 'content': 'Call local.echo with message TOOL_OK and wait for its result.'}], 'tools': [tool]})
        calls = [x['item'] for x in packets(r) if x.get('type') == 'response.output_item.done'
                 and x.get('item', {}).get('type') == 'function_call']
        assert complete(packets(r)) and calls, 'Function call missing or failed'
        assert (await client.get(tool_path, headers=headers)).json()['state'] == 'waiting_tool'
        result = await client.post(tool_path + '/turn', headers=headers, json={
            'input': [{'type': 'function_call_output', 'call_id': calls[0]['call_id'], 'output': 'TOOL_OK'}], 'tools': [tool]})
        assert complete(packets(result)), 'Tool continuation did not complete'
        assert (await client.get(tool_path, headers=headers)).json()['state'] == 'idle'

        interrupt_payload = {'model': model, 'input': [{'role': 'user',
            'content': 'Write a detailed 2000-word report on maritime navigation.'}]}
        cancelled = False
        async with client.stream('POST', '/v1/siwc/responses', headers=headers, json=interrupt_payload) as response:
            response.raise_for_status()
            rid = response.headers['x-inference-request-id']
            async for line in response.aiter_lines():
                if line == 'event: gateway.started':
                    outcome = await client.post('/v1/siwc/cancel/' + rid, headers=headers)
                    assert outcome.status_code == 200 and outcome.json()['cancellationRequested']
                if line == 'event: gateway.interrupted':
                    cancelled = True
                    break
                if line == 'event: response.completed':
                    raise AssertionError('Cancelled request completed successfully')
        assert cancelled, 'Cancellation did not signal gateway.interrupted'
        assert (await client.post('/v1/siwc/cancel/' + rid, headers=headers)).status_code == 404
        return {'desktop_required': False, 'model': model, 'unauthorized_rejected': True,
                'model_catalog': True, 'streamed_multiturn': True, 'locally_saved_turns': 4,
                'function_roundtrip': True, 'cancelled_without_completion': True}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--base', default='http://127.0.0.1:8768')
    parser.add_argument('--model', default='gpt-6.1-sol')
    args = parser.parse_args()
    token = os.environ.get('MCP_SIWC_CONTROL_TOKEN', '')
    if len(token) < 24:
        parser.error('Set MCP_SIWC_CONTROL_TOKEN before running the smoke test')
    print(json.dumps(asyncio.run(run(args.base, token, args.model)), indent=2))


if __name__ == '__main__':
    main()
