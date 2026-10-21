"""Session replay, tool handoff and crash recovery are local, not ChatGPT Desktop."""
import asyncio
import json
import time
import httpx
import pytest
from siwc_gateway.auth import CredentialStore
from siwc_gateway.gateway import SIWCGateway
from siwc_gateway.sessions import SessionStore, SessionError
from siwc_gateway.standalone import create_app

TOKEN = 'a' * 32
AUTH = {'Authorization': 'Bearer ' + TOKEN}


def harness(tmp_path, monkeypatch, respond):
    monkeypatch.setenv('MCP_SIWC_HOME', str(tmp_path / 'siwc'))
    grant = CredentialStore()
    grant.save({'client_id': 'oaiapp_test', 'subject': 'subject-test',
                'access_token': 'dummy-token', 'refresh_token': 'refresh',
                'scopes': ['chatgpt.tokens.use.direct'], 'saved_at': time.time(), 'expires_in': 3600})
    calls = []
    def upstream(req):
        assert req.url.path == '/v1/responses'
        body = json.loads(req.content)
        assert body['store'] is False and body['stream'] is True
        calls.append(body)
        return respond(body, len(calls))
    gateway = SIWCGateway(TOKEN, grant, httpx.MockTransport(upstream))
    return create_app(TOKEN, gateway=gateway), calls


def stream(*events):
    return httpx.Response(200, text=''.join('event: ' + event['type'] + '\ndata: ' + json.dumps(event) + '\n\n' for event in events))


def done(item):
    return stream({'type': 'response.output_item.done', 'item': item},
                  {'type': 'response.completed', 'response': {'output': [item]}})


def message(text):
    return {'type': 'message', 'role': 'assistant', 'content': [{'type': 'output_text', 'text': text}]}


def test_local_multiturn_persistence_and_delete(tmp_path, monkeypatch):
    app, calls = harness(tmp_path, monkeypatch, lambda _body, n: done(message('A' if n == 1 else 'B')))
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://local') as c:
            assert (await c.get('/v1/siwc/sessions')).status_code == 401
            session = await c.post('/v1/siwc/sessions', headers=AUTH, json={'model':'gpt-6.1-sol'})
            assert session.status_code == 201
            sid = session.json()['id']; route = f'/v1/siwc/sessions/{sid}'
            assert (await c.post(route + '/turn', json={'input':[{'role':'user','content':'Hello'}]})).status_code == 401
            one = await c.post(route + '/turn', headers=AUTH, json={'input':[{'role':'user','content':'Hello'}]})
            assert 'event: response.completed' in one.text
            assert one.headers['x-local-session-id'] == sid
            saved = (await c.get(route, headers=AUTH)).json()
            assert saved['state'] == 'idle' and len(saved['history']) == 2
            two = await c.post(route + '/turn', headers=AUTH, json={'input':[{'role':'user','content':'Remember A'}]})
            assert 'event: response.completed' in two.text
            saved = (await c.get(route, headers=AUTH)).json()
            assert [v.get('role') for v in saved['history']] == ['user','assistant','user','assistant']
            assert len((await c.get('/v1/siwc/sessions', headers=AUTH)).json()['sessions']) == 1
            assert (await c.delete(route, headers=AUTH)).json() == {'deleted':True}
            assert (await c.get(route, headers=AUTH)).status_code == 409
    asyncio.run(run())
    assert len(calls) == 2 and len(calls[0]['input']) == 1
    assert calls[1]['input'][1]['content'][0]['text'] == 'A'
    assert len(calls[1]['input']) == 3


def test_local_tool_call_and_result_replay(tmp_path, monkeypatch):
    call = {'type':'function_call','name':'echo','call_id':'call_1','arguments':'{}'}
    app, calls = harness(tmp_path, monkeypatch, lambda _b,n: done(call if n == 1 else message('TOOL_OK')))
    function = {'type':'namespace','name':'local','description':'Local tool provider','tools':[{'type':'function','name':'echo',
                 'description':'Echo','parameters':{'type':'object','properties':{}},'strict':True}]}
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://local') as c:
            sid = (await c.post('/v1/siwc/sessions',headers=AUTH,json={'model':'gpt-6.1-sol'})).json()['id']
            route = f'/v1/siwc/sessions/{sid}'
            first = await c.post(route+'/turn',headers=AUTH,json={'input':[{'role':'user','content':'Echo'}],'tools':[function]})
            assert 'response.completed' in first.text
            assert (await c.get(route,headers=AUTH)).json()['state'] == 'waiting_tool'
            invalid = await c.post(route+'/turn',headers=AUTH,json={'input':[{'role':'user','content':'Skip tool'}]})
            assert invalid.status_code == 409
            second = await c.post(route+'/turn',headers=AUTH,json={'input':[{'type':'function_call_output','call_id':'call_1','output':'echoed'}],'tools':[function]})
            assert 'response.completed' in second.text
            saved = (await c.get(route,headers=AUTH)).json()
            assert saved['state'] == 'idle' and len(saved['history']) == 4
    asyncio.run(run())
    assert len(calls) == 2
    assert calls[1]['input'][1] == call and calls[1]['input'][2]['output'] == 'echoed'


def test_failure_never_commits_turn_and_retry_succeeds(tmp_path, monkeypatch):
    app, calls = harness(tmp_path, monkeypatch,
                         lambda _b,n: httpx.Response(503, json={'error':'temporary'}) if n == 1 else done(message('RECOVERED')))
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://local') as c:
            sid=(await c.post('/v1/siwc/sessions',headers=AUTH,json={'model':'gpt-6.1-sol'})).json()['id']
            route=f'/v1/siwc/sessions/{sid}'
            body={'input':[{'role':'user','content':'Retry'}]}
            first=await c.post(route+'/turn',headers=AUTH,json=body)
            assert 'gateway.failed' in first.text and 'response.completed' not in first.text
            saved=(await c.get(route,headers=AUTH)).json()
            assert saved['state']=='failed' and saved['history']==[]
            second=await c.post(route+'/turn',headers=AUTH,json=body)
            assert 'response.completed' in second.text
            assert len((await c.get(route,headers=AUTH)).json()['history'])==2
    asyncio.run(run())
    assert len(calls)==2


def test_busy_lock_revision_guard_and_restart(tmp_path):
    sessions=SessionStore(tmp_path/'siwc')
    sid=sessions.create('gpt-6.1-sol')['id']
    incoming=[{'role':'user','content':'First'}]
    claim=sessions.begin(sid,incoming,expected_revision=0)
    with pytest.raises(SessionError,match='already running'):
        sessions.begin(sid,incoming)
    with pytest.raises(SessionError,match='currently running'):
        sessions.delete(sid)
    sessions.recover_interrupted()
    assert sessions.get(sid)['state']=='interrupted'
    with pytest.raises(SessionError,match='Session changed'):
        sessions.begin(sid,incoming,expected_revision=0)
    claim2=sessions.begin(sid,incoming,expected_revision=1)
    sessions.finish(sid,claim2['revision'],incoming,[message('OK')],True)
    with pytest.raises(SessionError,match='Session changed'):
        sessions.finish(sid,claim['revision'],incoming,[message('BAD')],True)
    assert len(sessions.get(sid)['history']) == 2


def test_tool_item_survives_empty_completed_output(tmp_path, monkeypatch):
    """The live SIWC stream uses store=false and can omit completed.output."""
    call = {'type':'function_call','name':'echo','call_id':'call_empty','arguments':'{}'}
    def respond(_body, n):
        if n == 1:
            return stream({'type':'response.output_item.done','item':call},
                          {'type':'response.completed','response':{'output':[]}})
        return done(message('TOOL_FINISHED'))
    app, upstream = harness(tmp_path, monkeypatch, respond)
    function = {'type':'namespace','name':'local','description':'Echo tools','tools':[
        {'type':'function','name':'echo','description':'Echo',
         'strict':True,'parameters':{'type':'object','properties':{},'required':[],'additionalProperties':False}}]}
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://local') as c:
            sid=(await c.post('/v1/siwc/sessions',headers=AUTH,json={'model':'gpt-6.1-sol'})).json()['id']
            url=f'/v1/siwc/sessions/{sid}'
            first=await c.post(url+'/turn',headers=AUTH,json={'input':[{'role':'user','content':'Call echo'}],'tools':[function]})
            assert 'response.completed' in first.text and 'gateway.failed' not in first.text
            saved=(await c.get(url,headers=AUTH)).json()
            assert saved['state']=='waiting_tool' and saved['history'][1]==call
            second=await c.post(url+'/turn',headers=AUTH,json={'input':[{'type':'function_call_output','call_id':'call_empty','output':'OK'}],'tools':[function]})
            assert 'response.completed' in second.text
            assert (await c.get(url,headers=AUTH)).json()['state']=='idle'
    asyncio.run(run())
    assert len(upstream)==2


def test_hosted_web_search_event_is_not_replayed_as_unsupported_input(tmp_path, monkeypatch):
    hosted = {'type':'web_search_call','id':'ws_1','status':'completed'}
    def respond(_body, n):
        if n == 1:
            return stream({'type':'response.output_item.done','item':hosted},
                          {'type':'response.output_text.delta','delta':'FOUND'},
                          {'type':'response.completed','response':{'output':[]}})
        return done(message('FOLLOWUP'))
    app, calls = harness(tmp_path, monkeypatch, respond)
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://local') as c:
            sid=(await c.post('/v1/siwc/sessions',headers=AUTH,json={'model':'gpt-6.1-sol'})).json()['id']
            url=f'/v1/siwc/sessions/{sid}'
            one=await c.post(url+'/turn',headers=AUTH,json={'input':[{'role':'user','content':'Find'}]})
            assert 'response.completed' in one.text
            saved=(await c.get(url,headers=AUTH)).json()
            assert saved['history'][1]['role']=='assistant'
            two=await c.post(url+'/turn',headers=AUTH,json={'input':[{'role':'user','content':'Follow up'}]})
            assert 'response.completed' in two.text
    asyncio.run(run())
    assert len(calls)==2
    assert not any(item.get('type')=='web_search_call' for item in calls[1]['input'])
