import os, asyncio, aiohttp, aiomqtt, json, threading, time
from modules import shared
from modules.shared_cmd_options import cmd_opts


w_ts_start = int(time.time() * 1000)


def serialize_data(data, callback_id):
    obj = {
        'data': data,
        'callback_id': callback_id,
        'request_id': None
    }
    return json.dumps(obj)


def assert_shit():
    assert all(
        (cmd_opts.w_host, cmd_opts.w_port, cmd_opts.w_auser,
         cmd_opts.w_apass, cmd_opts.w_id, cmd_opts.w_topic,
         cmd_opts.w_master, cmd_opts.w_broker)
    ), "nyau?"


async def ping():
    return {
        'id': cmd_opts.w_id,
        'ts': w_ts_start,
        'status': 'online' if shared.model_loaded else 'booting',
        'busy_queue': getattr(shared.state, 'job_count', 0)
    }, cmd_opts.w_broker


async def t2i_infer(path, method, params):
    url = f'http://127.0.0.1:{cmd_opts.port}/sdapi/v1/{path}'
    kwargs = {'params' if method == 'GET' else 'json': params}
    
    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, **kwargs) as response:
            if response.status == 200:
                return await response.json(), None
            else:
                return await response.text(), None
            

async def handle_payload(client, payload):
    obj_payload = json.loads(payload)
    task = obj_payload.get('data', {}).get('task')
    params = obj_payload.get('data', {}).get('params')
    identifier = obj_payload.get('data', {}).get('id')
    request_id = obj_payload.get('request_id')

    if task is None:
        return

    if task == 'ping':
        res, topic = await ping()
    elif task == 'request' and identifier == cmd_opts.w_id:
        path = obj_payload.get('data', {}).get('path')
        method = obj_payload.get('data', {}).get('method')
        res, topic = await t2i_infer(path, method, params) or (None, None)
    else:
        print(f'w: unknown task {task}')
        return

    res = serialize_data(res, request_id)
    await client.publish(topic or cmd_opts.w_master, res)
    print('w: w -> m!')


def start_init():
    return serialize_data({
        'ts': w_ts_start,
        'id': cmd_opts.w_id
    }, None)


async def start_mqtt():
    assert_shit()
    print('w: nyaa!!')

    mqtt = aiomqtt.Client(
        hostname=cmd_opts.w_host,
        port=cmd_opts.w_port,
        username=cmd_opts.w_auser,
        password=cmd_opts.w_apass
    )

    async with mqtt as client:
        await client.subscribe(cmd_opts.w_topic)
        await client.publish(cmd_opts.w_broker, start_init())

        async for msg in client.messages:
            await handle_payload(client, msg.payload)


def start_async():
    asyncio.run(start_mqtt())


def shutdown_after_cooldown():
    print('Cooldown: shutting down now...')
    os._exit(0)


def start_thread():
    print('w - nyauuuuuuuuuuuuuuuuuuuuuu!')
    threading.Thread(target=start_async, daemon=True).start()
    threading.Timer(5400, shutdown_after_cooldown).start()
