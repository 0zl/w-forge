import aiohttp, aiomqtt, logging, json, threading, time
from modules import shared
from modules.shared_cmd_options import cmd_opts


w_ts_start = int(time.time() * 1000)
w_logger = logging.getLogger('w-mqtt')

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
         cmd_opts.w_master)
    ), "nyau?"


async def ping():
    url = f'http://127.0.0.1:{cmd_opts.port}'

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                status_code = resp.status
                if status_code == 200:
                    status = 'online'
                else:
                    if shared.model_loaded:
                        status = 'offline'
                    else:
                        status = 'booting'
        except aiohttp.ClientConnectorError:
            if shared.model_loaded:
                status = 'offline'
            else:
                status = 'booting'
    
    return {
        'id': cmd_opts.w_id,
        'ts': w_ts_start,
        'status': status,
        'busy_queue': shared.state.job_count
    }


async def t2i_infer(params):
    pass


async def handle_payload(client, payload):
    obj_payload = json.loads(payload)
    task = obj_payload.get('data', {}).get('task')
    params = obj_payload.get('data', {}).get('params')
    request_id = obj_payload.get('request_id')

    if task is None:
        return

    if task == 'ping':
        res = await ping()
    elif task == 't2i':
        res = await t2i_infer(params)
    else:
        w_logger.error('w: unknown task %s', task)
        return

    res = serialize_data(res, request_id)
    await client.publish(cmd_opts.w_master, res)
    w_logger.info('w: w -> m!')


def start_init():
    return serialize_data({
        'ts': w_ts_start,
        'id': cmd_opts.w_id
    }, None)


async def start_mqtt():
    assert_shit()
    w_logger.info('w: nyaa!!')

    mqtt = aiomqtt.Client(
        hostname=cmd_opts.w_host,
        port=cmd_opts.w_port,
        username=cmd_opts.w_auser,
        password=cmd_opts.w_apass,
        protocol='tcp',
        logger=w_logger
    )

    async with mqtt as client:
        await client.subscribe(cmd_opts.w_topic)
        await client.publish(cmd_opts.w_master, start_init())

        async for msg in client.messages:
            await handle_payload(client, msg.payload)


def start_thread():
    threading.Thread(target=start_mqtt, daemon=True).start()
