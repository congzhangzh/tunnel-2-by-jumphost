import traceback
import logging
import fire
import asyncio
from functools import partial
from typing import Union

class AsyncioTaskFilter(logging.Filter):
    def filter(self, record):
        task = asyncio.current_task()
        record.task_info = f'Task id={id(task)}' if task else 'No current task'
        return True

# 创建日志记录器，设置日志级别为DEBUG
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)
logger.addFilter(AsyncioTaskFilter())

# 创建一个日志处理器，并设置格式化字符串
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(task_info)s] (%(filename)s:%(lineno)d)')
handler.setFormatter(formatter)

# 将处理器添加到日志记录器
logger.addHandler(handler)

"""
Design Notes:
Q:How to control when a reverse tunnel should be created?
A:We use READY_MARK but not another control link to do this
"""

READY_MARK=b'ready'
async def forward_data(reader1, writer1, reader2, writer2):
    await asyncio.gather(
        proxy_data(reader1, writer2),
        proxy_data(reader2, writer1)
    )

async def proxy_data(reader, writer):
    while True:
        # logger.debug(f"--begin-- read, {reader.transport.get_extra_info('peername')}")
        logger.debug(f"--begin-- read")
        data = await reader.read(4096)
        logger.debug(f'--end-- read {data}')
        if not data:
            break
        logger.debug(f"--begin-- write, {writer.transport.get_extra_info('peername')}")
        writer.write(data)
        logger.debug(f'--begin-- write')

        await writer.drain()

async def handle_client_connection(tunnel_connection_queue, reader, writer):
    transport = writer.transport
    client_ip, client_port = transport.get_extra_info('peername')
    server_ip, server_port = transport.get_extra_info('sockname')
    logger.debug(f"[server][tunn conn] receive connected to tunnel port {server_ip}:{server_port}, from {client_ip}:{client_port}")
    
    await tunnel_connection_queue.put((reader, writer))

async def handle_external_connection(tunnel_connection_queue, reader, writer):
    transport = writer.transport
    client_ip, client_port = transport.get_extra_info('peername')
    server_ip, server_port = transport.get_extra_info('sockname')
    logger.debug(f"[server][external conn]receive connected to external port {server_ip}:{server_port}, from {client_ip}:{client_port}")

    logger.debug(f'[server][relay] --begin-- try to get one tunnel from queue')
    tunnel_reader, tunnel_writer = await tunnel_connection_queue.get()
    logger.debug(f'[server][relay] --end-- try to get one tunnel from queue')
    #!!!Trick, tell client this tunnel will be used, prepare another tunnel
    logger.debug(f'[server][relay] --begin-- try to send {READY_MARK}')
    await tunnel_writer.write(READY_MARK)
    await writer.drain()
    logger.debug(f'[server][relay] --end-- try to send {READY_MARK}')
    
    logger.debug(f'two way community between external connection and tunnel connection')
    # with  tunnel_writer, writer:
    await forward_data(reader, tunnel_writer, tunnel_reader, writer)
    
    # #TODO deadlock possible?
    writer.close()
    await writer.wait_closed()
    
    tunnel_writer.close()
    await tunnel_writer.wait_closed() 

def s(public_2_external_port, tunnel_port):
    loop = asyncio.get_event_loop()
    loop.create_task(_s( public_2_external_port, tunnel_port))
    loop.run_forever()

async def _s(public_2_external_port, tunnel_port):
    # 创建一个 asyncio.Queue 来管理连接
    tunnel_connection_queue = asyncio.Queue() # maxsize=1?

    time_to_wait=0
    server_4_external = await asyncio.start_server(
        partial(handle_external_connection, tunnel_connection_queue),
        '0.0.0.0', public_2_external_port, start_serving=True
    )

    server_4_tunnel = await asyncio.start_server(
        partial(handle_client_connection, tunnel_connection_queue),
        '0.0.0.0', tunnel_port, start_serving=True
    )

def c(jump_host, jump_host_tunnel_port, relay_to_host, relay_to_port):
    loop = asyncio.get_event_loop()
    loop.create_task(_c(jump_host, jump_host_tunnel_port, relay_to_host, relay_to_port))
    loop.run_forever()

async def _c(jump_host, jump_host_tunnel_port, relay_to_host, relay_to_port):
    time_to_wait=0
    while True:
        try:
            reader, writer = await asyncio.open_connection(jump_host, jump_host_tunnel_port)
            ready_mark=await reader.readexactly(len(READY_MARK))
            if ready_mark==READY_MARK:
                logger.debug("[client][relay mark]reday mark receive! forward data now!")
            else:
                raise Exception("[client][relay mark]unexpected data received!")
            tunnel_reader, tunnel_writer = await asyncio.open_connection(relay_to_host, relay_to_port)
            
            #with tunnel_writer, writer:
            logger.debug(f'two way community between tunnel connection and redirect connection')
            await forward_data(reader, tunnel_writer, tunnel_reader, writer)
            
            # #TODO deadlock possible?
            writer.close()
            await writer.wait_closed() 
            tunnel_writer.close()
            await tunnel_writer.wait_closed() 

            time_to_wait=0
        except Exception as e:
            logger.debug(f"[client] Error connecting to the server: {traceback.format_exc()}")
            time_to_wait=time_to_wait+1
            await asyncio.sleep(time_to_wait)
            logger.debug(f"[client] wakeup from sleep {time_to_wait}")

if __name__ == "__main__":
    fire.Fire({
        's':s,
        'c':c
    })
