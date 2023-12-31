import fire
import asyncio
from functools import partial
from typing import Union

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
        data = await reader.read(4096)
        if not data:
            break
        writer.write(data)
        await writer.drain()

async def handle_client_connection(tunnel_connection_queue, reader, writer):
    await tunnel_connection_queue.put((reader, writer))

async def handle_external_connection(tunnel_connection_queue, reader, writer):
    tunnel_reader, tunnel_writer = await tunnel_connection_queue.get()

    #!!!Trick, tell client this tunnel will be used, prepare another tunnel
    await tunnel_writer.write(READY_MARK)
    await writer.drain()
    
    with  tunnel_writer, writer:
        await asyncio.gather(
            forward_data(reader, tunnel_writer),
            forward_data(tunnel_reader, writer)
        )
    
    # #TODO deadlock possible?
    # writer.close()
    # await writer.wait_closed()
    # 
    # tunnel_writer.close()
    # await tunnel_writer.wait_closed() 

class MyCommands(fire.Command):
    async def s(self, public_2_external_port, tunnel_port):
        # 创建一个 asyncio.Queue 来管理连接
        tunnel_connection_queue = asyncio.Queue(maxsize=1)

        time_to_wait=0
        server_4_external = await asyncio.start_server(
            partial(handle_external_connection, tunnel_connection_queue),
            '0.0.0.0', public_2_external_port, start_serving=True
        )

        server_4_tunnel = await asyncio.start_server(
            partial(handle_client_connection, tunnel_connection_queue),
            '0.0.0.0', tunnel_port, start_serving=True
        )

    async def c(self, jump_host, jump_host_tunnel_port, relay_to_host, relay_to_port):
        time_to_wait=0
        while True:
            try:
                reader, writer = await asyncio.open_connection(jump_host, jump_host_tunnel_port)
                ready_mark=await reader.readexactly(len(READY_MARK))
                if ready_mark==READY_MARK:
                    print("reday mark receive! forward data now!")
                else:
                    raise Exception("unexpected data received!")
                tunnel_reader, tunnel_writer = await asyncio.open_connection(relay_to_host, relay_to_port)
                
                with tunnel_writer, writer:
                    await asyncio.gather(forward_data(reader, tunnel_writer),
                            forward_data(tunnel_reader, writer))
                
                # #TODO deadlock possible?
                # writer.close()
                # await writer.wait_closed() 
                # tunnel_writer.close()
                # await tunnel_writer.wait_closed() 

                time_to_wait=0
            except Exception as e:
                print(f"Error connecting to the server: {e}")
                time_to_wait=time_to_wait+1
                await asyncio.sleep(time_to_wait)

async def main():
    fire.Fire(MyCommands())

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
