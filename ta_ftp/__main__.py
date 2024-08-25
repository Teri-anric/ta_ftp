import timeit

import click
from .server import Server
from .client import Client
from pathlib import Path
from socket import socket, AF_INET, SOCK_STREAM
from .constants import DEFAULT_CHUNK_SIZE, BASE_PATH, DEFAULT_PORT
from threading import Thread
from logging import getLogger, INFO, StreamHandler
from datetime import timedelta

logger = getLogger(__name__)
logger.setLevel(INFO)
logger.addHandler(StreamHandler())

@click.group()
def cli():
    """CLI tool for Client and Server operations."""
    pass


@cli.command()
@click.option('--host', required=True, help='The server host.')
@click.option('--port', default=DEFAULT_PORT, type=int, help='The server port.')
@click.option('--file-path', required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path), help='Path to the file to send.')
@click.option('--chunk-size', default=DEFAULT_CHUNK_SIZE, type=int, help='Size of each data chunk to send.')
def client(host: str, port: int, file_path: Path, chunk_size: int):
    """Run the client to send a file to the server."""
    client_instance = Client(host, port, file_path, chunk_size)
    time = timedelta(seconds=timeit.timeit(client_instance.run, number=1))
    file_size = file_path.stat().st_size / 1024**3

    click.echo(f"Time: {file_size:.2f}Gb/{time} seconds")


@cli.command()
@click.option('--host', default='0.0.0.0', help='The host to bind the server to.')
@click.option('--port', default=DEFAULT_PORT, type=int, help='The port to bind the server to.')
@click.option('--folder', default=BASE_PATH, type=click.Path(exists=True, file_okay=False, path_type=Path), help='Folder to save received files.')
def server(host: str, port: int, folder: Path):
    """Run the server to receive files from clients."""
    server_threads = []
    with socket(AF_INET, SOCK_STREAM) as server_sock:
        server_sock.bind((host, port))
        server_sock.listen()
        print(f"Server listening on {host}:{port}")
        try:
            while True:
                conn, addr = server_sock.accept()
                print(f"Connection accepted from {addr}")
                server_thread = Server(conn, folder, daemon=True)
                server_thread.run()
                server_threads.append(server_thread)
        except KeyboardInterrupt:
            print("Stop server...")
            server_sock.close()
            print("Server shutting down.")


if __name__ == '__main__':
    cli()