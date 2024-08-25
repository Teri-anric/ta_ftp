import logging
from pathlib import Path
from socket import socket, AF_INET, SOCK_STREAM

from ta_ftp.constants import DEFAULT_PORT, BASE_PATH, DEFAULT_CHUNK_SIZE
from .utils import Decoder, Encoder
from threading import Thread
from hashlib import sha256
import os

class Server(Thread):
    def __init__(self, conn: socket, folder: Path = BASE_PATH, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn = conn
        self.decoder = Decoder(conn)
        self.encoder = Encoder(conn)
        # file info
        self.folder = folder
        self.file_name = None
        self.file_size = None
        self.chunk_size = DEFAULT_CHUNK_SIZE


    @property
    def file_path(self):
        return self.folder / self.file_name

    def run(self):
        self.prepare_connection()
        self.stream_content()
        self.check_file()
        self.conn.close()

    def prepare_connection(self):
        # logging.info(f"Connected from {addr}")
        if self.decoder.read_bytes(5) != b"TAFTP":
            logging.error("Connection can't be established, protocol not supported")
            self.conn.close()
            return False
        # read info
        self.file_size = self.decoder.read_int()
        self.chunk_size = self.decoder.read_int()
        self.file_name = self.decoder.read_string()
        logging.info(f"Start save file {self.file_name}")
        # get local file size
        local_file_size = 0
        if self.file_path.is_file():
            local_file_size = self.file_path.stat().st_size
        else:
            self.file_path.open('x').close()
        # send data
        self.encoder.write_int(local_file_size)

    def stream_content(self, dunamic: bool = False):
        fp = open(self.file_path, "rb+")
        while True:
            # read data
            offset = self.decoder.read_int()
            length = self.decoder.read_int()
            hashsum = self.decoder.read_bytes(32)
            data = self.decoder.read_bytes(length)
            # test hashsum
            if hashsum != sha256(data).digest():
                logging.warning(f"Hashsum send chunk file {self.file_name} is not valid, ansewer 0.")
                self.encoder.write_int(0)
                continue
            # test end file
            if (dunamic or offset == self.file_size) and length == 0:
                logging.info(f"File {self.file_name} save is successful")
                break
            # tests seek
            if not dunamic and fp.seek(0, os.SEEK_CUR) != offset:
                logging.warning(f"File {self.file_name} is oweride, client send new data.")
            fp.seek(offset, os.SEEK_SET)
            # send size data wrote
            self.encoder.write_int(fp.write(data))
        fp.close()

    def check_file(self):
        with open(self.file_path, "rb") as fp:
            offset = 0
            while data := fp.read(self.chunk_size):
                hashsum = sha256(data)
                self.encoder.write_int(offset)
                self.encoder.write_bytes(hashsum.digest())
                offset += len(data)
        self.stream_content(dunamic=True)