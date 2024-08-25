import logging
from hashlib import sha256
from pathlib import Path
from socket import socket, AF_INET, SOCK_STREAM

from .constants import DEFAULT_CHUNK_SIZE
from .utils import Decoder, Encoder
from math import floor
from threading import Thread

class Client(Thread):
    def __init__(self, host: str, port: int, file_path: Path, chunk_size: int = DEFAULT_CHUNK_SIZE, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.encoder = Encoder(self.sock)
        self.decoder = Decoder(self.sock)
        self.file_path = file_path
        self.chunk_size = chunk_size

    @property
    def file_size(self) -> int:
        return self.file_path.stat().st_size

    @property
    def file_name(self):
        return self.file_path.name

    def connect(self):
        self.sock.connect((self.host, self.port))
        logging.info(f"Connect to {self.host}:{self.port}")

    def disconnect(self):
        logging.info(f"Disconnect from {self.host}:{self.port}")
        self.sock.close()

    def run(self):
        self.connect()
        offset = self.prepare_connection()
        self.stream_content(offset)
        self.check_file()
        self.disconnect()

    def check_file(self, size: int = None) -> None:
        size = size or self.file_size
        corrupted_chunk = []
        with open(self.file_path, "rb") as fp:
            offset = 0
            while offset < size:
                _offset = self.decoder.read_int()
                if _offset != offset:
                    logging.warning(f"")
                    offset = _offset
                    fp.seek(_offset)
                _hashsum = self.decoder.read_bytes(32)
                data = fp.read(self.chunk_size)
                hashsum = sha256(data)
                if hashsum.digest() != _hashsum:
                    logging.warning(f"File is corrupted: {self.file_name}:{offset}")
                    corrupted_chunk.append(offset)
                offset += len(data)
        self.fragment_send(corrupted_chunk)


    def prepare_connection(self):
        # send hello msg
        self.encoder.write_bytes(b"TAFTP")
        # send info
        self.encoder.write_int(self.file_size)
        self.encoder.write_int(self.chunk_size)
        self.encoder.write_string(self.file_name)
        logging.info(f"Start upload file {self.file_name}")
        # get server size
        return self.decoder.read_int()

    def stream_content(self, offset = 0):
        fp = open(self.file_path, 'rb')
        file_size = self.file_size
        while offset < file_size:
            # Read data
            logging.info(f"Uploading chunk {offset // self.chunk_size}...")
            data = fp.read(self.chunk_size)
            length = len(data)
            # calculate hashsum
            hashsum = sha256(data).digest()
            # send header and data
            self.encoder.write_int(offset)
            self.encoder.write_int(length)
            self.encoder.write_bytes(hashsum)
            self.encoder.write_bytes(data)
            # Wait response offset
            written = self.decoder.read_int()
            if written != length:
                logging.warning(f"Failed to write all data to server. Recorded: {written}, resend chunk.")
                continue
            # change offset
            offset += length
        # End file send
        self.encoder.write_int(file_size)
        self.encoder.write_int(0)
        self.encoder.write_bytes(sha256(b'').digest())  # send empty hash
        # close descriptors
        fp.close()
        logging.info(f"Upload file {self.file_name} finished.")

    def fragment_send(self, offsets: list[int]):
        fp = open(self.file_path, 'rb')
        while offsets:
            offset = offsets.pop()
            # Read data
            fp.seek(offset)
            data = fp.read(self.chunk_size)
            # calculate hashsum
            hashsum = sha256(data).digest()
            # send header and data
            self.encoder.write_int(offset)
            self.encoder.write_int(len(data))
            self.encoder.write_bytes(hashsum)
            self.encoder.write_bytes(data)
            # Wait response offset
            written = self.decoder.read_int()
            if written != length:
                logging.warning(f"Failed to write all data to server. Recorded: {written}, resend chunk.")
                offsets.append(offset)
                continue
        # End file send
        self.encoder.write_int(0)
        self.encoder.write_int(0)
        self.encoder.write_bytes(sha256(b'').digest())  # send empty hash
        # close descriptors
        fp.close()