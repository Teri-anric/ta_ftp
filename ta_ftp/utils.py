from socket import socket


class Decoder:
    def __init__(self, conn: socket):
        self.conn = conn

    def read_bytes(self, size: int) -> bytes:
        return self.conn.recv(size)

    def read_string(self, size: int = 4, encoding='utf-8') -> str:
        str_len = self.read_int(size)
        return self.read_bytes(str_len).decode(encoding=encoding)

    def read_int(self, size: int = 8) -> int:
        return int.from_bytes(self.read_bytes(size), byteorder='big')



class Encoder:
    def __init__(self, conn: socket):
        self.conn = conn

    def write_bytes(self, data: bytes) -> int:
        return self.conn.send(data)

    def write_string(self, string: str, size: int = 4, encoding='utf-8') -> int:
        encode_string = string.encode(encoding=encoding)
        return self.write_int(len(encode_string), size=size) + self.write_bytes(encode_string)

    def write_int(self, num: int, size: int = 8) -> int:
        return self.write_bytes(num.to_bytes(size, byteorder='big'))

