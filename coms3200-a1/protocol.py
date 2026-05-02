"""! @file protocol.py
@author Darren
@ai Inspiration
@ai Wrote Code
@aitool ChatGPT
@aidetails ChatGPT was used to help design and implement newline-delimited
JSON protocol helper functions for socket communication.
"""

import json
import socket


ENCODING = "utf-8"


def send_json(sock: socket.socket, message: dict) -> None:
    """Send one JSON message followed by a newline."""
    data = json.dumps(message, separators=(",", ":")).encode(ENCODING) + b"\n"
    sock.sendall(data)


def recv_json(sock_file) -> dict | None:
    """
    Receive one JSON message from a socket file object.

    Returns None if the connection is closed.
    Raises ValueError if invalid JSON is received.
    """
    line = sock_file.readline()

    if line == b"":
        return None

    try:
        return json.loads(line.decode(ENCODING))
    except json.JSONDecodeError as exc:
        raise ValueError("invalid json message") from exc


def make_socket_file(sock: socket.socket):
    """
    Create a binary read file from a socket.

    This makes it easier to read newline-delimited protocol messages.
    """
    return sock.makefile("rb")