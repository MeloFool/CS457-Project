"""Newline-delimited JSON wire protocol shared by client.py and server.py.

One JSON object per line (see docs/concurrency_design.md section 5). Framing
buffers bytes across recv() calls so a message split across TCP segments, or
several messages arriving in one recv(), are both handled correctly.
"""

import json
import socket

RECV_CHUNK = 4096

# Client -> server
CHOOSE = "CHOOSE"
QUIT = "QUIT"

# Server -> client
WELCOME = "WELCOME"
KICK_START = "KICK_START"
LOCKED_IN = "LOCKED_IN"
KICK_RESULT = "KICK_RESULT"
SCORE_UPDATE = "SCORE_UPDATE"
GAME_OVER = "GAME_OVER"
ERROR = "ERROR"

DIRECTIONS = ("LEFT", "CENTER", "RIGHT")

# ERROR codes (docs/concurrency_design.md section 4)
NOT_ACCEPTING_CHOICES = "NOT_ACCEPTING_CHOICES"
STALE_KICK = "STALE_KICK"
ALREADY_SUBMITTED = "ALREADY_SUBMITTED"
INVALID_DIRECTION = "INVALID_DIRECTION"
MALFORMED_MESSAGE = "MALFORMED_MESSAGE"
GAME_FULL = "GAME_FULL"


class ConnectionClosed(Exception):
    """Raised when the peer closes the connection (recv() returns b"")."""


class ProtocolError(Exception):
    """Raised when a line is not a well-formed protocol message."""


def encode(msg_type, **fields):
    """Build the wire bytes for one message: JSON object + newline."""
    payload = {"type": msg_type, **fields}
    return (json.dumps(payload) + "\n").encode("utf-8")


def send_message(sock, msg_type, **fields):
    sock.sendall(encode(msg_type, **fields))


class MessageReader:
    """Reassembles whole JSON messages from a socket's byte stream.

    Call read_messages() each time the socket is readable; it does exactly
    one recv() and returns the list of complete messages that produces
    (zero, one, or several), holding any trailing partial line in an
    internal buffer until the rest arrives.
    """

    def __init__(self, sock):
        self._sock = sock
        self._buffer = b""

    def read_messages(self):
        data = self._sock.recv(RECV_CHUNK)
        if not data:
            raise ConnectionClosed()
        self._buffer += data

        messages = []
        while b"\n" in self._buffer:
            line, self._buffer = self._buffer.split(b"\n", 1)
            if not line:
                continue
            messages.append(_decode_line(line))
        return messages


def _decode_line(line):
    try:
        message = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"malformed message: {exc}") from exc
    if not isinstance(message, dict) or "type" not in message:
        raise ProtocolError("malformed message: missing 'type' field")
    return message
