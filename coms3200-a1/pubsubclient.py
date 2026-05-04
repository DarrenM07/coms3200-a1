"""! @file pubsubclient.py
@author Darren
@ai Inspiration
@ai Wrote Code
@aitool ChatGPT
@aidetails ChatGPT was used to help design and implement the initial
command-line parsing and validation structure for the pubsub client.
"""

import sys
import socket
from protocol import make_socket_file, recv_json, send_json

from common import (
    is_printable_message,
    is_valid_id,
    is_valid_topic,
    parse_endpoint,
    normalised_endpoint_for_error,
)


USAGE = "Usage: pubsubclient [--topic topic] [server]:port clientid [message]"


def usage_error() -> None:
    """Print the client usage error and exit."""
    print(USAGE, file=sys.stderr, flush=True)
    sys.exit(1)


def parse_args(argv: list[str]) -> dict:
    """
    Parse pubsubclient command-line arguments.

    Expected:
    pubsubclient [--topic topic] [server]:port clientid [message]
    """
    args = argv[1:]
    default_topic = None

    if len(args) == 0:
        usage_error()

    if args[0] == "--topic":
        if len(args) < 2 or args[1] == "":
            usage_error()
        default_topic = args[1]
        args = args[2:]

    elif args[0].startswith("-"):
        usage_error()

    if len(args) not in (2, 3):
        usage_error()

    endpoint = args[0]
    client_id = args[1]
    message = args[2] if len(args) == 3 else None

    if endpoint == "" or client_id == "":
        usage_error()

    try:
        host, port = parse_endpoint(endpoint)
    except ValueError:
        usage_error()

    if message is not None and default_topic is None:
        usage_error()

    return {
        "default_topic": default_topic,
        "endpoint_arg": endpoint,
        "host": host,
        "port": port,
        "client_id": client_id,
        "message": message,
    }


def validate_args(parsed: dict) -> None:
    """Validate parsed command-line values after usage checking."""
    client_id = parsed["client_id"]
    default_topic = parsed["default_topic"]
    message = parsed["message"]

    if not is_valid_id(client_id):
        print(f'pubsubclient: bad client ID "{client_id}"', file=sys.stderr, flush=True)
        sys.exit(4)

    if default_topic is not None and not is_valid_topic(default_topic):
        print(
            f'pubsubclient: invalid topic string "{default_topic}"',
            file=sys.stderr,
            flush=True,
        )
        sys.exit(5)

    if message is not None and not is_printable_message(message):
        print(
            "pubsubclient: messages must only contain printable characters",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(6)


def connect_to_server(parsed: dict) -> socket.socket:
    """Connect to the pubsub server or exit with status 7."""
    endpoint_for_error = normalised_endpoint_for_error(parsed["endpoint_arg"])

    try:
        client_socket = socket.create_connection(
            (parsed["host"], int(parsed["port"])),
            timeout=1.0,
        )
    except (OSError, ValueError):
        print(
            f'pubsubclient: unable to connect to "{endpoint_for_error}"',
            file=sys.stderr,
            flush=True,
        )
        sys.exit(7)

    return client_socket

def main() -> None:
    """Run the pubsub client."""
    parsed = parse_args(sys.argv)
    validate_args(parsed)

    client_socket = connect_to_server(parsed)
    sock_file = make_socket_file(client_socket)

    send_json(
        client_socket,
        {
            "type": "hello_client",
            "clientid": parsed["client_id"],
        },
    )

    response = recv_json(sock_file)

    if response is not None and response.get("type") == "error":
        if response.get("code") == "duplicate_client_id":
            print(
                f'pubsubclient: client ID "{parsed["client_id"]}" is not unique',
                file=sys.stderr,
                flush=True,
            )
            client_socket.close()
            sys.exit(9)

    if response is None or response.get("type") != "hello_ack":
        endpoint_for_error = normalised_endpoint_for_error(parsed["endpoint_arg"])
        print(
            f'pubsubclient: server at "{endpoint_for_error}" is not a valid server',
            file=sys.stderr,
            flush=True,
        )
        client_socket.close()
        sys.exit(8)

    print("Welcome to pubsubclient!", flush=True)

    try:
        while True:
            line = sys.stdin.readline()

            # EOF detected
            if line == "":
                client_socket.close()
                sys.exit(0)

            line = line.strip()

            # ignore empty lines
            if line == "":
                continue

            if line.startswith("/"):
                if line == "/quit":
                    client_socket.close()
                    sys.exit(0)

                elif line.startswith("/topic"):
                    parts = line.split(maxsplit=1)

                    if len(parts) != 2 or parts[1] == "":
                        print(
                            "pubsubclient: unknown argument(s) - usage: /topic topic",
                            file=sys.stderr,
                            flush=True,
                        )
                        continue

                    topic = parts[1]

                    if not is_valid_topic(topic):
                        print(
                            f'pubsubclient: invalid topic string "{topic}"',
                            file=sys.stderr,
                            flush=True,
                        )
                        continue

                    parsed["default_topic"] = topic

                else:
                    print("pubsubclient: unknown command", file=sys.stderr, flush=True)

            else:
                if parsed["default_topic"] is None:
                    print("pubsubclient: no default topic set", file=sys.stderr, flush=True)
                else:
                    # Temporary until publish is implemented.
                    pass

    except KeyboardInterrupt:
        client_socket.close()
        sys.exit(0)

if __name__ == "__main__":
    main()