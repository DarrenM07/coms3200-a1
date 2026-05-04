"""! @file pubsubclient.py
@author Darren
@ai Inspiration
@ai Wrote Code
@aitool ChatGPT
@aidetails ChatGPT was used to help design and implement the initial
command-line parsing, validation, TCP connection, handshake, interactive mode,
default topic command, and publish message handling for the pubsub client.
"""

import socket
import sys
import threading

from common import (
    is_printable_message,
    is_valid_id,
    is_valid_topic,
    normalised_endpoint_for_error,
    parse_endpoint,
)
from protocol import make_socket_file, recv_json, send_json


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
        client_socket.settimeout(None)
    except (OSError, ValueError):
        print(
            f'pubsubclient: unable to connect to "{endpoint_for_error}"',
            file=sys.stderr,
            flush=True,
        )
        sys.exit(7)

    return client_socket


def perform_handshake(client_socket: socket.socket, parsed: dict):
    """Perform initial client-server handshake."""
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

    return sock_file


def handle_topic_command(line: str, parsed: dict) -> None:
    """Handle /topic command."""
    parts = line.split(maxsplit=1)

    if len(parts) != 2 or parts[1] == "":
        print(
            "pubsubclient: unknown argument(s) - usage: /topic topic",
            file=sys.stderr,
            flush=True,
        )
        return

    topic = parts[1]

    if not is_valid_topic(topic):
        print(
            f'pubsubclient: invalid topic string "{topic}"',
            file=sys.stderr,
            flush=True,
        )
        return

    parsed["default_topic"] = topic


def handle_publish_command(line: str, client_socket: socket.socket) -> None:
    """Handle /publish command."""
    parts = line.split(maxsplit=2)

    if len(parts) != 3 or parts[1] == "" or parts[2] == "":
        print(
            "pubsubclient: unknown argument(s) - usage: /publish topic message",
            file=sys.stderr,
            flush=True,
        )
        return

    topic = parts[1]
    message = parts[2]

    if not is_valid_topic(topic):
        print(
            f'pubsubclient: invalid topic string "{topic}"',
            file=sys.stderr,
            flush=True,
        )
        return

    if not is_printable_message(message):
        print(
            "pubsubclient: messages must only contain printable characters",
            file=sys.stderr,
            flush=True,
        )
        return

    send_json(
        client_socket,
        {
            "type": "publish",
            "topic": topic,
            "message": message,
        },
    )


def handle_default_message(line: str, parsed: dict, client_socket: socket.socket) -> None:
    """Publish a normal input line using the default topic."""
    if parsed["default_topic"] is None:
        print("pubsubclient: no default topic set", file=sys.stderr, flush=True)
        return

    if not is_printable_message(line):
        print(
            "pubsubclient: messages must only contain printable characters",
            file=sys.stderr,
            flush=True,
        )
        return

    send_json(
        client_socket,
        {
            "type": "publish",
            "topic": parsed["default_topic"],
            "message": line,
        },
    )


def interactive_loop(client_socket: socket.socket, parsed: dict) -> None:
    """Run the client interactive input loop."""
    print("Welcome to pubsubclient!", flush=True)

    try:
        while True:
            line = sys.stdin.readline()

            if line == "":
                client_socket.close()
                sys.exit(0)

            line = line.rstrip("\n")

            if line.strip() == "":
                continue

            stripped = line.strip()

            if stripped.startswith("/"):
                if stripped == "/quit":
                    client_socket.close()
                    sys.exit(0)

                if stripped == "/listsubs":
                    handle_listsubs_command(parsed)
                    continue

                if stripped.startswith("/subscribe"):
                    handle_subscribe_command(stripped, parsed, client_socket)
                    continue

                if stripped.startswith("/topic"):
                    handle_topic_command(stripped, parsed)
                    continue

                if stripped.startswith("/publish"):
                    handle_publish_command(stripped, client_socket)
                    continue

                print("pubsubclient: unknown command", file=sys.stderr, flush=True)
                continue

            handle_default_message(line, parsed, client_socket)

    except KeyboardInterrupt:
        client_socket.close()
        sys.exit(0)

def handle_subscribe_command(line: str, parsed: dict, client_socket: socket.socket) -> None:
    """Handle /subscribe command without filter for now."""
    parts = line.split(maxsplit=1)

    if len(parts) != 2 or parts[1] == "":
        print(
            "pubsubclient: unknown argument(s) - usage: /subscribe topic [filter]",
            file=sys.stderr,
            flush=True,
        )
        return

    topic = parts[1]

    if not is_valid_topic(topic):
        print(
            f'pubsubclient: invalid topic string "{topic}"',
            file=sys.stderr,
            flush=True,
        )
        return

    subscription = {"topic": topic, "filter_raw": None}

    if subscription in parsed["subscriptions"]:
        print("pubsubclient: identical subscription ignored", file=sys.stderr, flush=True)
        return

    parsed["subscriptions"].append(subscription)

    send_json(
        client_socket,
        {
            "type": "subscribe",
            "topic": topic,
            "filter": None,
        },
    )

def server_reader_loop(sock_file) -> None:
    """Read and print messages delivered from the server."""
    try:
        while True:
            message = recv_json(sock_file)

            if message is None:
                print(
                    "pubsubclient: server disconnected - exiting",
                    file=sys.stderr,
                    flush=True,
                )
                sys.exit(10)

            if message.get("type") == "deliver_message":
                print(
                    f'{message["topic"]}: {message["message"]} '
                    f'({message["from_server"]}:{message["from_client"]})',
                    flush=True,
                )

    except (OSError, ValueError):
        print(
            "pubsubclient: server disconnected - exiting",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(10)

def quote_if_needed(value: str) -> str:
    """Quote a value if it contains whitespace."""
    if any(char.isspace() for char in value):
        return f'"{value}"'
    return value


def handle_listsubs_command(parsed: dict) -> None:
    """Handle /listsubs command."""
    if not parsed["subscriptions"]:
        print("No subscriptions", flush=True)
        return

    for subscription in parsed["subscriptions"]:
        topic = quote_if_needed(subscription["topic"])

        if subscription["filter_raw"] is None:
            print(f"/subscribe {topic}", flush=True)
        else:
            filter_raw = quote_if_needed(subscription["filter_raw"])
            print(f"/subscribe {topic} {filter_raw}", flush=True)

def main() -> None:
    """Run the pubsub client."""
    parsed = parse_args(sys.argv)
    validate_args(parsed)
    parsed["subscriptions"] = []

    client_socket = connect_to_server(parsed)
    sock_file = perform_handshake(client_socket, parsed)

    if parsed["message"] is not None:
        send_json(
            client_socket,
            {
                "type": "publish",
                "topic": parsed["default_topic"],
                "message": parsed["message"],
            },
        )
        client_socket.close()
        sys.exit(0)

    reader_thread = threading.Thread(
        target=server_reader_loop,
        args=(sock_file,),
        daemon=True,
    )
    reader_thread.start()

    interactive_loop(client_socket, parsed)

if __name__ == "__main__":
    main()