"""! @file pubsubserver.py
@author Darren
@ai Inspiration
@ai Wrote Code
@aitool ChatGPT
@aidetails ChatGPT was used to help design and implement the initial
command-line parsing, validation, and TCP listening structure for the pubsub server.
"""

import socket
import sys
import time

from common import is_valid_id, parse_endpoint


USAGE = "Usage: pubsubserver [--server [server]:port]... [--listenon port] serverid"


def usage_error() -> None:
    """Print the server usage error and exit."""
    print(USAGE, file=sys.stderr, flush=True)
    sys.exit(1)


def parse_args(argv: list[str]) -> dict:
    """Parse pubsubserver command-line arguments."""
    args = argv[1:]
    peer_args = []
    listen_port = None
    seen_listenon = False

    index = 0
    while index < len(args):
        arg = args[index]

        if arg == "--server":
            if index + 1 >= len(args) or args[index + 1] == "":
                usage_error()

            peer_endpoint = args[index + 1]

            try:
                parse_endpoint(peer_endpoint)
            except ValueError:
                usage_error()

            peer_args.append(peer_endpoint)
            index += 2

        elif arg == "--listenon":
            if seen_listenon:
                usage_error()

            if index + 1 >= len(args) or args[index + 1] == "":
                usage_error()

            listen_port = args[index + 1]
            seen_listenon = True
            index += 2

        elif arg.startswith("-"):
            usage_error()

        else:
            break

    remaining = args[index:]

    if len(remaining) != 1:
        usage_error()

    server_id = remaining[0]

    if server_id == "":
        usage_error()

    return {
        "peer_args": peer_args,
        "listen_port": listen_port,
        "server_id": server_id,
    }


def validate_args(parsed: dict) -> None:
    """Validate parsed command-line values after usage checking."""
    server_id = parsed["server_id"]

    if not is_valid_id(server_id):
        print(f'pubsubserver: bad server ID "{server_id}"', file=sys.stderr, flush=True)
        sys.exit(2)


def create_listening_socket(listen_port: str | None) -> socket.socket:
    """
    Create and return a TCP listening socket.

    If listen_port is None, bind to port 0 so the OS chooses a free port.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        port_to_bind = 0 if listen_port is None else int(listen_port)
    except ValueError:
        server_socket.close()
        print(f'pubsubserver: can’t listen on port "{listen_port}"', file=sys.stderr, flush=True)
        sys.exit(3)

    try:
        server_socket.bind(("", port_to_bind))
        server_socket.listen()
    except OSError:
        server_socket.close()
        if listen_port is None:
            print('pubsubserver: can’t listen on port "0"', file=sys.stderr, flush=True)
        else:
            print(f'pubsubserver: can’t listen on port "{listen_port}"', file=sys.stderr, flush=True)
        sys.exit(3)

    return server_socket


def main() -> None:
    """Run the pubsub server."""
    parsed = parse_args(sys.argv)
    validate_args(parsed)

    server_socket = create_listening_socket(parsed["listen_port"])
    actual_port = server_socket.getsockname()[1]

    print(f"pubsubserver: listening on port {actual_port}", file=sys.stderr, flush=True)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server_socket.close()
        sys.exit(0)


if __name__ == "__main__":
    main()