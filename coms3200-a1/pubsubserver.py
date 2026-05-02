"""! @file pubsubserver.py
@author Darren
@ai Inspiration
@ai Wrote Code
@aitool ChatGPT
@aidetails ChatGPT was used to help design and implement the initial
command-line parsing and validation structure for the pubsub server.
"""

import sys

from common import is_valid_id, parse_endpoint


USAGE = "Usage: pubsubserver [--server [server]:port]... [--listenon port] serverid"


def usage_error() -> None:
    """Print the server usage error and exit."""
    print(USAGE, file=sys.stderr, flush=True)
    sys.exit(1)


def parse_args(argv: list[str]) -> dict:
    """
    Parse pubsubserver command-line arguments.

    Expected:
    pubsubserver [--server [server]:port]... [--listenon port] serverid
    """
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


def main() -> None:
    """Run the pubsub server."""
    parsed = parse_args(sys.argv)
    validate_args(parsed)

    # Temporary output for testing Phase 1D only.
    # We will remove this when implementing socket listening.
    print(parsed)


if __name__ == "__main__":
    main()