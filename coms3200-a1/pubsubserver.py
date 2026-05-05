"""! @file pubsubserver.py
@author Darren
@ai Inspiration
@ai Wrote Code
@aitool ChatGPT
@aidetails ChatGPT was used to help design and implement the initial
command-line parsing, validation, and TCP listening structure for the pubsub server.
"""

import os
import socket
import sys
import time
import threading

from common import filter_matches_message, is_valid_id, parse_endpoint
from protocol import make_socket_file, recv_json, send_json


USAGE = "Usage: pubsubserver [--server [server]:port]... [--listenon port] serverid"
clients = {}
subscriptions = {}
rate_limits = {}
last_publish_times = {}
clients_lock = threading.Lock()

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

def handle_connection(client_socket: socket.socket, server_id: str) -> None:
    """Handle one incoming client connection."""
    client_id = None

    try:
        sock_file = make_socket_file(client_socket)
        message = recv_json(sock_file)

        if message is None or message.get("type") != "hello_client":
            print(
                "pubsubserver: Connection with unknown client aborted",
                file=sys.stderr,
                flush=True,
            )
            client_socket.close()
            return

        client_id = message.get("clientid")

        with clients_lock:
            if client_id in clients:
                print(
                    f'pubsubserver: Client ID "{client_id}" would be duplicated - aborting connection',
                    flush=True,
                )
                send_json(
                    client_socket,
                    {
                        "type": "error",
                        "code": "duplicate_client_id",
                    },
                )
                client_socket.close()
                return

            clients[client_id] = client_socket
            subscriptions.setdefault(client_id, [])

        send_json(
            client_socket,
            {
                "type": "hello_ack",
                "serverid": server_id,
            },
        )

        print(f'pubsubserver: Client "{client_id}" has connected', flush=True)

        while True:
            next_message = recv_json(sock_file)

            if next_message is None:
                break

            if next_message.get("type") == "subscribe":
                topic = next_message.get("topic")
                filter_data = next_message.get("filter")

                subscription = {
                    "topic": topic,
                    "filter": filter_data,
                }

                with clients_lock:
                    client_subs = subscriptions.setdefault(client_id, [])
                    if subscription not in client_subs:
                        client_subs.append(subscription)

                continue

            if next_message.get("type") == "unsubscribe":
                topic = next_message.get("topic")

                with clients_lock:
                    client_subs = subscriptions.setdefault(client_id, [])
                    subscriptions[client_id] = [
                        existing_subscription
                        for existing_subscription in client_subs
                        if existing_subscription["topic"] != topic
                    ]

                continue

            if next_message.get("type") == "listclients":
                with clients_lock:
                    client_ids = list(clients.keys())

                send_json(
                    client_socket,
                    {
                        "type": "listclients_response",
                        "clients": client_ids,
                    },
                )
                continue

            if next_message.get("type") == "listpeers":
                send_json(
                    client_socket,
                    {
                        "type": "listpeers_response",
                        "peers": [],
                    },
                )
                continue

            if next_message.get("type") == "publish":
                topic = next_message.get("topic")
                publish_message = next_message.get("message")

                limit_key = (client_id, topic)
                now = time.time()

                with clients_lock:
                    limit_seconds = rate_limits.get(limit_key, 0)
                    last_time = last_publish_times.get(limit_key)

                    if (
                        limit_seconds > 0
                        and last_time is not None
                        and now - last_time < limit_seconds
                    ):
                        rate_limited = True
                    else:
                        rate_limited = False
                        if limit_seconds > 0:
                            last_publish_times[limit_key] = now

                if rate_limited:
                    try:
                        send_json(
                            client_socket,
                            {
                                "type": "rate_limit_failed",
                            },
                        )
                    except OSError:
                        pass
                    continue

                with clients_lock:
                    target_sockets = []

                    for subscriber_id, sock in clients.items():
                        for subscription in subscriptions.get(subscriber_id, []):
                            if subscription["topic"] != topic:
                                continue

                            filter_data = subscription["filter"]

                            if filter_data is None:
                                target_sockets.append(sock)
                                break

                            if filter_matches_message(
                                publish_message,
                                filter_data["operator"],
                                filter_data["value"],
                            ):
                                target_sockets.append(sock)
                                break

                for target_socket in target_sockets:
                    try:
                        send_json(
                            target_socket,
                            {
                                "type": "deliver_message",
                                "topic": topic,
                                "message": publish_message,
                                "from_client": client_id,
                                "from_server": server_id,
                            },
                        )
                    except OSError:
                        pass

                continue

    except (OSError, ValueError):
        pass

    finally:
        if client_id is not None:
            with clients_lock:
                if clients.get(client_id) is client_socket:
                    del clients[client_id]
                    subscriptions.pop(client_id, None)
                    print(f'pubsubserver: Client "{client_id}" has disconnected', flush=True)

        client_socket.close()

def shutdown_server() -> None:
    """Notify all connected clients and terminate the server."""
    with clients_lock:
        client_sockets = list(clients.values())

    for client_socket in client_sockets:
        try:
            send_json(client_socket, {"type": "server_shutdown"})
        except OSError:
            pass

        try:
            client_socket.close()
        except OSError:
            pass

    os._exit(0)

def server_stdin_loop(server_id: str) -> None:
    """Handle server commands from stdin."""
    while True:
        line = sys.stdin.readline()

        if line == "":
            shutdown_server()

        line = line.strip()

        if line == "":
            continue

        if line == "/quit":
            shutdown_server()

        if line == "/listclients":
            with clients_lock:
                client_names = [
                    f"{server_id}:{client_id}"
                    for client_id in clients.keys()
                ]

            if not client_names:
                print("pubsubserver: No clients connected", flush=True)
            else:
                for name in sorted(client_names):
                    print(name, flush=True)

            continue

        if line == "/listpeers":
            print("pubsubserver: No peer servers connected", flush=True)
            continue

        if line.startswith("/limit"):
            parts = line.split(maxsplit=3)

            if len(parts) != 4:
                print(
                    "pubsubserver: unknown argument(s) - usage: /limit clientid topic N",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            client_id = parts[1]
            topic = parts[2]
            limit_raw = parts[3]

            with clients_lock:
                client_socket = clients.get(client_id)

            if client_socket is None or not is_valid_id(client_id):
                print(
                    f'pubsubserver: Client "{client_id}" is unknown',
                    file=sys.stderr,
                    flush=True,
                )
                continue

            from common import is_valid_topic

            if not is_valid_topic(topic):
                print(
                    f'pubsubserver: Topic "{topic}" is not valid',
                    file=sys.stderr,
                    flush=True,
                )
                continue

            try:
                limit_seconds = int(limit_raw)
            except ValueError:
                print(
                    "pubsubserver: Rate limit must be 0 to 3600 seconds inclusive",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            if limit_seconds < 0 or limit_seconds > 3600:
                print(
                    "pubsubserver: Rate limit must be 0 to 3600 seconds inclusive",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            with clients_lock:
                rate_limits[(client_id, topic)] = limit_seconds
                last_publish_times.pop((client_id, topic), None)

            try:
                send_json(
                    client_socket,
                    {
                        "type": "rate_limit_notice",
                        "topic": topic,
                        "limit": limit_seconds,
                    },
                )
            except OSError:
                pass

            continue

        print("pubsubserver: unknown command", file=sys.stderr, flush=True)

def main() -> None:
    """Run the pubsub server."""
    parsed = parse_args(sys.argv)
    validate_args(parsed)

    server_socket = create_listening_socket(parsed["listen_port"])
    actual_port = server_socket.getsockname()[1]

    print(f"pubsubserver: listening on port {actual_port}", file=sys.stderr, flush=True)
    stdin_thread = threading.Thread(
        target=server_stdin_loop,
        args=(parsed["server_id"],),
        daemon=True,
    )
    stdin_thread.start()

    try:
        while True:
            client_socket, _ = server_socket.accept()
            thread = threading.Thread(
                target=handle_connection,
                args=(client_socket, parsed["server_id"]),
                daemon=True,
            )
            thread.start()
    except KeyboardInterrupt:
        server_socket.close()
        sys.exit(0)


if __name__ == "__main__":
    main()