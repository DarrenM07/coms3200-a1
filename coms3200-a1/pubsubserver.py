"""! @file pubsubserver.py
@author Darren
@ai Inspiration
@ai Wrote Code
@ai Debugging
@ai Testing
@aitool ChatGPT
@aidetails ChatGPT was used to help design, implement, test and debug
command-line parsing, validation, TCP listening, client handling, subscriptions,
filtered publication, rate limiting, file sending, shutdown handling and server
stdin command behaviour for the pubsub server.
"""

import os
import socket
import sys
import time
import threading

from common import filter_matches_message, is_valid_id, is_valid_topic, parse_endpoint
from protocol import make_socket_file, recv_json, send_json


USAGE = "Usage: pubsubserver [--server [server]:port]... [--listenon port] serverid"

clients = {}
subscriptions = {}
rate_limits = {}
last_publish_times = {}
clients_lock = threading.Lock()
peers = {}
peers_lock = threading.Lock()
pending_client_list_requests = {}
pending_client_list_lock = threading.Lock()
own_server_id = None
own_listen_port = None
seen_origins = set()
seen_origins_lock = threading.Lock()
pending_peer_list_requests = {}
pending_peer_list_lock = threading.Lock()
client_request_routes = {}
client_request_routes_lock = threading.Lock()
peer_request_routes = {}
peer_request_routes_lock = threading.Lock()

def usage_error() -> None:
    print(USAGE, file=sys.stderr, flush=True)
    sys.exit(1)


def parse_args(argv: list[str]) -> dict:
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

    return {"peer_args": peer_args, "listen_port": listen_port, "server_id": server_id}


def validate_args(parsed: dict) -> None:
    server_id = parsed["server_id"]
    if not is_valid_id(server_id):
        print(f'pubsubserver: bad server ID "{server_id}"', file=sys.stderr, flush=True)
        sys.exit(2)


def resolve_listen_port(listen_port: str | None) -> int:
    if listen_port is None or listen_port == "0":
        return 0

    try:
        port = int(listen_port)
    except ValueError:
        try:
            return socket.getservbyname(listen_port, "tcp")
        except OSError as exc:
            raise ValueError from exc

    if port < 1024:
        raise PermissionError

    return port


def create_listening_socket(listen_port: str | None) -> socket.socket:
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        port_to_bind = resolve_listen_port(listen_port)
    except (ValueError, PermissionError):
        server_socket.close()
        print(f'pubsubserver: can\'t listen on port "{listen_port}"', file=sys.stderr, flush=True)
        sys.exit(3)

    try:
        server_socket.bind(("", port_to_bind))
        server_socket.listen()
    except OSError:
        server_socket.close()
        shown_port = "0" if listen_port is None else listen_port
        print(f'pubsubserver: can\'t listen on port "{shown_port}"', file=sys.stderr, flush=True)
        sys.exit(3)

    return server_socket


def matching_client_sockets(topic: str, message: str | None = None, file_mode: bool = False) -> list[socket.socket]:
    targets = []
    for subscriber_id, sock in clients.items():
        for subscription in subscriptions.get(subscriber_id, []):
            if subscription["topic"] != topic:
                continue

            filter_data = subscription["filter"]

            if file_mode:
                targets.append(sock)
                break

            if filter_data is None:
                targets.append(sock)
                break

            if message is not None and filter_matches_message(message, filter_data["operator"], filter_data["value"]):
                targets.append(sock)
                break

    return targets


def check_rate_limit(client_id: str, topic: str) -> bool:
    limit_key = (client_id, topic)
    now = time.time()
    with clients_lock:
        limit_seconds = rate_limits.get(limit_key, 0)
        last_time = last_publish_times.get(limit_key)
        if limit_seconds > 0 and last_time is not None and now - last_time < limit_seconds:
            return False
        if limit_seconds > 0:
            last_publish_times[limit_key] = now
    return True

def forward_to_peers(message: dict, except_peer_id: str | None = None) -> None:
    """Forward a message to all directly connected peers except one."""
    with peers_lock:
        peer_items = list(peers.items())

    for peer_id, peer_socket in peer_items:
        if peer_id == except_peer_id:
            continue

        try:
            send_json(peer_socket, message)
        except OSError:
            pass

def deliver_file_to_local_clients(
    topic: str,
    filename: str,
    file_data: str,
    file_size: int,
    from_server: str,
    from_client: str,
) -> None:
    """Deliver a file to local clients subscribed to the topic without filters."""
    with clients_lock:
        targets = matching_client_sockets(topic, None, file_mode=True)

    for target_socket in targets:
        try:
            send_json(
                target_socket,
                {
                    "type": "deliver_file",
                    "topic": topic,
                    "filename": filename,
                    "data": file_data,
                    "size": file_size,
                    "from_client": from_client,
                    "from_server": from_server,
                },
            )
        except OSError:
            pass

def deliver_publish_to_local_clients(
    topic: str,
    publish_message: str,
    from_server: str,
    from_client: str,
) -> None:
    """Deliver a published text message to local matching clients."""
    with clients_lock:
        targets = matching_client_sockets(topic, publish_message, file_mode=False)

    for target_socket in targets:
        try:
            send_json(
                target_socket,
                {
                    "type": "deliver_message",
                    "topic": topic,
                    "message": publish_message,
                    "from_client": from_client,
                    "from_server": from_server,
                },
            )
        except OSError:
            pass

def handle_connection(client_socket: socket.socket, server_id: str) -> None:
    client_id = None
    client_socket.settimeout(1.0)

    try:
        sock_file = make_socket_file(client_socket)
        message = recv_json(sock_file)

        if message is None:
            print(
                "pubsubserver: Connection with unknown client aborted",
                file=sys.stderr,
                flush=True,
            )
            client_socket.close()
            return

        if message.get("type") == "hello_server":
            peer_id = message.get("serverid")

            # prevent self connection
            if peer_id == server_id:
                send_json(client_socket, {"type": "hello_server_ack", "serverid": server_id})
                client_socket.close()
                return

            # duplicate check BEFORE storing
            with peers_lock:
                if peer_id in peers:
                    send_json(client_socket, {"type": "hello_server_ack", "serverid": server_id})
                    client_socket.close()
                    return

            # send ACK ONCE
            send_json(client_socket, {"type": "hello_server_ack", "serverid": server_id})
            client_socket.settimeout(None)

            # store AFTER validation
            with peers_lock:
                peers[peer_id] = client_socket

            print(f'pubsubserver: Connection received from peer "{peer_id}"', flush=True)

            handle_peer_connection(client_socket, peer_id)
            return

        if message.get("type") != "hello_client":
            print(
                "pubsubserver: Connection with unknown client aborted",
                file=sys.stderr,
                flush=True,
            )
            client_socket.close()
            return

        client_socket.settimeout(None)
        client_id = message.get("clientid")

        with clients_lock:
            if client_id in clients:
                print(
                    f'pubsubserver: Client ID "{client_id}" would be duplicated - aborting connection',
                    flush=True,
                )
                send_json(client_socket, {"type": "error", "code": "duplicate_client_id"})
                client_socket.close()
                return

            clients[client_id] = client_socket
            subscriptions.setdefault(client_id, [])

        send_json(client_socket, {"type": "hello_ack", "serverid": server_id})
        print(f'pubsubserver: Client "{client_id}" has connected', flush=True)

        while True:
            next_message = recv_json(sock_file)

            if next_message is None:
                break

            if next_message.get("type") == "subscribe":
                subscription = {
                    "topic": next_message.get("topic"),
                    "filter": next_message.get("filter"),
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
                        sub for sub in client_subs if sub["topic"] != topic
                    ]

                continue

            if next_message.get("type") == "publish":
                topic = next_message.get("topic")
                publish_message = next_message.get("message")

                if not check_rate_limit(client_id, topic):
                    try:
                        send_json(client_socket, {"type": "rate_limit_failed"})
                    except OSError:
                        pass
                    continue

                deliver_publish_to_local_clients(
                    topic,
                    publish_message,
                    server_id,
                    client_id,
                )

                forward_to_peers(
                    {
                        "type": "federated_publish",
                        "topic": topic,
                        "message": publish_message,
                        "from_client": client_id,
                        "from_server": server_id,
                        "origin_id": f"{server_id}:{client_id}:{time.time_ns()}",
                    }
                )

                continue

            if next_message.get("type") == "send_file":
                topic = next_message.get("topic")

                if not check_rate_limit(client_id, topic):
                    try:
                        send_json(client_socket, {"type": "rate_limit_failed"})
                    except OSError:
                        pass
                    continue

                deliver_file_to_local_clients(
                    topic,
                    next_message.get("filename"),
                    next_message.get("data"),
                    next_message.get("size"),
                    server_id,
                    client_id,
                )

                forward_to_peers(
                    {
                        "type": "federated_file",
                        "topic": topic,
                        "filename": next_message.get("filename"),
                        "data": next_message.get("data"),
                        "size": next_message.get("size"),
                        "from_client": client_id,
                        "from_server": server_id,
                        "origin_id": f"{server_id}:{client_id}:file:{time.time_ns()}",
                    }
                )

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

        try:
            client_socket.close()
        except OSError:
            pass

def shutdown_server() -> None:
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

    with peers_lock:
        peer_sockets = list(peers.values())

    for peer_socket in peer_sockets:
        try:
            send_json(
                peer_socket,
                {
                    "type": "peer_shutdown",
                    "serverid": get_own_server_id(),
                },
            )
        except OSError:
            pass
    os._exit(0)

def get_own_server_id() -> str:
    return own_server_id

def local_client_names(server_id: str) -> list[str]:
    """Return local client names as serverid:clientid."""
    with clients_lock:
        return [f"{server_id}:{client_id}" for client_id in clients.keys()]
    
def local_peer_names() -> list[str]:
    with peers_lock:
        return sorted(peers.keys())

def request_clients_from_peers(request_id: str) -> None:
    """Ask all direct peers for their local client names."""
    forward_to_peers(
        {
            "type": "list_clients_request",
            "request_id": request_id,
            "visited": [get_own_server_id()],
        }
    )

def server_stdin_loop(server_id: str) -> None:
    while True:
        line = sys.stdin.readline()
        if line == "":
            shutdown_server()

        line = line.strip()
        if line == "":
            continue

        args = line.split()
        command = args[0]

        if command == "/quit":
            if len(args) != 1:
                print("pubsubserver: unknown argument(s) - usage: /quit", file=sys.stderr, flush=True)
                continue
            shutdown_server()

        if command == "/listclients":
            if len(args) > 2 or (len(args) == 2 and args[1] != "--all"):
                print(
                    "pubsubserver: unknown argument(s) - usage: /listclients [--all]",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            client_names = local_client_names(server_id)

            if len(args) == 2 and args[1] == "--all":
                request_id = f"{server_id}:{time.time_ns()}"

                with pending_client_list_lock:
                    pending_client_list_requests[request_id] = []

                request_clients_from_peers(request_id)

                time.sleep(0.2)

                with pending_client_list_lock:
                    peer_names = pending_client_list_requests.pop(request_id, [])

                client_names.extend(peer_names)

            if not client_names:
                print("pubsubserver: No clients connected", flush=True)
            else:
                for name in sorted(set(client_names)):
                    print(name, flush=True)

            continue

        if command == "/listpeers":
            if len(args) > 2 or (len(args) == 2 and args[1] != "--all"):
                print(
                    "pubsubserver: unknown argument(s) - usage: /listpeers [--all]",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            peer_ids = local_peer_names()

            if len(args) == 2 and args[1] == "--all":
                request_id = f"{server_id}:peers:{time.time_ns()}"

                with pending_peer_list_lock:
                    pending_peer_list_requests[request_id] = []

                forward_to_peers(
                    {
                        "type": "list_peers_request",
                        "request_id": request_id,
                        "visited": [server_id],
                    }
                )

                time.sleep(0.25)

                with pending_peer_list_lock:
                    extra_peers = pending_peer_list_requests.pop(request_id, [])

                peer_ids.extend(extra_peers)

            peer_ids = sorted(set(peer_ids))
            peer_ids = [peer_id for peer_id in peer_ids if peer_id != server_id]

            if not peer_ids:
                print("pubsubserver: No peer servers connected", flush=True)
            else:
                for peer_id in peer_ids:
                    print(peer_id, flush=True)

            continue

        if command == "/peer":
            if len(args) != 2:
                print(
                    "pubsubserver: unknown argument(s) - usage: /peer [server]:port",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            endpoint = args[1]

            try:
                parse_endpoint(endpoint)
            except ValueError:
                print(
                    "pubsubserver: unknown argument(s) - usage: /peer [server]:port",
                    file=sys.stderr,
                    flush=True,
                )
                continue

            connect_to_peer(endpoint, server_id)
            continue

        if command == "/limit":
            if len(args) != 4:
                print("pubsubserver: unknown argument(s) - usage: /limit clientid topic N", file=sys.stderr, flush=True)
                continue

            client_id = args[1]
            topic = args[2]
            limit_raw = args[3]

            with clients_lock:
                client_socket = clients.get(client_id)

            if client_socket is None or not is_valid_id(client_id):
                print(f'pubsubserver: Client "{client_id}" is unknown', file=sys.stderr, flush=True)
                continue

            if not is_valid_topic(topic):
                print(f'pubsubserver: Topic "{topic}" is not valid', file=sys.stderr, flush=True)
                continue

            try:
                limit_seconds = int(limit_raw)
            except ValueError:
                print("pubsubserver: Rate limit must be 0 to 3600 seconds inclusive", file=sys.stderr, flush=True)
                continue

            if limit_seconds < 0 or limit_seconds > 3600:
                print("pubsubserver: Rate limit must be 0 to 3600 seconds inclusive", file=sys.stderr, flush=True)
                continue

            with clients_lock:
                rate_limits[(client_id, topic)] = limit_seconds
                last_publish_times.pop((client_id, topic), None)

            try:
                send_json(client_socket, {"type": "rate_limit_notice", "topic": topic, "limit": limit_seconds})
            except OSError:
                pass
            continue

        print("pubsubserver: unknown command", file=sys.stderr, flush=True)

def should_process_origin(origin_id: str | None) -> bool:
    """Return True if this federated message origin has not been processed before."""
    if origin_id is None:
        return True

    with seen_origins_lock:
        if origin_id in seen_origins:
            return False

        seen_origins.add(origin_id)

        if len(seen_origins) > 10000:
            seen_origins.clear()

        return True

def handle_peer_connection(peer_socket: socket.socket, peer_id: str) -> None:
    """Handle an established peer server connection."""
    try:
        sock_file = make_socket_file(peer_socket)

        while True:
            message = recv_json(sock_file)
            if message is None:
                break

            if message.get("type") == "peer_shutdown":
                print(
                    f'pubsubserver: Peer server "{message.get("serverid")}" shutting down',
                    flush=True,
                )
                break

            if message.get("type") == "federated_publish":
                topic = message.get("topic")
                publish_message = message.get("message")
                from_client = message.get("from_client")
                from_server = message.get("from_server")
                origin_id = message.get("origin_id")

                if not should_process_origin(origin_id):
                    continue

                deliver_publish_to_local_clients(
                    topic,
                    publish_message,
                    from_server,
                    from_client,
                )

                forward_to_peers(message, except_peer_id=peer_id)
                continue

            if message.get("type") == "federated_file":
                topic = message.get("topic")

                if not should_process_origin(message.get("origin_id")):
                    continue

                deliver_file_to_local_clients(
                    topic,
                    message.get("filename"),
                    message.get("data"),
                    message.get("size"),
                    message.get("from_server"),
                    message.get("from_client"),
                )

                forward_to_peers(message, except_peer_id=peer_id)
                continue

            if message.get("type") == "list_clients_response":
                request_id = message.get("request_id")
                clients_from_peer = message.get("clients", [])

                handled_locally = False

                with pending_client_list_lock:
                    if request_id in pending_client_list_requests:
                        pending_client_list_requests[request_id].extend(clients_from_peer)
                        handled_locally = True

                if not handled_locally:
                    with client_request_routes_lock:
                        route_peer_id = client_request_routes.get(request_id)

                    if route_peer_id is not None:
                        with peers_lock:
                            route_socket = peers.get(route_peer_id)

                        if route_socket is not None:
                            try:
                                send_json(
                                    route_socket,
                                    {
                                        "type": "list_clients_response",
                                        "request_id": request_id,
                                        "clients": clients_from_peer,
                                    },
                                )
                            except OSError:
                                pass

                continue

            if message.get("type") == "list_clients_request":
                request_id = message.get("request_id")
                visited = message.get("visited", [])

                with client_request_routes_lock:
                    client_request_routes[request_id] = peer_id

                if get_own_server_id() not in visited:
                    visited = visited + [get_own_server_id()]

                send_json(
                    peer_socket,
                    {
                        "type": "list_clients_response",
                        "request_id": request_id,
                        "clients": local_client_names(get_own_server_id()),
                    },
                )

                with peers_lock:
                    peer_items = list(peers.items())

                for next_peer_id, next_peer_socket in peer_items:
                    if next_peer_id == peer_id or next_peer_id in visited:
                        continue

                    try:
                        send_json(
                            next_peer_socket,
                            {
                                "type": "list_clients_request",
                                "request_id": request_id,
                                "visited": visited,
                            },
                        )
                    except OSError:
                        pass

                continue

            if message.get("type") == "list_peers_request":
                request_id = message.get("request_id")
                visited = message.get("visited", [])

                with peer_request_routes_lock:
                    peer_request_routes[request_id] = peer_id

                send_json(
                    peer_socket,
                    {
                        "type": "list_peers_response",
                        "request_id": request_id,
                        "peers": local_peer_names(),
                    },
                )

                if get_own_server_id() not in visited:
                    visited = visited + [get_own_server_id()]

                with peers_lock:
                    peer_items = list(peers.items())

                for next_peer_id, next_peer_socket in peer_items:
                    if next_peer_id == peer_id or next_peer_id in visited:
                        continue

                    try:
                        send_json(
                            next_peer_socket,
                            {
                                "type": "list_peers_request",
                                "request_id": request_id,
                                "visited": visited,
                            },
                        )
                    except OSError:
                        pass

                continue

            if message.get("type") == "list_peers_response":
                request_id = message.get("request_id")
                peers_from_peer = message.get("peers", [])

                handled_locally = False

                with pending_peer_list_lock:
                    if request_id in pending_peer_list_requests:
                        pending_peer_list_requests[request_id].extend(peers_from_peer)
                        handled_locally = True

                if not handled_locally:
                    with peer_request_routes_lock:
                        route_peer_id = peer_request_routes.get(request_id)

                    if route_peer_id is not None:
                        with peers_lock:
                            route_socket = peers.get(route_peer_id)

                        if route_socket is not None:
                            try:
                                send_json(
                                    route_socket,
                                    {
                                        "type": "list_peers_response",
                                        "request_id": request_id,
                                        "peers": peers_from_peer,
                                    },
                                )
                            except OSError:
                                pass

                continue

    except (OSError, ValueError):
        pass

    finally:
        with peers_lock:
            if peers.get(peer_id) is peer_socket:
                del peers[peer_id]

        try:
            peer_socket.close()
        except OSError:
            pass

def connect_to_peer(endpoint: str, server_id: str) -> None:
    """Connect to a peer server from --server or /peer."""
    try:
        host, port = parse_endpoint(endpoint)
        if int(port) == own_listen_port:
            print("pubsubserver: Can't connect to self as peer", file=sys.stderr, flush=True)
            return
        peer_socket = socket.create_connection((host, int(port)), timeout=1.0)
        peer_socket.settimeout(1.0)
    except (OSError, ValueError):
        print(f'pubsubserver: can\'t connect to peer "{endpoint}"', file=sys.stderr, flush=True)
        return

    try:
        sock_file = make_socket_file(peer_socket)

        send_json(
            peer_socket,
            {
                "type": "hello_server",
                "serverid": server_id,
            },
        )

        response = recv_json(sock_file)

        if response is None or response.get("type") != "hello_server_ack":
            print(f'pubsubserver: Peer server not found at "{endpoint}"', file=sys.stderr, flush=True)
            peer_socket.close()
            return

        peer_id = response.get("serverid")

        if peer_id == server_id:
            print(
                f'pubsubserver: Unable to connect to server "{endpoint}" due to common server IDs',
                file=sys.stderr,
                flush=True,
            )
            peer_socket.close()
            return

        with peers_lock:
            if peer_id in peers:
                print(f'pubsubserver: Already connected to peer server at "{endpoint}"', file=sys.stderr, flush=True)
                peer_socket.close()
                return

            peer_socket.settimeout(None)
            peers[peer_id] = peer_socket

        print(f'pubsubserver: Connected to peer "{peer_id}" at "{endpoint}"', flush=True)

        thread = threading.Thread(
            target=handle_peer_connection,
            args=(peer_socket, peer_id),
            daemon=True,
        )
        thread.start()

    except (OSError, ValueError):
        print(f'pubsubserver: Peer server not found at "{endpoint}"', file=sys.stderr, flush=True)
        try:
            peer_socket.close()
        except OSError:
            pass

def main() -> None:
    parsed = parse_args(sys.argv)
    validate_args(parsed)

    global own_server_id
    own_server_id = parsed["server_id"]

    server_socket = create_listening_socket(parsed["listen_port"])
    actual_port = server_socket.getsockname()[1]
    global own_listen_port
    own_listen_port = actual_port
    print(f"pubsubserver: listening on port {actual_port}", file=sys.stderr, flush=True)

    stdin_thread = threading.Thread(target=server_stdin_loop, args=(parsed["server_id"],), daemon=True)
    stdin_thread.start()

    for peer_endpoint in parsed["peer_args"]:
        connect_to_peer(peer_endpoint, parsed["server_id"])

    try:
        while True:
            client_socket, _ = server_socket.accept()
            thread = threading.Thread(target=handle_connection, args=(client_socket, parsed["server_id"]), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        server_socket.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
