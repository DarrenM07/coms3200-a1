"""! @file pubsubclient.py
@author Darren
@ai Inspiration
@ai Wrote Code
@ai Debugging
@ai Testing
@aitool ChatGPT
@aidetails ChatGPT was used to help design, implement, test and debug
command-line parsing, validation, TCP connection, handshake, interactive mode,
subscriptions, filtered subscriptions, publishing, rate limiting, file sending,
shutdown handling and exact-output behaviour for the pubsub client.
"""

import os
import socket
import sys
import threading
import shlex
import base64
import time

from common import (
    is_printable_message,
    is_valid_id,
    is_valid_topic,
    normalised_endpoint_for_error,
    parse_endpoint,
    parse_filter,
)
from protocol import make_socket_file, recv_json, send_json


USAGE = "Usage: pubsubclient [--topic topic] [server]:port clientid [message]"


def usage_error() -> None:
    print(USAGE, file=sys.stderr, flush=True)
    sys.exit(1)


def parse_args(argv: list[str]) -> dict:
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
    client_id = parsed["client_id"]
    default_topic = parsed["default_topic"]
    message = parsed["message"]

    if not is_valid_id(client_id):
        print(f'pubsubclient: bad client ID "{client_id}"', file=sys.stderr, flush=True)
        sys.exit(4)

    if default_topic is not None and not is_valid_topic(default_topic):
        print(f'pubsubclient: invalid topic string "{default_topic}"', file=sys.stderr, flush=True)
        sys.exit(5)

    if message is not None and not is_printable_message(message):
        print("pubsubclient: messages must only contain printable characters", file=sys.stderr, flush=True)
        sys.exit(6)


def parse_command_args(line: str) -> list[str] | None:
    try:
        return shlex.split(line, posix=True)
    except ValueError:
        return None


def has_empty_argument(args: list[str]) -> bool:
    return any(arg == "" for arg in args)


def quote_if_needed(value: str) -> str:
    if any(char.isspace() for char in value):
        return f'"{value}"'
    return value


def connect_to_server(parsed: dict) -> socket.socket:
    endpoint_for_error = normalised_endpoint_for_error(parsed["endpoint_arg"])

    try:
        client_socket = socket.create_connection((parsed["host"], int(parsed["port"])), timeout=1.0)
        client_socket.settimeout(None)
    except (OSError, ValueError):
        print(f'pubsubclient: unable to connect to "{endpoint_for_error}"', file=sys.stderr, flush=True)
        sys.exit(7)

    return client_socket


def perform_handshake(client_socket: socket.socket, parsed: dict):
    sock_file = make_socket_file(client_socket)
    send_json(client_socket, {"type": "hello_client", "clientid": parsed["client_id"]})
    response = recv_json(sock_file)

    if response is not None and response.get("type") == "error":
        if response.get("code") == "duplicate_client_id":
            print(f'pubsubclient: client ID "{parsed["client_id"]}" is not unique', file=sys.stderr, flush=True)
            client_socket.close()
            sys.exit(9)

    if response is None or response.get("type") != "hello_ack":
        endpoint_for_error = normalised_endpoint_for_error(parsed["endpoint_arg"])
        print(f'pubsubclient: server at "{endpoint_for_error}" is not a valid server', file=sys.stderr, flush=True)
        client_socket.close()
        sys.exit(8)

    return sock_file


def check_client_rate_limit(parsed: dict, topic: str) -> bool:
    limit_seconds = 0

    for item in parsed["rate_limits"]:
        if item["client_id"] == parsed["client_id"] and item["topic"] == topic:
            limit_seconds = item["limit"]
            break

    if limit_seconds <= 0:
        return True

    now = time.time()
    last_time = parsed["last_publish_times"].get(topic)

    if last_time is not None and now - last_time < limit_seconds:
        print("pubsubclient: message publication failed due to rate limit", file=sys.stderr, flush=True)
        return False

    parsed["last_publish_times"][topic] = now
    return True


def handle_topic_command(line: str, parsed: dict) -> None:
    args = parse_command_args(line)
    if args is None or len(args) != 2 or has_empty_argument(args[1:]):
        print("pubsubclient: unknown argument(s) - usage: /topic topic", file=sys.stderr, flush=True)
        return

    topic = args[1]
    if not is_valid_topic(topic):
        print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr, flush=True)
        return

    parsed["default_topic"] = topic


def handle_publish_command(line: str, parsed: dict, client_socket: socket.socket) -> None:
    args = parse_command_args(line)
    if args is None or len(args) != 3 or has_empty_argument(args[1:]):
        print("pubsubclient: unknown argument(s) - usage: /publish topic message", file=sys.stderr, flush=True)
        return

    topic = args[1]
    message = args[2]

    if not is_valid_topic(topic):
        print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr, flush=True)
        return

    if not is_printable_message(message):
        print("pubsubclient: messages must only contain printable characters", file=sys.stderr, flush=True)
        return

    if not check_client_rate_limit(parsed, topic):
        return

    send_json(client_socket, {"type": "publish", "topic": topic, "message": message})


def handle_default_message(line: str, parsed: dict, client_socket: socket.socket) -> None:
    if parsed["default_topic"] is None:
        print("pubsubclient: no default topic set", file=sys.stderr, flush=True)
        return

    if not is_printable_message(line):
        print("pubsubclient: messages must only contain printable characters", file=sys.stderr, flush=True)
        return

    if not check_client_rate_limit(parsed, parsed["default_topic"]):
        return

    send_json(client_socket, {"type": "publish", "topic": parsed["default_topic"], "message": line})


def handle_subscribe_command(line: str, parsed: dict, client_socket: socket.socket) -> None:
    args = parse_command_args(line)
    if args is None or len(args) not in (2, 3) or has_empty_argument(args[1:]):
        print("pubsubclient: unknown argument(s) - usage: /subscribe topic [filter]", file=sys.stderr, flush=True)
        return

    topic = args[1]
    filter_raw = args[2] if len(args) == 3 else None

    if not is_valid_topic(topic):
        print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr, flush=True)
        return

    filter_data = None
    if filter_raw is not None:
        try:
            operator, value = parse_filter(filter_raw)
        except ValueError:
            print(f'pubsubclient: invalid filter string "{filter_raw}"', file=sys.stderr, flush=True)
            return
        filter_data = {"operator": operator, "value": value}

    for existing in parsed["subscriptions"]:
        same_topic = existing["topic"] == topic
        same_filter_absence = existing["filter_raw"] is None and filter_raw is None
        same_filter_value = False
        if existing["filter_data"] is not None and filter_data is not None:
            same_filter_value = (
                existing["filter_data"]["operator"] == filter_data["operator"]
                and existing["filter_data"]["value"] == filter_data["value"]
            )
        if same_topic and (same_filter_absence or same_filter_value):
            print("pubsubclient: identical subscription ignored", file=sys.stderr, flush=True)
            return

    parsed["subscriptions"].append({"topic": topic, "filter_raw": filter_raw, "filter_data": filter_data})
    send_json(client_socket, {"type": "subscribe", "topic": topic, "filter": filter_data, "filter_raw": filter_raw})


def handle_unsubscribe_command(line: str, parsed: dict, client_socket: socket.socket) -> None:
    args = parse_command_args(line)
    if args is None or len(args) != 2 or has_empty_argument(args[1:]):
        print("pubsubclient: unknown argument(s) - usage: /unsubscribe topic", file=sys.stderr, flush=True)
        return

    topic = args[1]
    if not is_valid_topic(topic):
        print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr, flush=True)
        return

    existing = [sub for sub in parsed["subscriptions"] if sub["topic"] == topic]
    if not existing:
        print(f'pubsubclient: not subscribed to messages about "{topic}"', file=sys.stderr, flush=True)
        return

    parsed["subscriptions"] = [sub for sub in parsed["subscriptions"] if sub["topic"] != topic]
    send_json(client_socket, {"type": "unsubscribe", "topic": topic})
    print(f'pubsubclient: unsubscribed from messages about "{topic}"', flush=True)


def handle_listsubs_command(parsed: dict) -> None:
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


def handle_listlimits_command(parsed: dict) -> None:
    active_limits = [limit for limit in parsed["rate_limits"] if limit["limit"] != 0]
    if not active_limits:
        print("No limits", flush=True)
        return

    for limit in active_limits:
        topic = quote_if_needed(limit["topic"])
        print(f'/limit {limit["client_id"]} {topic} {limit["limit"]}', flush=True)


def handle_sendfile_command(line: str, parsed: dict, client_socket: socket.socket) -> None:
    args = parse_command_args(line)
    if args is None or len(args) not in (2, 3) or has_empty_argument(args[1:]):
        print("pubsubclient: unknown argument(s) - usage: /sendfile filename [topic]", file=sys.stderr, flush=True)
        return

    filename = args[1]

    try:
        with open(filename, "rb") as file:
            file_bytes = file.read()
    except OSError:
        print(f'pubsubclient: unable to open file "{filename}"', file=sys.stderr, flush=True)
        return

    if len(args) == 3:
        topic = args[2]
        if not is_valid_topic(topic):
            print(f'pubsubclient: invalid topic string "{topic}"', file=sys.stderr, flush=True)
            return
    else:
        topic = parsed["default_topic"]
        if topic is None:
            print("pubsubclient: no default topic set", file=sys.stderr, flush=True)
            return

    if not check_client_rate_limit(parsed, topic):
        return

    send_json(
        client_socket,
        {
            "type": "send_file",
            "topic": topic,
            "filename": os.path.basename(filename),
            "data": base64.b64encode(file_bytes).decode("ascii"),
            "size": len(file_bytes),
        },
    )


def server_reader_loop(sock_file, parsed: dict) -> None:
    try:
        while True:
            message = recv_json(sock_file)

            if message is None:
                print("pubsubclient: server disconnected - exiting", file=sys.stderr, flush=True)
                os._exit(10)

            if message.get("type") == "deliver_message":
                print(f'{message["topic"]}: {message["message"]} ({message["from_server"]}:{message["from_client"]})', flush=True)

            elif message.get("type") == "server_shutdown":
                print("pubsubclient: exiting due to server shutdown", flush=True)
                os._exit(0)

            elif message.get("type") == "rate_limit_notice":
                topic = message["topic"]
                limit = message["limit"]
                existing = None
                for item in parsed["rate_limits"]:
                    if item["client_id"] == parsed["client_id"] and item["topic"] == topic:
                        existing = item
                        break
                if existing is None:
                    parsed["rate_limits"].append({"client_id": parsed["client_id"], "topic": topic, "limit": limit})
                else:
                    existing["limit"] = limit
                parsed["last_publish_times"].pop(topic, None)
                print(f'pubsubclient: you are rate limited on topic "{topic}" to {limit} seconds between messages', flush=True)

            elif message.get("type") == "rate_limit_failed":
                print("pubsubclient: message publication failed due to rate limit", file=sys.stderr, flush=True)

            elif message.get("type") == "deliver_file":
                parsed["received_file_count"] += 1
                original_filename = message["filename"]
                save_filename = f'{parsed["received_file_count"]}_{os.path.basename(original_filename)}'
                file_bytes = base64.b64decode(message["data"].encode("ascii"))

                try:
                    with open(save_filename, "wb") as file:
                        file.write(file_bytes)
                except OSError:
                    print(f'pubsubclient: cannot save file "{save_filename}"', file=sys.stderr, flush=True)
                    continue

                print(f'{message["topic"]}: received file "{save_filename}" from {message["from_server"]}:{message["from_client"]} ({len(file_bytes)} bytes)', flush=True)

    except (OSError, ValueError):
        print("pubsubclient: server disconnected - exiting", file=sys.stderr, flush=True)
        os._exit(10)


def interactive_loop(client_socket: socket.socket, parsed: dict) -> None:
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
                args = parse_command_args(stripped)
                if args is None:
                    print("pubsubclient: unknown command", file=sys.stderr, flush=True)
                    continue

                command = args[0]

                if command == "/quit":
                    if len(args) != 1:
                        print("pubsubclient: unknown argument(s) - usage: /quit", file=sys.stderr, flush=True)
                        continue
                    client_socket.close()
                    sys.exit(0)

                if command == "/listsubs":
                    if len(args) != 1:
                        print("pubsubclient: unknown argument(s) - usage: /listsubs", file=sys.stderr, flush=True)
                        continue
                    handle_listsubs_command(parsed)
                    continue

                if command == "/listlimits":
                    if len(args) != 1:
                        print("pubsubclient: unknown argument(s) - usage: /listlimits", file=sys.stderr, flush=True)
                        continue
                    handle_listlimits_command(parsed)
                    continue

                if command == "/unsubscribe":
                    handle_unsubscribe_command(stripped, parsed, client_socket)
                    continue
                if command == "/subscribe":
                    handle_subscribe_command(stripped, parsed, client_socket)
                    continue
                if command == "/topic":
                    handle_topic_command(stripped, parsed)
                    continue
                if command == "/sendfile":
                    handle_sendfile_command(stripped, parsed, client_socket)
                    continue
                if command == "/publish":
                    handle_publish_command(stripped, parsed, client_socket)
                    continue

                print("pubsubclient: unknown command", file=sys.stderr, flush=True)
                continue

            handle_default_message(line, parsed, client_socket)

    except KeyboardInterrupt:
        client_socket.close()
        sys.exit(0)


def main() -> None:
    parsed = parse_args(sys.argv)
    validate_args(parsed)
    parsed["subscriptions"] = []
    parsed["rate_limits"] = []
    parsed["received_file_count"] = 0
    parsed["last_publish_times"] = {}

    client_socket = connect_to_server(parsed)
    sock_file = perform_handshake(client_socket, parsed)

    if parsed["message"] is not None:
        send_json(client_socket, {"type": "publish", "topic": parsed["default_topic"], "message": parsed["message"]})
        client_socket.close()
        sys.exit(0)

    reader_thread = threading.Thread(target=server_reader_loop, args=(sock_file, parsed), daemon=True)
    reader_thread.start()
    interactive_loop(client_socket, parsed)


if __name__ == "__main__":
    main()
