"""Penalty Shootout game client.

A background thread owns the socket's recv() loop so the main thread is
free to block on input() for the player's choice without missing messages
from the server.

Usage:
    python3 client.py -i server.lowther.edu -p 8080
"""

import argparse
import socket
import sys
import threading

import protocol

DIRECTION_ALIASES = {
    "L": "LEFT", "LEFT": "LEFT",
    "C": "CENTER", "CENTER": "CENTER",
    "R": "RIGHT", "RIGHT": "RIGHT",
}

GOAL_DIAGRAM = (
    "        ______________________\n"
    "       |  LEFT | CENTER | RIGHT |\n"
    "       |_______|________|_______|\n"
)


class ClientState:
    """Fields written by the listener thread and read by the main thread,
    guarded by `lock` the same way server.GameState guards shared state."""

    def __init__(self):
        self.lock = threading.Lock()
        self.player_id = None
        self.kick_id = None
        self.round = None
        self.role = None
        self.scores = {"1": 0, "2": 0}
        self.phase = "WAITING"


def parse_args():
    parser = argparse.ArgumentParser(
        prog="client.py",
        description="Penalty Shootout game client.",
        epilog="Example: python3 client.py -i server.lowther.edu -p 8080",
    )
    parser.add_argument(
        "-i", "--hostname",
        required=True,
        metavar="HOSTNAME",
        help="server DNS hostname or IP to connect to",
    )
    parser.add_argument(
        "-p", "--port",
        required=True,
        type=int,
        metavar="PORT",
        help="server port (1024-65535)",
    )
    args = parser.parse_args()
    if not (1024 <= args.port <= 65535):
        parser.error(f"port must be in range 1024-65535 (got {args.port})")
    return args


def resolve_host(hostname):
    """Resolve hostname -> IP via DNS, failing gracefully on lookup errors."""
    try:
        return socket.gethostbyname(hostname)
    except socket.gaierror:
        print(f"[ERROR] Unable to resolve DNS hostname '{hostname}'.")
        print("Please check your Cisco router DNS configuration "
              "('ip dns server' / 'ip host') or verify domain spelling.")
        sys.exit(1)


def connect(ip, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        return sock
    except OSError as exc:
        print(f"[ERROR] Could not connect to {ip}:{port} ({exc}).")
        sys.exit(1)


def listen_for_server(reader, sock, state, turn_event, game_over_event):
    try:
        while True:
            try:
                messages = reader.read_messages()
            except protocol.ConnectionClosed:
                print("\n[CLIENT] Server closed the connection.")
                return
            except protocol.ProtocolError as exc:
                print(f"\n[CLIENT] Received malformed message from server: {exc}")
                continue
            except OSError as exc:
                print(f"\n[CLIENT] Connection error: {exc}")
                return

            for msg in messages:
                handle_server_message(msg, state, turn_event)
                if state.phase == "GAME_OVER":
                    return
    finally:
        game_over_event.set()
        try:
            sock.close()
        except OSError:
            pass


def handle_server_message(msg, state, turn_event):
    msg_type = msg.get("type")

    if msg_type == protocol.WELCOME:
        with state.lock:
            state.player_id = msg["player_id"]
        if state.player_id == 1:
            print("[CLIENT] Connected as Player 1. Waiting for Player 2...")
        else:
            print("[CLIENT] Connected as Player 2. Player 2 has joined, starting match!")

    elif msg_type == protocol.KICK_START:
        with state.lock:
            state.kick_id = msg["kick_id"]
            state.round = msg["round"]
            state.role = msg["role"]
        print_turn_prompt(state)
        turn_event.set()

    elif msg_type == protocol.LOCKED_IN:
        print("[CLIENT] Choice locked in. Waiting for opponent...")

    elif msg_type == protocol.KICK_RESULT:
        print_kick_result(msg, state)

    elif msg_type == protocol.SCORE_UPDATE:
        print_score_update(msg)

    elif msg_type == protocol.GAME_OVER:
        with state.lock:
            state.phase = "GAME_OVER"
        print_game_over(msg, state)

    elif msg_type == protocol.ERROR:
        print(f"[CLIENT] ERROR ({msg.get('code')}): {msg.get('message')}")
        if state.phase != "GAME_OVER":
            turn_event.set()  # our last choice did not count; let the player retry


def print_turn_prompt(state):
    verb = "shoot" if state.role == "SHOOTER" else "dive to block"
    print(f"\n--- Round {state.round}, Kick {state.kick_id} ---")
    print(GOAL_DIAGRAM, end="")
    print(f"YOUR TURN ({state.role}): choose where to {verb}.")


def prompt_direction(role):
    verb = "shoot" if role == "SHOOTER" else "dive"
    while True:
        raw = input(f"Where do you {verb}? [L]eft / [C]enter / [R]ight: ").strip().upper()
        direction = DIRECTION_ALIASES.get(raw)
        if direction:
            return direction
        print("Invalid input. Please enter L, C, R (or LEFT/CENTER/RIGHT).")


def print_kick_result(msg, state):
    you = state.player_id
    shooter_label = "You" if msg["shooter"] == you else "Opponent"
    outcome = "GOAL!" if msg["goal"] else "SAVED!"
    print(f"\n[RESULT] {shooter_label} shot {msg['shot']}, keeper dove {msg['dive']} -> {outcome}")
    print_scores(msg["scores"], you)


def print_score_update(msg):
    print(f"\n=== Round {msg['round']} complete {'(SUDDEN DEATH) ' if msg['sudden_death'] else ''}===")
    print(" Round |   P1   |   P2")
    print("-------+--------+-------")
    for entry in msg["history"]:
        p1_mark = "GOAL" if entry["1"] else "miss"
        p2_mark = "GOAL" if entry["2"] else "miss"
        print(f"  {entry['round']:>3}  |  {p1_mark:<4}  |  {p2_mark:<4}")
    print_scores(msg["scores"], None)


def print_scores(scores, you):
    if you is None:
        print(f"Score: P1 {scores['1']} - P2 {scores['2']}")
    else:
        opp = 2 if you == 1 else 1
        print(f"Score: You {scores[str(you)]} - Opponent {scores[str(opp)]}")


def print_game_over(msg, state):
    you = state.player_id
    result = "YOU WIN!" if msg["winner"] == you else "YOU LOSE."
    print("\n" + "=" * 40)
    print(f"GAME OVER - {result} ({msg['reason']})")
    print_scores(msg["scores"], you)
    print("=" * 40)


def main():
    args = parse_args()
    server_ip = resolve_host(args.hostname)
    print(f"[CLIENT] Resolved {args.hostname} -> {server_ip}")

    sock = connect(server_ip, args.port)
    print(f"[CLIENT] Connected to {args.hostname} ({server_ip}:{args.port})")

    state = ClientState()
    reader = protocol.MessageReader(sock)
    turn_event = threading.Event()
    game_over_event = threading.Event()

    listener = threading.Thread(
        target=listen_for_server,
        args=(reader, sock, state, turn_event, game_over_event),
        daemon=True,
    )
    listener.start()

    try:
        while not game_over_event.is_set():
            if not turn_event.wait(timeout=0.2):
                continue
            if game_over_event.is_set():
                break
            turn_event.clear()
            with state.lock:
                kick_id, role = state.kick_id, state.role
            direction = prompt_direction(role)
            try:
                protocol.send_message(sock, protocol.CHOOSE, kick_id=kick_id, direction=direction)
            except OSError:
                print("[CLIENT] Failed to send choice; connection may be lost.")
                break
    except (KeyboardInterrupt, EOFError):
        print("\n[CLIENT] Quitting.")
        try:
            protocol.send_message(sock, protocol.QUIT)
        except OSError:
            pass
    finally:
        sock.close()


if __name__ == "__main__":
    main()
