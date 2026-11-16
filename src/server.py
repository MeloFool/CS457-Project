"""Penalty Shootout game server.

One thread per connected client (see docs/concurrency_design.md). The main
thread only accepts connections; each worker thread blocks on recv() for its
own client and mutates/broadcasts GameState under a single state_lock.

Usage:
    python3 server.py -p 8080
"""

import argparse
import socket
import sys
import threading

import protocol

PHASE_WAITING = "WAITING_FOR_PLAYERS"
PHASE_CHOOSING = "CHOOSING"
PHASE_GAME_OVER = "GAME_OVER"

ROUNDS_BEFORE_SUDDEN_DEATH = 5


class GameState:
    """All state shared between the two worker threads, guarded by `lock`."""

    def __init__(self):
        self.lock = threading.Lock()
        self.players = {}  # {player_id: socket}
        self.phase = PHASE_WAITING
        self.round = 0
        self.kick_id = 0
        self.shooter = None
        self.keeper = None
        self.pending = {1: None, 2: None}  # {player_id: direction or None}
        self.scores = {1: 0, 2: 0}
        self.history = []
        self.round_results = []  # kicks resolved so far in the current round


def parse_args():
    parser = argparse.ArgumentParser(
        prog="server.py",
        description="Penalty Shootout game server.",
        epilog="Example: python3 server.py -p 8080",
    )
    parser.add_argument(
        "-p", "--port",
        required=True,
        type=int,
        metavar="PORT",
        help="listening port to bind on 0.0.0.0 (1024-65535)",
    )
    args = parser.parse_args()
    if not (1024 <= args.port <= 65535):
        parser.error(f"port must be in range 1024-65535 (got {args.port})")
    return args


def broadcast(state, msg_type, **fields):
    for sock in state.players.values():
        protocol.send_message(sock, msg_type, **fields)


def broadcast_kick_start(state):
    for player_id, sock in state.players.items():
        role = "SHOOTER" if player_id == state.shooter else "KEEPER"
        protocol.send_message(
            sock, protocol.KICK_START,
            kick_id=state.kick_id, round=state.round, role=role,
        )


def start_match(state):
    state.phase = PHASE_CHOOSING
    state.round = 1
    state.kick_id = 1
    state.shooter, state.keeper = 1, 2
    state.pending = {1: None, 2: None}
    state.scores = {1: 0, 2: 0}
    state.history = []
    state.round_results = []
    print("[SERVER] Both players connected, match starting.")
    broadcast_kick_start(state)


def on_connect(conn, addr, state):
    """Register a new connection under the lock. Returns the assigned
    player_id, or None if the connection was rejected (game already full)."""
    with state.lock:
        if len(state.players) >= 2:
            full = True
            player_id = None
        else:
            full = False
            player_id = 1 if 1 not in state.players else 2
            state.players[player_id] = conn
            protocol.send_message(conn, protocol.WELCOME, player_id=player_id)
            if len(state.players) == 2:
                start_match(state)

    if full:
        try:
            protocol.send_message(
                conn, protocol.ERROR,
                code=protocol.GAME_FULL, message="Game already has two players.",
            )
        except OSError:
            pass
        conn.close()
        return None
    return player_id


def resolve_kick(state):
    """Called with both players' choices in for the current kick. Must be
    called while holding state.lock."""
    shooter_id, keeper_id = state.shooter, state.keeper
    shot, dive = state.pending[shooter_id], state.pending[keeper_id]
    goal = shot != dive
    if goal:
        state.scores[shooter_id] += 1

    print(f"[SERVER] Kick {state.kick_id}: P{shooter_id} shot {shot}, "
          f"P{keeper_id} dove {dive} -> {'GOAL' if goal else 'SAVE'}")
    broadcast(
        state, protocol.KICK_RESULT,
        kick_id=state.kick_id, shooter=shooter_id, shot=shot, dive=dive,
        goal=goal, scores=dict(state.scores),
    )

    state.round_results.append({"shooter": shooter_id, "goal": goal})
    # Roles swap after every kick, regardless of round completion.
    state.shooter, state.keeper = state.keeper, state.shooter
    state.pending = {1: None, 2: None}

    # A round is two kicks (each player shoots once); only then can we
    # check round/match completion and swap out round_results.
    if len(state.round_results) == 2:
        p1_scored = any(r["shooter"] == 1 and r["goal"] for r in state.round_results)
        p2_scored = any(r["shooter"] == 2 and r["goal"] for r in state.round_results)
        state.history.append({"round": state.round, 1: p1_scored, 2: p2_scored})
        state.round_results = []

        sudden_death = state.round >= ROUNDS_BEFORE_SUDDEN_DEATH
        broadcast(
            state, protocol.SCORE_UPDATE,
            round=state.round, scores=dict(state.scores),
            history=list(state.history), sudden_death=sudden_death,
        )

        if sudden_death and state.scores[1] != state.scores[2]:
            winner = 1 if state.scores[1] > state.scores[2] else 2
            state.phase = PHASE_GAME_OVER
            print(f"[SERVER] Game over: P{winner} wins {state.scores}")
            broadcast(
                state, protocol.GAME_OVER,
                winner=winner, reason="WIN", scores=dict(state.scores),
            )
            return

        state.round += 1

    state.kick_id += 1
    broadcast_kick_start(state)


def handle_choose(player_id, msg, state):
    sock = state.players[player_id]

    if state.phase != PHASE_CHOOSING:
        protocol.send_message(
            sock, protocol.ERROR, code=protocol.NOT_ACCEPTING_CHOICES,
            message="Not currently accepting choices.",
        )
        return

    kick_id = msg.get("kick_id")
    if kick_id != state.kick_id:
        protocol.send_message(
            sock, protocol.ERROR, code=protocol.STALE_KICK,
            message="This kick has already been resolved.",
        )
        return

    if state.pending.get(player_id) is not None:
        protocol.send_message(
            sock, protocol.ERROR, code=protocol.ALREADY_SUBMITTED,
            message="You already locked in a choice for this kick.",
        )
        return

    direction = msg.get("direction")
    if direction not in protocol.DIRECTIONS:
        protocol.send_message(
            sock, protocol.ERROR, code=protocol.INVALID_DIRECTION,
            message=f"direction must be one of {protocol.DIRECTIONS}",
        )
        return

    state.pending[player_id] = direction
    if state.pending[state.shooter] is not None and state.pending[state.keeper] is not None:
        resolve_kick(state)
    else:
        protocol.send_message(sock, protocol.LOCKED_IN, kick_id=kick_id)


def handle_quit(player_id, state):
    if state.phase == PHASE_GAME_OVER:
        return
    state.phase = PHASE_GAME_OVER
    other_id = 2 if player_id == 1 else 1
    other_sock = state.players.get(other_id)
    if other_sock is not None:
        protocol.send_message(
            other_sock, protocol.GAME_OVER,
            winner=other_id, reason="FORFEIT", scores=dict(state.scores),
        )


def process_message(player_id, msg, state):
    msg_type = msg.get("type")
    if msg_type == protocol.CHOOSE:
        handle_choose(player_id, msg, state)
    elif msg_type == protocol.QUIT:
        handle_quit(player_id, state)
    else:
        protocol.send_message(
            state.players[player_id], protocol.ERROR,
            code=protocol.MALFORMED_MESSAGE, message=f"unknown message type '{msg_type}'",
        )


def handle_disconnect(player_id, state):
    with state.lock:
        sock = state.players.pop(player_id, None)
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

        print(f"[SERVER] Player {player_id} disconnected.")
        if state.phase == PHASE_CHOOSING:
            state.phase = PHASE_GAME_OVER
            other_id = 2 if player_id == 1 else 1
            other_sock = state.players.get(other_id)
            if other_sock is not None:
                try:
                    protocol.send_message(
                        other_sock, protocol.GAME_OVER,
                        winner=other_id, reason="FORFEIT", scores=dict(state.scores),
                    )
                except OSError:
                    pass
        # If phase was WAITING_FOR_PLAYERS, the slot is simply freed.
        # If phase was already GAME_OVER, there is nothing left to do.


def handle_client(conn, player_id, state):
    reader = protocol.MessageReader(conn)
    while True:
        try:
            messages = reader.read_messages()
        except protocol.ConnectionClosed:
            handle_disconnect(player_id, state)
            return
        except OSError:
            handle_disconnect(player_id, state)
            return
        except protocol.ProtocolError as exc:
            with state.lock:
                try:
                    protocol.send_message(
                        conn, protocol.ERROR,
                        code=protocol.MALFORMED_MESSAGE, message=str(exc),
                    )
                except OSError:
                    handle_disconnect(player_id, state)
                    return
            continue

        for msg in messages:
            with state.lock:
                try:
                    process_message(player_id, msg, state)
                except OSError:
                    handle_disconnect(player_id, state)
                    return


def main():
    args = parse_args()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("0.0.0.0", args.port))
    server_sock.listen()
    print(f"[SERVER] Listening on 0.0.0.0:{args.port}")

    state = GameState()
    try:
        while True:
            conn, addr = server_sock.accept()
            player_id = on_connect(conn, addr, state)
            if player_id is None:
                print(f"[SERVER] Rejected connection from {addr}: game full")
                continue
            print(f"[SERVER] Player {player_id} connected from {addr}")
            threading.Thread(
                target=handle_client, args=(conn, player_id, state), daemon=True,
            ).start()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down.")
    finally:
        server_sock.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
