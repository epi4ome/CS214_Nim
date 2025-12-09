#!/usr/bin/env python3
import socket, sys, time

def send_frame(sock, payload):
    length = len(payload)
    frame = f"0|{length:02d}|{payload}"
    sock.sendall(frame.encode())
    print(f"SEND: {frame}")

def recv_frame(sock):
    header = sock.recv(5)
    if not header:
        return None
    if len(header) < 5:
        return None

    length = int(header[2:4])
    payload = sock.recv(length)
    if not payload or len(payload) < length:
        return None

    msg = (header + payload).decode()
    print(f"RECV: {msg}")
    return msg

def play_script(host, port, name, moves):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # OPEN
    send_frame(sock, f"OPEN|{name}|")

    while True:
        msg = recv_frame(sock)
        if msg is None:
            print("Disconnected")
            break

        parts = msg.split("|")

        if "WAIT" in msg:
            continue

        if "NAME" in msg:
            continue

        if "PLAY" in msg:
            # PLAY|turn|board|
            # parts = ['0','LL','PLAY','turn','board','']
            if len(parts) < 5:
                continue

            if parts[2] != "PLAY":
                continue

            try:
                turn = int(parts[3])
            except:
                print("Invalid PLAY turn field:", parts)
                continue

            board = parts[4]

            # Is it our turn?
            if name == "Bob" and turn == 1:
                if moves:
                    pile, qty = moves.pop(0)
                    time.sleep(0.2)
                    send_frame(sock, f"MOVE|{pile}|{qty}|")

            if name == "Oskar" and turn == 2:
                if moves:
                    pile, qty = moves.pop(0)
                    time.sleep(0.2)
                    send_frame(sock, f"MOVE|{pile}|{qty}|")

        if "OVER" in msg:
            break

    sock.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: ngp_client.py <name> <host> [port]")
        sys.exit(1)

    name = sys.argv[1]
    host = sys.argv[2]
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 15000

    # Example move scripts
    script_bob   = [(1,1), (3,5), (5,9)]
    script_oskar = [(2,3), (4,7)]

    moves = script_bob if name == "Bob" else script_oskar

    play_script(host, port, name, moves[:])
