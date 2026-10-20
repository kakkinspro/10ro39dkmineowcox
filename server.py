#!/usr/bin/env python3
"""
Zombie Killer - server (sostituisce `python -m http.server`)

  python server.py            -> porta 8000
  python server.py 9000       -> porta 9000

Fa due cose, usando SOLO la libreria standard di Python (nessuna installazione):
  1. serve i file del gioco (html, png, ...);
  2. fa da "ponte" WebSocket (/ws) tra i due giocatori del multiplayer online:
     i giocatori non si collegano direttamente fra loro, ma entrambi al server,
     quindi firewall e NAT dei giocatori non sono piu' un problema.
"""
import asyncio
import base64
import hashlib
import json
import mimetypes
import os
import random
import socket
import struct
import sys
import urllib.parse

ROOT = os.path.dirname(os.path.abspath(__file__))
PORT = int(os.environ.get("PORT") or (sys.argv[1] if len(sys.argv) > 1 else 8000))
GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_MSG = 512 * 1024          # dimensione massima di un messaggio WebSocket
MAX_ROOMS = 500
BUF_LIMIT = 768 * 1024        # se il buffer di un client supera questo, scarto i dati (client lento)
INDEX_FILES = ("index.html", "ZombieKiller.html")

mimetypes.add_type("text/javascript", ".js")
mimetypes.add_type("image/webp", ".webp")
mimetypes.add_type("audio/mpeg", ".mp3")

rooms = {}   # id stanza -> Client (host)


# ----------------------------------------------------------------- WebSocket
def unmask(data, mask):
    n = len(data)
    if not n:
        return data
    m = (mask * (n // 4 + 1))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(m, "big")).to_bytes(n, "big")


def make_frame(op, data=b""):
    n = len(data)
    head = bytes([0x80 | op])
    if n < 126:
        head += bytes([n])
    elif n < 65536:
        head += bytes([126]) + struct.pack(">H", n)
    else:
        head += bytes([127]) + struct.pack(">Q", n)
    return head + data


async def read_message(reader):
    """Legge un messaggio completo (gestisce frammentazione). Ritorna (opcode, bytes)."""
    buf, first_op = b"", 0
    while True:
        b1, b2 = await reader.readexactly(2)
        fin, op, masked, ln = b1 & 0x80, b1 & 0x0F, b2 & 0x80, b2 & 0x7F
        if ln == 126:
            ln = struct.unpack(">H", await reader.readexactly(2))[0]
        elif ln == 127:
            ln = struct.unpack(">Q", await reader.readexactly(8))[0]
        if ln > MAX_MSG or len(buf) + ln > MAX_MSG:
            raise ValueError("messaggio troppo grande")
        mask = await reader.readexactly(4) if masked else None
        data = await reader.readexactly(ln) if ln else b""
        if mask:
            data = unmask(data, mask)
        if op >= 8:                       # frame di controllo (close/ping/pong)
            return op, data
        if op != 0:
            first_op = op
        buf += data
        if fin:
            return first_op, buf


class Client:
    def __init__(self, writer):
        self.writer = writer
        self.role = None          # 'host' | 'guest'
        self.id = None            # id stanza (solo host)
        self.guests = {}          # host: cid -> Client
        self.host = None          # guest: Client host
        self.cid = None           # guest: id della connessione presso l'host
        self.alive = True

    def raw(self, frame):
        if not self.alive:
            return
        try:
            self.writer.write(frame)
        except Exception:
            self.alive = False

    def send(self, obj, droppable=False):
        if droppable:
            try:
                if self.writer.transport.get_write_buffer_size() > BUF_LIMIT:
                    return
            except Exception:
                pass
        self.raw(make_frame(1, json.dumps(obj, separators=(",", ":")).encode()))


def detach(c):
    """Sgancia il client dalla stanza e avvisa l'altro lato."""
    if c.role == "guest" and c.host is not None:
        h, cid = c.host, c.cid
        c.host = None
        if h.guests.pop(cid, None) is not None:
            h.send({"op": "closed", "cid": cid})
    elif c.role == "host":
        if c.id and rooms.get(c.id) is c:
            del rooms[c.id]
        for g in list(c.guests.values()):
            g.host = None
            g.send({"op": "closed"})
        c.guests.clear()


def handle_msg(c, m):
    op = m.get("op")
    if op == "reg":
        if c.role:
            return
        rid = m.get("id")
        if rid:
            rid = str(rid)[:64]
            if rid in rooms or len(rooms) >= MAX_ROOMS:
                c.send({"op": "error", "type": "unavailable-id"})
                return
            c.role, c.id = "host", rid
            rooms[rid] = c
            c.send({"op": "open", "id": rid})
        else:
            c.role = "guest"
            c.send({"op": "open", "id": "g%06d" % random.randrange(10 ** 6)})
    elif op == "join" and c.role == "guest":
        if c.host is not None:
            return
        h = rooms.get(str(m.get("to", "")))
        if not h:
            c.send({"op": "error", "type": "peer-unavailable"})
            return
        c.cid = random.getrandbits(40)
        c.host = h
        h.guests[c.cid] = c
        h.send({"op": "incoming", "cid": c.cid})
        c.send({"op": "joined"})
    elif op == "data":
        d = m.get("d")
        if c.role == "guest" and c.host is not None:
            c.host.send({"op": "data", "cid": c.cid, "d": d}, droppable=True)
        elif c.role == "host":
            g = c.guests.get(m.get("cid"))
            if g:
                g.send({"op": "data", "d": d}, droppable=True)
    elif op == "close":
        if c.role == "guest":
            detach(c)
        elif c.role == "host":
            g = c.guests.pop(m.get("cid"), None)
            if g:
                g.host = None
                g.send({"op": "closed"})


async def ws_session(reader, writer):
    c = Client(writer)
    c.send({"op": "hello", "relay": 1})

    async def pinger():
        while c.alive:
            await asyncio.sleep(25)
            c.raw(make_frame(9, b"k"))

    ptask = asyncio.ensure_future(pinger())
    try:
        while True:
            op, data = await asyncio.wait_for(read_message(reader), 120)
            if op == 8:
                break
            if op == 9:
                c.raw(make_frame(10, data))
            elif op in (1, 2):
                try:
                    m = json.loads(data.decode("utf-8"))
                except Exception:
                    continue
                if isinstance(m, dict):
                    handle_msg(c, m)
    except Exception:
        pass
    finally:
        ptask.cancel()
        c.alive = False
        detach(c)
        try:
            writer.write(make_frame(8, b""))
            writer.close()
        except Exception:
            pass


# ------------------------------------------------------------------- HTTP
def http_reply(writer, code, reason, body=b"", ctype="text/plain; charset=utf-8", head=False, extra=""):
    hdr = (
        "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n"
        "Cache-Control: no-cache\r\nConnection: close\r\n%s\r\n" % (code, reason, ctype, len(body), extra)
    ).encode()
    writer.write(hdr + (b"" if head else body))


def resolve(path):
    path = urllib.parse.unquote(path.split("?", 1)[0].split("#", 1)[0])
    full = os.path.normpath(os.path.join(ROOT, path.lstrip("/")))
    if full != ROOT and not full.startswith(ROOT + os.sep):
        return None
    if os.path.isdir(full):
        for name in INDEX_FILES:
            p = os.path.join(full, name)
            if os.path.isfile(p):
                return p
        return None
    if full.lower().endswith((".py", ".pyc")) or not os.path.isfile(full):
        return None
    return full


async def handle(reader, writer):
    try:
        raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 15)
        lines = raw.decode("latin-1").split("\r\n")
        method, target, _ = lines[0].split(" ", 2)
        headers = {}
        for ln in lines[1:]:
            if ":" in ln:
                k, v = ln.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        if urllib.parse.urlparse(target).path == "/ws" and "websocket" in headers.get("upgrade", "").lower():
            key = headers.get("sec-websocket-key", "")
            acc = base64.b64encode(hashlib.sha1(key.encode() + GUID).digest()).decode()
            writer.write(
                ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                 "Sec-WebSocket-Accept: %s\r\n\r\n" % acc).encode()
            )
            await ws_session(reader, writer)
            return

        if method not in ("GET", "HEAD"):
            http_reply(writer, 405, "Method Not Allowed", b"405")
        else:
            f = resolve(target)
            if not f:
                http_reply(writer, 404, "Not Found", b"404 - file non trovato", head=method == "HEAD")
            else:
                with open(f, "rb") as fh:
                    body = fh.read()
                ctype = mimetypes.guess_type(f)[0] or "application/octet-stream"
                if ctype.startswith("text/") or ctype in ("application/json", "text/javascript"):
                    ctype += "; charset=utf-8"
                http_reply(writer, 200, "OK", body, ctype, head=method == "HEAD")
        await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


def lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))     # nessun pacchetto inviato: serve solo a scoprire l'IP locale
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return None


async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", PORT)
    print("=" * 60)
    print(" Zombie Killer - server avviato (con ponte multiplayer)")
    print("=" * 60)
    print(" Sul tuo PC:          http://localhost:%d" % PORT)
    ip = lan_ip()
    if ip:
        print(" Stessa rete / Wi-Fi: http://%s:%d" % (ip, PORT))
    print(" Per giocare via internet vedi le istruzioni (tunnel o hosting).")
    print(" CTRL+C per fermare.")
    print("=" * 60)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer fermato.")
