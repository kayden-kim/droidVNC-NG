# 6. Bridge WebSocket Repeaters via a Socket Pair

Date: 2026-11-16

## Status

Accepted

## Context

Repeaters such as [vnc-relay](https://github.com/kayden-kim/vnc-relay) accept UltraVNC-style Mode-2
server registrations not only over plain TCP but also over WebSocket (`ws://`, `wss://`), on the
path `/relay/server`. This lets the server side cross HTTP(S)-only firewalls and reverse proxies.
The bytes inside the WebSocket are the same as over TCP: the 250-byte `ID:<id>` request first,
then the RFB stream. Messages are binary, the client offers subprotocol `binary`. A relay can
additionally require a single-use, short-lived access ticket (a JWS), sent as a second
subprotocol value `ticket.<JWS>`.

LibVNCServer's WebSocket support is server side only: it answers incoming upgrades, sends unmasked
frames and expects masked ones. An outbound WebSocket client needs the opposite, plus TLS with the
platform's trust store.

This is a fork-maintained feature, so changes to upstream-owned code should stay small and the
LibVNCServer submodule should stay untouched.

### Alternatives

#### Alternative 1: WebSocket client inside LibVNCServer

Add client-side framing and TLS to LibVNCServer's socket layer.

**Rejected**: invasive patch to a submodule that is updated regularly, TLS would need its own trust
store handling in native code.

#### Alternative 2: Loopback TCP proxy

Open a `ServerSocket` on 127.0.0.1 and let LibVNCServer reverse-connect to it.

**Rejected**: any app on the device can race to connect to the loopback port first and get its
traffic bridged to the repeater.

#### Alternative 3: Socket pair bridged in Java

Create an `AF_UNIX` socket pair, hand one end to LibVNCServer via `rfbNewClient()` as an already
connected repeater client, and pump the other end to an OkHttp WebSocket.

## Decision

Go for alternative #3.

- `server/WebSocketRepeater.kt` connects with OkHttp, sends the ID request, and bridges.
  Backpressure: OkHttp's reader thread blocks writing into the socket pair; the pump stops reading
  from LibVNCServer while more than 1 MiB is queued for sending.
- `vncConnectRepeaterSocket()` in `droidvnc-ng.c` adopts the socket and sets host, port and
  repeater ID like `rfbUltraVNCRepeaterMode2Connection()` does for TCP.
- `ACTION_CONNECT_REPEATER` takes a `ws://` or `wss://` URL in `EXTRA_HOST`; everything else
  (request ids, reconnects, client list, disconnect) is shared with the TCP repeater path.
  A URL without a path gets `/relay/server`.
- `EXTRA_REPEATER_TICKET` carries the access ticket. A ticket must not be presented twice, and
  retrying without it would silently downgrade authentication, so connections with a ticket never
  auto-reconnect. A ticket with a plain TCP repeater fails the request for the same reason.
- `android:usesCleartextTraffic="true"`: OkHttp, unlike native sockets, honours the network
  security policy, which forbids `ws://` by default.

## Consequences

- **Positive**: LibVNCServer stays unmodified; the native change is one additive JNI function.
- **Positive**: TLS uses the platform's trust store and hostname verification.
- **Neutral**: LibVNCServer logs harmless errors for the socket pair (no peer address, no
  `TCP_NODELAY`).
- **Negative**: one extra thread and one buffer copy per direction and connection.
- **Negative**: new dependency on OkHttp.
- **Negative**: ticket-protected connections do not auto-reconnect; the calling app has to send a
  new `ACTION_CONNECT_REPEATER` with a fresh ticket instead.
- **Negative**: cleartext HTTP is permitted app-wide, not only for `ws://` repeaters.
