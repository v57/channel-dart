<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Dart implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Async/await and stream values are supported out of the box.

# Installation
```bash
dart pub add channel_dart
```

# Dependencies

One package: [`fast_json`](https://pub.dev/packages/fast_json), an optional faster JSON decoder (`JsonEngineKind.fast`).
The default engine uses `dart:convert`, and the WebSocket client and server use `dart:io`.

The optional QUIC transport (`package:channel_dart/quic.dart`) adds no package: it calls OpenSSL 3.5+'s native QUIC
through `dart:ffi` and needs `libssl`/`libcrypto` 3.5 or later at run time (Debian 13, Ubuntu 25.04+, Homebrew's
`openssl@3`). Programs that don't import it never load OpenSSL.

# Usage

```dart
import 'package:channel_dart/client.dart';
import 'package:channel_dart/server.dart';

Future<void> main() async {
  // Server
  final server = await Channel<Map<String, Object?>>()
      .post('hello', (_) => 'world')
      .listen(2049);

  // Client
  final client = Channel<Map<String, Object?>>().connect(
    2049,
    options: ClientOptions<Map<String, Object?>>(state: <String, Object?>{}),
  );

  final value = await client.send<String>('hello');
  print(value); // world

  client.stop();
  await server.stop();
}
```

# Transports
- client transport (`lib/client.dart`)
- server transport (`lib/server.dart`)
- process/in-memory transport (`lib/process.dart`)
- QUIC client and server, experimental (`lib/quic.dart`, see below)
- generic sender API (`send<T>`, `values<T>`, `subscribe<T>`)

# QUIC (experimental)

The protocol over one QUIC stream, as in [channel/docs/quic.md](../channel/docs/quic.md): ALPN `channel/1`, TLS
always on, the client's headers in a first `hello` frame. Same requests, streams, events, reconnect and replay as
the WebSocket client.

```dart
import 'package:channel_dart/quic.dart';

// Server: QUIC has no plaintext mode, so it needs a certificate and key (PEM)
final server = await Channel<Map<String, String>>()
    .post('hello', (_) => 'world')
    .listenQuic(4433, options: QuicListenOptions(cert: cert, key: key, state: (headers) => headers));

// Client
final client = Channel<Object?>().connectQuic(
  'quic://127.0.0.1:4433',
  quic: QuicOptions(ca: cert), // or fingerprint: 'DD:D2:…', servername: 'localhost'
);
print(await client.send<String>('hello'));
```

- `QuicOptions`: `ca` trusts a self-signed certificate (the address must be in its names), `fingerprint` pins the
  certificate's SHA-256 instead of verifying it, `servername` overrides the name sent and checked. Without them the
  system's roots verify the server.
- `listenQuic` takes `port` or `'host:port'` (127.0.0.1 by default), and the same `state`, `onConnect`,
  `onDisconnect` and `maxMessageSize` as `listen`. Run `listen` next to it for WebSocket clients.
- Each connection dials from its own UDP socket. OpenSSL owns the socket; a helper isolate waits on it with
  `poll(2)` and wakes the event loop, which does the OpenSSL work.
- OpenSSL can't lower the stream limits: the server resets any stream after a connection's first one.
- Set `CHANNEL_QUIC_LIBSSL` / `CHANNEL_QUIC_LIBCRYPTO` to load OpenSSL from another path. Without OpenSSL 3.5+,
  `connectQuic` and `listenQuic` throw `QuicUnavailable`.

# JSON engines

Two decode implementations are available:

- `DartJsonEngine` (default, `dart:convert`)
- `FastJsonEngine` (`fast_json`)

# Tests
Interoperability tests run against the TypeScript fixture server
(`../channel/tests/server.ts`) on `127.0.0.1:2049`. `test/quic_test.dart` starts its own fixture on 2449
(`CHANNEL_QUIC_TEST_PORT`) for the QUIC interop tests, and runs the TypeScript reference's QUIC client against the
Dart server.
