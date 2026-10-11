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
- generic sender API (`send<T>`, `values<T>`, `subscribe<T>`)

# JSON engines

Two decode implementations are available:

- `DartJsonEngine` (default, `dart:convert`)
- `FastJsonEngine` (`fast_json`)

# Tests
Interoperability tests run against the TypeScript fixture server
(`../channel/tests/server.ts`) on `127.0.0.1:2049`.
