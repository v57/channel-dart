// Regressions for AUDIT.md §3.1 and AUDIT2.md §1.7/§2/§4 (2026-10).
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:channel_dart/client.dart';
import 'package:channel_dart/server.dart';
import 'package:channel_dart/src/server/frame_limit.dart';
import 'package:test/test.dart';

/// A WebSocket server that answers the first request with [reply] (mirrors audit2/hostile.ts).
Future<HttpServer> hostileServer(
  void Function(WebSocket ws, int id) reply,
) async {
  final server = await HttpServer.bind('127.0.0.1', 0);
  server.listen((HttpRequest request) async {
    final ws = await WebSocketTransformer.upgrade(request);
    ws.listen((dynamic message) {
      final decoded = jsonDecode(message as String);
      final first = decoded is List ? decoded.first : decoded;
      reply(ws, (first as Map)['id'] as int);
    });
  });
  return server;
}

void main() {
  test('Sec-WebSocket-Accept matches the RFC 6455 example', () {
    expect(
      webSocketAcceptKey('dGhlIHNhbXBsZSBub25jZQ=='),
      's3pPLMBiTxaQ9kYGzzhZRbK+xOo=',
    );
  });

  test('Subscription.publish drops every falsy body like TS', () {
    final events = <Object?>[];
    final subscription = Subscription()..publishers.add(_ListPublisher(events));
    for (final body in <Object?>[null, false, 0, 0.0, double.nan, '']) {
      subscription.publish('t', body);
    }
    expect(events, isEmpty);
    for (final body in <Object?>['0', true, 1, <String, Object?>{}]) {
      subscription.publish('t', body);
    }
    expect(events.length, 4);
  });

  group('client vs hostile server', () {
    test('unparseable reply fails the call instead of crashing', () async {
      final server = await hostileServer((ws, id) => ws.add('{oops'));
      final client = Channel<Map<String, Object?>>().connect(server.port);
      try {
        await expectLater(
          client.send<Object?>('text/count', body: {'text': 'hello'}),
          throwsA('invalid message from server'),
        );
      } finally {
        client.stop();
        await server.close(force: true);
      }
    });

    test('error-object reply surfaces as an error', () async {
      final server = await hostileServer(
        (ws, id) => ws.add(
          jsonEncode({
            'id': id,
            'error': {'code': 500, 'message': 'nope'},
          }),
        ),
      );
      final client = Channel<Map<String, Object?>>().connect(server.port);
      try {
        await expectLater(
          client.send<Object?>('text/count', body: {'text': 'hello'}),
          throwsA({'code': 500, 'message': 'nope'}),
        );
      } finally {
        client.stop();
        await server.close(force: true);
      }
    });

    test('stop settles a queued call', () async {
      final probe = await ServerSocket.bind('127.0.0.1', 0);
      final port = probe.port;
      await probe.close(); // nothing listens: the call stays queued
      final client = Channel<Map<String, Object?>>().connect(port);
      final call = client.send<Object?>('never');
      await Future<void>.delayed(const Duration(milliseconds: 50));
      client.stop();
      await expectLater(call, throwsA('disconnected'));
    });
  });

  group('server frame limit', () {
    late ChannelServer<Map<String, Object?>> server;
    late int port;

    setUp(() async {
      final probe = await ServerSocket.bind('127.0.0.1', 0);
      port = probe.port;
      await probe.close();
      server = await Channel<Map<String, Object?>>()
          .post('echo', (body) => body.body)
          .listen(
            port,
            options: const ListenOptions<Map<String, Object?>>(
              maxMessageSize: 1024,
            ),
          );
    });

    tearDown(() => server.stop());

    test('messages under the cap still work', () async {
      final client = Channel<Map<String, Object?>>().connect(port);
      try {
        expect(await client.send<Object?>('echo', body: 'x' * 512), 'x' * 512);
      } finally {
        client.stop();
      }
    });

    test('oversized message closes with 1009', () async {
      final ws = await WebSocket.connect('ws://127.0.0.1:$port');
      ws.add(jsonEncode({'id': 1, 'path': 'echo', 'body': 'x' * 4096}));
      await ws
          .listen((_) {})
          .asFuture<void>()
          .timeout(const Duration(seconds: 5));
      expect(ws.closeCode, 1009);
    });

    test('declared 1 GB frame is rejected before its payload arrives', () async {
      final socket = await Socket.connect('127.0.0.1', port);
      final key = base64.encode(
        List<int>.generate(16, (_) => Random().nextInt(256)),
      );
      socket.write(
        'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n'
        'Connection: Upgrade\r\nSec-WebSocket-Key: $key\r\n'
        'Sec-WebSocket-Version: 13\r\n\r\n',
      );
      final received = <int>[];
      final closed = Completer<void>();
      socket.listen(
        received.addAll,
        onDone: closed.complete,
        onError: (Object _) => closed.complete(),
      );
      await Future<void>.delayed(const Duration(milliseconds: 100));
      // FIN+text, masked, 64-bit length = 1 GiB, mask key, then a few payload bytes.
      final header = BytesBuilder()
        ..add([0x81, 0xff])
        ..add((ByteData(8)..setUint64(0, 1 << 30)).buffer.asUint8List())
        ..add([1, 2, 3, 4, 5, 6, 7, 8]);
      socket.add(header.toBytes());
      await closed.future.timeout(const Duration(seconds: 5));
      final text = latin1.decode(received);
      expect(text, startsWith('HTTP/1.1 101'));
      expect(text, contains(webSocketAcceptKey(key)));
      // The server's close frame carries 1009.
      expect(received.sublist(received.length - 4), [0x88, 0x02, 0x03, 0xf1]);
      socket.destroy();
    });
  });
}

class _ListPublisher implements SubscriptionPublisher {
  _ListPublisher(this.events);

  final List<Object?> events;

  @override
  void publish(SubscriptionEvent event) => events.add(event.body);
}
