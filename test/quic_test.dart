// QUIC transport: frames, the Dart client against the Dart server, the wire rules with raw frames, trust,
// reconnect, the Dart client against the TypeScript fixture's QUIC server, and the TypeScript reference's client
// against the Dart server. Mirrors channel-py/tests/quic and channel-zig/tests/quic.
@Timeout(Duration(seconds: 60))
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:channel_dart/quic.dart';
import 'package:channel_dart/src/quic/quic_client.dart' show RawQuicConnection;
import 'package:test/test.dart';

import 'support/ts_server_fixture.dart';

// The TypeScript reference's test certificate: localhost, 127.0.0.1 and ::1
final certDir = Directory.fromUri(Directory.current.uri.resolve('../channel/tests/quic/'));
const fingerprint = 'DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B';

String? readPem(String name) {
  final file = File('${certDir.path}$name');
  return file.existsSync() ? file.readAsStringSync() : null;
}

String? quicUnavailable() {
  try {
    Channel<Object?>().connectQuic('quic://127.0.0.1:9', quic: const QuicOptions(fingerprint: fingerprint)).stop();
    return null;
  } on QuicUnavailable catch (error) {
    return '$error';
  }
}

Future<void> waitFor(bool Function() ready, [Duration timeout = const Duration(seconds: 5)]) async {
  final deadline = DateTime.now().add(timeout);
  while (!ready()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('timed out');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

class Fixture {
  Fixture(this.cert, this.key);

  final String cert;
  final String key;
  final ticks = Subscription();
  final servers = <QuicChannelServer<Map<String, String>>>[];
  late QuicChannelServer<Map<String, String>> server;
  var connected = 0;
  var disconnected = 0;
  var dropped = false;

  String get address => 'quic://127.0.0.1:${server.port}';

  Channel<Map<String, String>> channel() {
    return Channel<Map<String, String>>()
      ..post('hello', (_) => 'world')
      ..post('echo', (body) => body.body)
      ..post('name', (body) => body.state['x-name'])
      ..post('mirror', (body) => body.sender.send<String>('hello'))
      ..post('slow', (_) async {
        await Future<void>.delayed(const Duration(milliseconds: 300));
        return 'slow';
      })
      ..post('disconnect', (body) {
        // Once: the client sends its unanswered requests again after reconnecting
        if (!dropped) {
          dropped = true;
          body.sender.stop();
          return Completer<Object?>().future;
        }
        return 'dropped before';
      })
      ..stream('count', (_) async* {
        for (var i = 0; i < 3; i += 1) {
          yield i;
        }
      })
      ..post('tick', (_) {
        ticks.send('a', 'tick');
        return null;
      })
      ..events({'ticks': ticks});
  }

  Future<QuicChannelServer<Map<String, String>>> listen([Object address = '127.0.0.1:0']) async {
    server = await channel().listenQuic(
      address,
      options: QuicListenOptions<Map<String, String>>(
        cert: cert,
        key: key,
        state: (headers) => headers,
        onConnect: (_) => connected += 1,
        onDisconnect: (_) => disconnected += 1,
      ),
    );
    servers.add(server);
    return server;
  }

  Future<void> stop() async {
    for (final server in servers) {
      await server.stop();
    }
  }
}

Channel<Map<String, Object?>> clientChannel() => Channel<Map<String, Object?>>()..post('hello', (_) => 'client world');

void main() {
  final cert = readPem('cert.pem');
  final key = readPem('key.pem');
  final unavailable = cert == null || key == null ? "the TypeScript reference's test certificate isn't there" : quicUnavailable();

  group('frames', () {
    test('lengths and keepalives', () {
      for (final size in [0, 1, 63, 64, 16383, 16384, 70000]) {
        final payload = Uint8List(size)..fillRange(0, size, 0x78);
        final frame = encodeQuicFrame(payload);
        expect(frame.length - size, size < 64 ? 1 : size < 16384 ? 2 : 4, reason: '$size');
        // A keepalive in front is skipped, and an empty frame yields nothing
        final stream = [0, ...frame, ...encodeQuicFrame(utf8.encode('next'))];
        final expected = [if (size > 0) payload, utf8.encode('next')];
        expect(QuicFrameReader().push(stream), expected);
        // Byte by byte gives the same frames
        final reader = QuicFrameReader();
        final got = <List<int>>[];
        for (final byte in stream) {
          got.addAll(reader.push([byte]));
        }
        expect(got, expected);
      }
    });

    test('8-byte lengths and the limit', () {
      expect(QuicFrameReader().push([0xc0, 0, 0, 0, 0, 0, 0, 5, ...utf8.encode('hello')]), [utf8.encode('hello')]);
      expect(encodeQuicFrame(const []), [0]);
      expect(encodeQuicFrame(List.filled(64, 0x61)).sublist(0, 2), [0x40, 64]);
      // Anything above the limit is refused as soon as its length is known
      expect(() => QuicFrameReader(10).push(encodeQuicFrame(List.filled(11, 0x78)).sublist(0, 4)), throwsA(isA<QuicFrameTooLarge>()));
      expect(QuicFrameReader(10).push(encodeQuicFrame(List.filled(10, 0x78))), [List.filled(10, 0x78)]);
      expect(() => QuicFrameReader().push([0x81, 0, 0, 1]), throwsA(isA<QuicFrameTooLarge>())); // 16 MiB + 1
      expect(utf8.decode(encodeQuicHello({'X-Name': 'a'}).sublist(1)), '{"hello":{"X-Name":"a"}}');
    });

    test('addresses and fingerprints are checked up front', () {
      final channel = Channel<Object?>();
      expect(() => channel.connect('quic://127.0.0.1:1'), throwsArgumentError);
      expect(() => channel.connectQuic('ws://127.0.0.1:1'), throwsArgumentError);
      expect(() => channel.connectQuic('quic://127.0.0.1'), throwsArgumentError);
      expect(() => channel.connectQuic('quic://127.0.0.1:1', quic: const QuicOptions(fingerprint: 'nope')), throwsArgumentError);
    });
  });

  group('Dart client and server', skip: unavailable, () {
    late Fixture fixture;
    final clients = <ClientSender<Map<String, Object?>>>[];

    ClientSender<Map<String, Object?>> connect(String address, QuicOptions quic, {Map<String, String>? headers}) {
      final client = clientChannel().connectQuic(
        address,
        quic: quic,
        options: ClientOptions<Map<String, Object?>>(state: {}, headers: headers == null ? null : () => headers),
      );
      clients.add(client);
      return client;
    }

    setUp(() async {
      fixture = Fixture(cert!, key!);
      await fixture.listen();
    });

    tearDown(() async {
      for (final client in clients) {
        client.stop();
      }
      clients.clear();
      await fixture.stop();
    });

    test('posts, hello headers as state, streams, events, many in flight', () async {
      final c = connect(fixture.address, QuicOptions(ca: cert), headers: {'X-Name': 'dart'});
      expect(await c.send<String>('hello').timeout(const Duration(seconds: 5)), 'world');
      final body = {
        'list': [1, 'two', null],
        'text': 'é' * 20000,
      };
      expect(await c.send<Object?>('echo', body: body), body);
      // The hello headers become the state, names lowercased
      expect(await c.send<String>('name'), 'dart');
      // The server calls the client back
      expect(await c.send<String>('mirror'), 'client world');
      expect(await c.values<int>('count').toList(), [0, 1, 2]);

      final got = <Object?>[];
      final subscription = await c.subscribe<Object?>('ticks', body: 'a', onEvent: got.add);
      fixture.ticks.send('a', 'tick');
      fixture.ticks.send('b', 'not for us');
      fixture.ticks.send('a', 'tick');
      await waitFor(() => got.length == 2);
      subscription.cancel();
      await Future<void>.delayed(const Duration(milliseconds: 20));
      fixture.ticks.send('a', 'after cancel');
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(got, ['tick', 'tick']);

      final many = [for (var i = 0; i < 2000; i += 1) c.send<int>('echo', body: i)];
      expect(await Future.wait(many).timeout(const Duration(seconds: 10)), List.generate(2000, (i) => i));
      expect(fixture.server.connectionCount, 1);
    });

    test('certificate trust: CA, server name, pin; refused otherwise', () async {
      final pinned = connect(fixture.address, QuicOptions(fingerprint: fingerprint.replaceAll(':', '').toLowerCase()));
      expect(await pinned.send<String>('hello').timeout(const Duration(seconds: 5)), 'world');
      final byName = connect(fixture.address, QuicOptions(ca: cert, servername: 'localhost'));
      expect(await byName.send<String>('hello').timeout(const Duration(seconds: 5)), 'world');
      final viaLocalhost = connect('quic://localhost:${fixture.server.port}', QuicOptions(ca: cert));
      expect(await viaLocalhost.send<String>('hello').timeout(const Duration(seconds: 5)), 'world');

      final refused = {
        'system roots': const QuicOptions(),
        'wrong pin': QuicOptions(fingerprint: '00' * 32),
        'name not in cert': QuicOptions(ca: cert, servername: 'example.com'),
      };
      final attempts = {for (final entry in refused.entries) entry.key: connect(fixture.address, entry.value)};
      await Future<void>.delayed(const Duration(milliseconds: 400));
      for (final entry in attempts.entries) {
        expect(entry.value.ws.isConnected, isFalse, reason: entry.key);
      }
      expect(fixture.connected, 3);
    });

    test('reconnect: unanswered requests are sent again, requests made meanwhile go out once back', () async {
      final port = fixture.server.port;
      final c = connect(fixture.address, QuicOptions(ca: cert));
      expect(await c.send<String>('hello').timeout(const Duration(seconds: 5)), 'world');

      // The server drops the connection without answering; like this port's WebSocket client, the request is
      // sent again on the next connection
      expect(await c.send<String>('disconnect').timeout(const Duration(seconds: 5)), 'dropped before');
      expect(fixture.connected, 2);

      // The server goes away: a request made meanwhile goes to the next server on the port
      await fixture.server.stop();
      await waitFor(() => !c.ws.isConnected);
      final queued = c.send<String>('echo', body: 'queued');
      await Future<void>.delayed(const Duration(milliseconds: 200));
      await fixture.listen('127.0.0.1:$port');
      expect(await queued.timeout(const Duration(seconds: 10)), 'queued');
      expect(c.ws.isConnected, isTrue);

      c.stop();
      await expectLater(c.send<String>('hello').timeout(const Duration(seconds: 2)), throwsA(anything));
    });

    test('the server closes a connection on stop', () async {
      final c = connect(fixture.address, QuicOptions(ca: cert));
      expect(await c.send<String>('hello').timeout(const Duration(seconds: 5)), 'world');
      final slow = c.send<String>('slow');
      await Future<void>.delayed(const Duration(milliseconds: 50));
      await fixture.server.stop();
      await waitFor(() => !c.ws.isConnected);
      expect([fixture.connected, fixture.disconnected], [1, 1]);
      expect(fixture.server.connectionCount, 0);
      c.stop();
      await expectLater(slow.timeout(const Duration(seconds: 2)), throwsA(anything));
    });

    test('a frame limit closes the connection', () async {
      final small = await Channel<Object?>()
          .post('echo', (body) => body.body)
          .listenQuic(0, options: QuicListenOptions<Object?>(cert: cert!, key: key!, maxMessageSize: 100));
      try {
        final raw = await RawQuicConnection.connect('quic://127.0.0.1:${small.port}', QuicOptions(ca: cert));
        final frames = StreamIterator(raw.messages);
        raw.write(encodeQuicHello({}));
        raw.write(encodeQuicFrame(utf8.encode(jsonEncode({'id': 1, 'path': 'echo', 'body': 'small'}))));
        expect(await frames.moveNext().timeout(const Duration(seconds: 5)), isTrue);
        expect((jsonDecode(frames.current as String) as Map)['body'], 'small');
        raw.write(encodeQuicFrame(utf8.encode(jsonEncode({'id': 2, 'path': 'echo', 'body': 'x' * 200}))));
        expect(await frames.moveNext().timeout(const Duration(seconds: 5)), isFalse);
        raw.close();
      } finally {
        await small.stop();
      }
    });
  });

  group('wire rules, with raw frames', skip: unavailable, () {
    late Fixture fixture;

    setUp(() async {
      fixture = Fixture(cert!, key!);
      await fixture.listen();
    });

    tearDown(() => fixture.stop());

    Future<RawQuicConnection> raw() => RawQuicConnection.connect(fixture.address, QuicOptions(ca: cert));

    test('a hello, a keepalive and a batch', () async {
      final connection = await raw();
      final frames = StreamIterator(connection.messages);
      connection.write([
        ...encodeQuicHello({'X-Name': 'raw'}),
        0,
        ...encodeQuicFrame(utf8.encode('[{"id":1,"path":"name"},{"id":2,"path":"hello"}]')),
      ]);
      final got = <int, Object?>{};
      while (got.length < 2) {
        expect(await frames.moveNext().timeout(const Duration(seconds: 5)), isTrue);
        final message = jsonDecode(frames.current as String);
        for (final item in message is List ? message : [message]) {
          got[item['id'] as int] = item['body'];
        }
      }
      expect(got, {1: 'raw', 2: 'world'});
      connection.close();
    });

    test('one stream per connection: later streams are refused, the first keeps working', () async {
      final connection = await raw();
      final frames = StreamIterator(connection.messages);
      connection.write(encodeQuicHello({}));
      // The server has its stream before the next ones arrive
      await waitFor(() => fixture.connected == 1);
      for (final (index, unidirectional) in [(0, false), (1, true)]) {
        connection.writeOnNewStream(unidirectional ? [5, ...utf8.encode('hello')] : encodeQuicHello({}), unidirectional: unidirectional);
        await waitFor(() => connection.extraStreamRefused(index), const Duration(seconds: 3));
      }
      connection.write(encodeQuicFrame(utf8.encode('{"id":7,"path":"hello"}')));
      expect(await frames.moveNext().timeout(const Duration(seconds: 5)), isTrue);
      expect((jsonDecode(frames.current as String) as Map)['body'], 'world');
      expect(fixture.connected, 1);
      connection.close();
    });

    final closing = {
      "a first frame that isn't a hello": encodeQuicFrame(utf8.encode('{"id":1,"path":"hello"}')),
      "a hello that isn't an object": encodeQuicFrame(utf8.encode('{"hello":"text"}')),
      'invalid JSON': [...encodeQuicHello({}), ...encodeQuicFrame(utf8.encode('{nope'))],
      'a frame over the limit': [...encodeQuicHello({}), 0x81, 0, 0, 1],
    };
    for (final entry in closing.entries) {
      test('${entry.key} closes the connection', () async {
        final connection = await raw();
        final closed = connection.messages.toList();
        connection.write(entry.value);
        await closed.timeout(const Duration(seconds: 5));
        connection.close();
      });
    }
  });

  group('interop', skip: unavailable, () {
    // Its own fixture, so interop_test.dart's one on 2049 can come and go meanwhile
    final port = int.parse(Platform.environment['CHANNEL_QUIC_TEST_PORT'] ?? '2449');
    final tsServer = TsServerFixture(port: port);

    setUpAll(() => tsServer.start());

    tearDownAll(() => tsServer.stop());

    test('Dart client against the TypeScript fixture', () async {
      final names = Subscription();
      final client = (Channel<Map<String, Object?>>()
            ..post('hello', (_) => 'client world')
            ..stream('stream/values', (_) async* {
              for (var i = 0; i < 3; i += 1) {
                yield i;
              }
            })
            ..events({'names': names}))
          .connectQuic(
            'quic://127.0.0.1:$port',
            quic: QuicOptions(ca: cert),
            options: ClientOptions<Map<String, Object?>>(state: {}, headers: () => {'X-Port': 'dart'}),
          );
      try {
        expect(await client.send<String>('hello').timeout(const Duration(seconds: 5)), 'world');
        expect(await client.send<Object?>('echo', body: {'a': [1, 'b']}), {'a': [1, 'b']});
        expect(await client.send<String>('mirror'), 'client world');
        expect(await client.send<String>('auth', body: {'name': 'quic'}), 'quic');
        expect(await client.send<String>('auth/name'), 'quic');
        // The hello headers, lowercased
        expect((await client.send<Map<String, Object?>>('v2/headers'))['x-port'], 'dart');
        await expectLater(client.send<Object?>('v2/error/code'), throwsA(anything));
        // The fixture calls this client's stream and subscribes to its events
        expect(await client.values<int>('mirror/stream').toList(), [0, 1, 2]);
        await client.send<Object?>('mirror/events');
        await Future<void>.delayed(const Duration(milliseconds: 20));
        names.send('1', 'h');
      } finally {
        client.stop();
      }

      final pinned = Channel<Object?>().connectQuic('quic://localhost:$port', quic: const QuicOptions(fingerprint: fingerprint));
      try {
        expect(await pinned.send<String>('hello').timeout(const Duration(seconds: 5)), 'world');
      } finally {
        pinned.stop();
      }
    });

    test("the TypeScript reference's client against the Dart server", () async {
      if (!File('../channel/quic.ts').existsSync()) {
        markTestSkipped("the TypeScript reference isn't next to this checkout");
        return;
      }
      final fixture = Fixture(cert!, key!);
      await fixture.listen();
      try {
        final result = await Process.run('bun', [
          'test/quic/client.ts',
          '${fixture.server.port}',
          '${certDir.path}cert.pem',
        ]).timeout(const Duration(seconds: 30));
        expect(result.exitCode, 0, reason: '${result.stderr}');
        expect(jsonDecode(result.stdout as String), {
          'hello': 'world',
          'echo': {
            'a': [1, 'b'],
          },
          'name': 'ts',
          'mirror': 'ts world',
          'values': [0, 1, 2],
          'event': 'tick',
        });
        await waitFor(() => fixture.connected >= 1 && fixture.disconnected == fixture.connected);
      } finally {
        await fixture.stop();
      }
    });
  });
}
