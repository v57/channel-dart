import 'dart:async';

import 'package:channel_dart/client.dart';
import 'package:channel_dart/channel.dart';
import 'package:test/test.dart';

import 'support/ts_server_fixture.dart';

Future<void> sleep([double seconds = 0.001]) {
  return Future<void>.delayed(
    Duration(microseconds: (seconds * 1000000).round()),
  );
}

void main() {
  final fixture = TsServerFixture();

  final clientEvents = <String, Subscription>{'names': Subscription()};

  var valuesSent = 0;

  final channel = Channel<Map<String, Object?>>()
    ..post('hello', (_) => 'client world')
    ..stream('stream/values', (_) async* {
      for (var i = 0; i < 3; i += 1) {
        yield i;
        await sleep();
      }
    })
    ..stream('stream/cancel', (_) async* {
      for (var i = 0; i < 10; i += 1) {
        yield i;
        valuesSent += 1;
        await sleep();
      }
    })
    ..events(clientEvents);

  late ClientSender<Map<String, Object?>> client;

  setUpAll(() async {
    await fixture.start();
    client = channel.connect(
      2049,
      options: ClientOptions<Map<String, Object?>>(state: <String, Object?>{}),
    );
  });

  tearDownAll(() async {
    client.stop();
    await fixture.stop();
  });

  test('post/hello', () async {
    final response = await client.send<String>('hello');
    expect(response, 'world');
  });

  test('post/echo', () async {
    final random = DateTime.now().microsecondsSinceEpoch / 1000000.0;
    final response = await client.send<num>('echo', body: random);
    expect(response, random);
  });

  test('post/empty', () async {
    final response = await client.send<Object?>('empty');
    expect(response, isNull);
  });

  test('post/notfound', () async {
    await expectLater(
      () => client.send<Object?>('notfound'),
      throwsA(equals('api not found')),
    );
  });

  test('events/notfound', () async {
    await expectLater(
      () => client.subscribe<Object?>('notfound', onEvent: (Object? _) {}),
      throwsA(equals('subscription not found')),
    );
  });

  test('stream/values', () async {
    final values = await client.values<int>('stream/values').toList();
    expect(values, <int>[0, 1, 2]);
  });

  test('stream/cancel', () async {
    final seen = <int>[];
    await for (final value in client.values<int>('stream/cancel')) {
      seen.add(value);
      if (value == 2) {
        break;
      }
    }
    expect(seen, <int>[0, 1, 2]);
  });

  test('server/post mirror', () async {
    final response = await client.send<String>('mirror');
    expect(response, 'client world');
  });

  test('server/subscribe mirror/events', () async {
    await client.send<Object?>('mirror/events');
    await sleep(0.005);

    clientEvents['names']!.send('2', 'po');
    clientEvents['names']!.send('1', 'h');
    await sleep(0.01);

    // Server unsubscribes after first matching event.
    clientEvents['names']!.send('1', 'we');
    await sleep(0.01);
  });

  test('server/stream mirror/stream', () async {
    final values = await client.values<int>('mirror/stream').toList();
    expect(values, <int>[0, 1, 2]);
  });

  test('server/stream mirror/stream/cancel', () async {
    valuesSent = 0;

    final seen = <int>[];
    await for (final value in client.values<int>('mirror/stream/cancel')) {
      seen.add(value);
      if (value == 2) {
        break;
      }
    }

    expect(seen, <int>[0, 1, 2]);
    expect(valuesSent, lessThanOrEqualTo(4));
  });

  test('state/auth', () async {
    final another = Channel<Map<String, Object?>>().connect(
      2049,
      options: ClientOptions<Map<String, Object?>>(state: <String, Object?>{}),
    );

    await another.send<Object?>(
      'auth',
      body: <String, Object?>{'name': 'tester'},
    );
    final name = await another.send<String>('auth/name');
    expect(name, 'tester');
    another.stop();
  });

  test('state/unauthorized', () async {
    final another = Channel<Map<String, Object?>>().connect(
      2049,
      options: ClientOptions<Map<String, Object?>>(state: <String, Object?>{}),
    );

    await expectLater(
      () => another.send<Object?>('auth/name'),
      throwsA(equals('unauthorized')),
    );
    another.stop();
  });
}
