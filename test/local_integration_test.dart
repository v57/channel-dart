import 'dart:async';

import 'package:channel_dart/client.dart';
import 'package:channel_dart/server.dart';
import 'package:test/test.dart';

Future<void> sleep([double seconds = 0.001]) {
  return Future<void>.delayed(
    Duration(microseconds: (seconds * 1000000).round()),
  );
}

void main() {
  group('dart server <-> dart client', () {
    late ChannelServer<Map<String, Object?>> server;
    late ClientSender<Map<String, Object?>> client;
    late Subscription updates;

    setUp(() async {
      updates = Subscription();
      final serverChannel = Channel<Map<String, Object?>>()
        ..post('hello', (_) => 'world')
        ..post(
          'whoami',
          (ChannelBody<Map<String, Object?>> body) => body.state['token'],
        )
        ..stream('stream/values', (_) async* {
          for (var i = 0; i < 3; i += 1) {
            yield i;
            await sleep();
          }
        })
        ..events(<String, Object?>{'updates': updates});

      server = await serverChannel.listen(
        0,
        options: ListenOptions<Map<String, Object?>>(
          state: (Map<String, String> headers) {
            return <String, Object?>{'token': headers['token'] ?? 'none'};
          },
        ),
      );

      client = Channel<Map<String, Object?>>().connect(
        server.httpServer.port,
        options: ClientOptions<Map<String, Object?>>(
          state: <String, Object?>{},
          headers: () => <String, String>{'token': 'abc-123'},
        ),
      );
    });

    tearDown(() async {
      client.stop();
      await server.stop();
    });

    test('request response', () async {
      final response = await client.send<String>('hello');
      expect(response, 'world');
    });

    test('connection state from headers', () async {
      final token = await client.send<String>('whoami');
      expect(token, 'abc-123');
    });

    test('stream', () async {
      final values = await client.values<int>('stream/values').toList();
      expect(values, <int>[0, 1, 2]);
    });

    test('subscription drops falsy body and supports cancel', () async {
      final firstEvent = Completer<Object?>();
      var receivedAfterCancel = false;

      final subscription = await client.subscribe<Object?>(
        'updates',
        body: 'u1',
        onEvent: (Object? event) {
          if (!firstEvent.isCompleted) {
            firstEvent.complete(event);
            return;
          }
          receivedAfterCancel = true;
        },
      );

      updates.send('u1', 0); // falsy: dropped like TS `if (!body) return`
      updates.send('u1', 7);
      expect(await firstEvent.future.timeout(const Duration(seconds: 2)), 7);

      subscription.cancel();
      await sleep(0.01);
      updates.send('u1', 1);
      await sleep(0.01);

      expect(receivedAfterCancel, isFalse);
    });
  });
}
