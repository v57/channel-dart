// A response answers a request only on the connection the request went to (security audit C1).
// Mirrors channel/tests/reply-origin.test.ts.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:channel_dart/client.dart';
import 'package:channel_dart/server.dart';
import 'package:test/test.dart';

typedef _State = Map<String, Object?>;

/// A server that forwards calls to a service it holds the sender of, like the Hub does. Request ids are one
/// counter for every connection, so another peer must not be able to answer what the server asked the service.
class _Relay {
  _Relay(this.server, this.serviceChannel, this.answered);

  final ChannelServer<_State> server;
  final Channel<_State> serviceChannel;
  final Completer<void> answered;

  int get port => server.httpServer.port;

  static Future<_Relay> start() async {
    Sender<_State>? service;
    final answered = Completer<void>();
    final serverChannel = Channel<_State>()
      ..post('register', (ChannelBody<_State> body) {
        service = body.sender;
        return true;
      })
      ..post(
        'call',
        (ChannelBody<_State> body) =>
            service!.send<Object?>('svc/read', body: body.body),
      )
      ..stream(
        'watch',
        (ChannelBody<_State> _) => service!.values<Object?>('svc/values'),
      );
    final server = await serverChannel.listen(0);
    final serviceChannel = Channel<_State>()
      ..post('svc/read', (ChannelBody<_State> _) async {
        await answered.future;
        return 'real';
      })
      ..stream('svc/values', (ChannelBody<_State> _) async* {
        await answered.future;
        yield 'real';
      });
    return _Relay(server, serviceChannel, answered);
  }
}

/// Every id the server could have used, answered from a connection the server never asked: one frame per forger.
Future<WebSocket> _spray(
  int port,
  List<Map<String, Object?> Function(int id)> forgers,
) async {
  final attacker = await WebSocket.connect('ws://127.0.0.1:$port');
  for (final forge in forgers) {
    attacker.add(jsonEncode(<Object?>[for (var id = 0; id < 256; id += 1) forge(id)]));
  }
  return attacker;
}

void main() {
  test('a response from another connection answers no forwarded call', () async {
    final relay = await _Relay.start();
    final service = relay.serviceChannel.connect(relay.port);
    await service.send<Object?>('register');
    final victim = Channel<_State>().connect(relay.port);
    final call = victim.send<Object?>('call', body: 'x');
    await Future<void>.delayed(const Duration(milliseconds: 50));
    final attacker = await _spray(
      relay.port,
      <Map<String, Object?> Function(int)>[
        (int id) => <String, Object?>{'id': id, 'body': 'forged'},
        (int id) => <String, Object?>{'id': id, 'topic': 'x', 'body': 'forged'},
      ],
    );
    await Future<void>.delayed(const Duration(milliseconds: 50));
    relay.answered.complete();
    try {
      expect(await call.timeout(const Duration(seconds: 5)), 'real');
    } finally {
      await attacker.close();
      victim.stop();
      service.stop();
      await relay.server.stop();
    }
  });

  test('another connection can neither feed nor end a forwarded stream', () async {
    final relay = await _Relay.start();
    final service = relay.serviceChannel.connect(relay.port);
    await service.send<Object?>('register');
    final victim = Channel<_State>().connect(relay.port);
    final reading = victim.values<Object?>('watch').toList();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    final attacker = await _spray(
      relay.port,
      <Map<String, Object?> Function(int)>[
        (int id) => <String, Object?>{'id': id, 'body': 'forged'},
        (int id) => <String, Object?>{'id': id, 'done': true},
      ],
    );
    await Future<void>.delayed(const Duration(milliseconds: 50));
    relay.answered.complete();
    try {
      expect(
        await reading.timeout(const Duration(seconds: 5)),
        <Object?>['real'],
      );
    } finally {
      await attacker.close();
      victim.stop();
      service.stop();
      await relay.server.stop();
    }
  });

  test('the connection a request went to still answers it', () async {
    final server = await (Channel<_State>()
          ..post(
            'mirror',
            (ChannelBody<_State> body) => body.sender.send<Object?>('hello'),
          ))
        .listen(0);
    final client = (Channel<_State>()
          ..post('hello', (ChannelBody<_State> _) => 'client'))
        .connect(server.httpServer.port);
    try {
      expect(await client.send<Object?>('mirror'), 'client');
    } finally {
      client.stop();
      await server.stop();
    }
  });
}
