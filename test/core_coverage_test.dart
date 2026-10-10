import 'dart:async';

import 'package:channel_dart/process.dart';
import 'package:channel_dart/server.dart';
import 'package:test/test.dart';

Future<void> tick() => Future<void>.delayed(Duration.zero);

class _StubSender<State> implements Sender<State> {
  @override
  final Map<int, StreamCanceller> streams = <int, StreamCanceller>{};

  @override
  final Map<int, CancellableRequest> requests = <int, CancellableRequest>{};

  @override
  Future<T> send<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    return Future<T>.value(null as T);
  }

  @override
  SendingRequest<T> request<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    throw StateError('unused');
  }

  @override
  Stream<T> values<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    throw StateError('unused');
  }

  @override
  Future<Cancellable> subscribe<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
    required void Function(T event) onEvent,
  }) {
    return Future<Cancellable>.value(_NoopCancel());
  }

  @override
  void stop() {}
}

class _NoopCancel implements Cancellable {
  @override
  void cancel() {}
}

class _ControllerPack<State> {
  _ControllerPack(this.controller);

  final ChannelController<State> controller;
  final List<Map<String, Object?>> responses = <Map<String, Object?>>[];
  final List<String> subscriptions = <String>[];
  final List<String> unsubscriptions = <String>[];
  final List<Map<String, Object?>> events = <Map<String, Object?>>[];
}

_ControllerPack<State> createController<State>(
  Sender<State> sender,
  State state,
) {
  late _ControllerPack<State> pack;
  final controller = ChannelController<State>(
    response: (Map<String, Object?> response) {
      pack.responses.add(response);
    },
    subscribe: (String topic) {
      pack.subscriptions.add(topic);
    },
    unsubscribe: (String topic) {
      pack.unsubscriptions.add(topic);
    },
    event: (String topic, Object? body) {
      pack.events.add(<String, Object?>{'topic': topic, 'body': body});
    },
    sender: sender,
    state: state,
  );

  pack = _ControllerPack<State>(controller);
  return pack;
}

int firstRequestId(Channel<Object?> channel) {
  return channel.requests.keys.first;
}

class _ConnectionConfig {
  const _ConnectionConfig({
    this.cancelResult = false,
    this.sendReturnsNull = false,
  });

  final bool cancelResult;
  final bool sendReturnsNull;
}

class _ConnectionPack {
  _ConnectionPack(this.connection);

  final ConnectionInterface connection;
  final List<int> sentIds = <int>[];
  final List<int> cancelIds = <int>[];
  final List<Object?> notifyBodies = <Object?>[];
}

_ConnectionPack createConnection([
  _ConnectionConfig config = const _ConnectionConfig(),
]) {
  var nextId = 200;
  late _ConnectionPack pack;

  final connection = _TestConnection(
    send: (Object? _) {
      if (config.sendReturnsNull) {
        return null;
      }
      return nextId++;
    },
    sent: (int id) {
      pack.sentIds.add(id);
    },
    cancel: (int id) {
      pack.cancelIds.add(id);
      return config.cancelResult;
    },
    notify: (Object? body) {
      pack.notifyBodies.add(body);
    },
    addTopic: (String _, EventBody _) {
      return () => true;
    },
    stop: () {},
  );

  pack = _ConnectionPack(connection);
  return pack;
}

class _TestConnection implements ConnectionInterface {
  _TestConnection({
    required int? Function(Object? body) send,
    required void Function(int id) sent,
    required bool Function(int id) cancel,
    required void Function(Object? body) notify,
    required bool Function() Function(String topic, EventBody event) addTopic,
    required void Function() stop,
  }) : _send = send,
       _sent = sent,
       _cancel = cancel,
       _notify = notify,
       _addTopic = addTopic,
       _stop = stop;

  final int? Function(Object? body) _send;
  final void Function(int id) _sent;
  final bool Function(int id) _cancel;
  final void Function(Object? body) _notify;
  final bool Function() Function(String topic, EventBody event) _addTopic;
  final void Function() _stop;

  @override
  int? send(Object? body) => _send(body);

  @override
  void sent(int id) => _sent(id);

  @override
  bool cancel(int id) => _cancel(id);

  @override
  void notify(Object? body) => _notify(body);

  @override
  bool Function() addTopic(String topic, EventBody event) =>
      _addTopic(topic, event);

  @override
  void stop() => _stop();
}

void main() {
  test('Channel.postOther and streamOther route dynamic paths', () async {
    final channel = Channel<Map<String, Object?>>()
      ..postOther(
        (String path) => path.startsWith('dynamic/'),
        (ChannelBody<Map<String, Object?>> body) => '${body.path}:${body.body}',
      )
      ..streamOther((String path) => path.endsWith('/stream'), (
        ChannelBody<Map<String, Object?>> body,
      ) async* {
        yield body.body;
      });

    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 1,
      'path': 'dynamic/echo',
      'body': 'ok',
    }, pack.controller);
    channel.receiveOne(<String, Object?>{
      'id': 2,
      'stream': 'dynamic/stream',
      'body': 7,
    }, pack.controller);
    await tick();

    expect(
      pack.responses,
      contains(
        predicate<Map<String, Object?>>(
          (Map<String, Object?> response) =>
              response['id'] == 1 && response['body'] == 'dynamic/echo:ok',
        ),
      ),
    );
    expect(
      pack.responses,
      contains(
        predicate<Map<String, Object?>>(
          (Map<String, Object?> response) =>
              response['id'] == 2 && response['body'] == 7,
        ),
      ),
    );
    expect(
      pack.responses,
      contains(
        predicate<Map<String, Object?>>(
          (Map<String, Object?> response) =>
              response['id'] == 2 && response['done'] == true,
        ),
      ),
    );
  });

  test('Channel.disconnect runs cleanup callbacks and clears pending maps', () {
    var disconnected = 0;
    final channel = Channel<Map<String, Object?>>()
      ..onDisconnect((Map<String, Object?> _, Sender<Map<String, Object?>> _) {
        disconnected += 1;
        throw StateError('ignored');
      });

    final sender = _StubSender<Map<String, Object?>>();

    var closedStreams = 0;
    sender.streams[1] = StreamCanceller(() async {
      closedStreams += 1;
    });

    var cancelActions = 0;
    final request = BasicCancellableRequest(onCancelled: () {});
    request.onCancel(() {
      cancelActions += 1;
    });
    request.onCancel(() {
      cancelActions += 1;
      throw StateError('ignored');
    });
    sender.requests[10] = request;

    channel.makeRequest(
      'pending',
      response: (Map<String, Object?> _) {},
      sender: sender,
    );

    channel.disconnect(<String, Object?>{}, sender);

    expect(disconnected, 1);
    expect(closedStreams, 1);
    expect(cancelActions, 2);
    expect(sender.streams, isEmpty);
    expect(sender.requests, isEmpty);
    expect(channel.requests, isEmpty);
  });

  test('Channel.cancel removes request by id', () {
    final channel = Channel<Object?>();
    final request = channel.makeRequest(
      'hello',
      response: (Map<String, Object?> _) {},
    );

    expect(channel.requests.length, 1);
    channel.cancel(request.id);
    expect(channel.requests, isEmpty);
  });

  test('Channel.receiveOne handles async post rejection', () async {
    final channel = Channel<Map<String, Object?>>()
      ..post('fails', (_) async {
        throw 'boom';
      });

    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 1,
      'path': 'fails',
    }, pack.controller);
    await tick();

    expect(
      pack.responses,
      contains(
        predicate<Map<String, Object?>>(
          (Map<String, Object?> response) =>
              response['id'] == 1 && response['error'] == 'boom',
        ),
      ),
    );
  });

  test('Channel.receiveOne cancel runs every task onCancel action', () async {
    final onCancelCalls = <int>[];
    final channel = Channel<Map<String, Object?>>()
      ..post('wait', (ChannelBody<Map<String, Object?>> body) {
        body.task?.onCancel(() => onCancelCalls.add(1));
        body.task?.onCancel(() => onCancelCalls.add(2));
        return Completer<Object?>().future;
      });

    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 5,
      'path': 'wait',
    }, pack.controller);
    channel.receiveOne(<String, Object?>{'cancel': 5}, pack.controller);
    await tick();

    expect(onCancelCalls, <int>[1, 2]);
    expect(
      pack.responses,
      contains(
        predicate<Map<String, Object?>>(
          (Map<String, Object?> response) =>
              response['id'] == 5 && response['error'] == 'cancelled',
        ),
      ),
    );
    expect(sender.requests, isEmpty);
  });

  test(
    'Channel.receiveOne subscribes after async subscription body resolves',
    () async {
      final news = Subscription()
        ..topic((Object? _) async => 'ready')
        ..body((Object? request) async {
          final input = request as Map<String, Object?>;
          return (input['value'] as String).toUpperCase();
        });

      final channel = Channel<Map<String, Object?>>()
        ..events(<String, Object?>{'news': news});

      final sender = _StubSender<Map<String, Object?>>();
      final pack = createController<Map<String, Object?>>(
        sender,
        <String, Object?>{},
      );

      channel.receiveOne(<String, Object?>{
        'id': 9,
        'sub': 'news',
        'body': <String, Object?>{'value': 'ok'},
      }, pack.controller);
      await tick();

      expect(pack.subscriptions, <String>['news/ready']);
      expect(
        pack.responses,
        contains(
          predicate<Map<String, Object?>>(
            (Map<String, Object?> response) =>
                response['id'] == 9 &&
                response['topic'] == 'news/ready' &&
                response['body'] == 'OK',
          ),
        ),
      );
    },
  );

  test('Subscription.topic and body are independent', () {
    final subscription = Subscription();
    final events = <SubscriptionEvent>[];

    subscription.publishers.add(
      _TestPublisher((SubscriptionEvent event) {
        events.add(event);
      }),
    );

    subscription
      ..topic((Object? body) => 'topic-$body')
      ..body((Object? body) => 'body-$body');

    subscription.send('x');
    subscription.send('y', 'payload');

    expect(events[0].topic, 'topic-x');
    expect(events[0].body, 'body-x');
    expect(events[1].topic, 'topic-y');
    expect(events[1].body, 'payload');
  });

  test('makeSender.request resolves and marks sent id', () async {
    final channel = Channel<Object?>();
    final connection = createConnection();
    final sender = makeSender<Object?>(channel, connection.connection);

    final request = sender.request<String>('hello');
    final requestId = firstRequestId(channel);

    final pack = createController<Object?>(_StubSender<Object?>(), null);
    channel.receiveOne(<String, Object?>{
      'id': requestId,
      'body': 'world',
    }, pack.controller);

    expect(await request.response, 'world');
    expect(connection.sentIds, <int>[200]);
  });

  test('makeSender.request rejects and marks sent id', () async {
    final channel = Channel<Object?>();
    final connection = createConnection();
    final sender = makeSender<Object?>(channel, connection.connection);

    final request = sender.request<String>('hello');
    final requestId = firstRequestId(channel);

    final pack = createController<Object?>(_StubSender<Object?>(), null);
    channel.receiveOne(<String, Object?>{
      'id': requestId,
      'error': 'failed',
    }, pack.controller);

    await expectLater(request.response, throwsA(equals('failed')));
    expect(connection.sentIds, <int>[200]);
  });

  test('makeSender.request cancel notifies when transport cancel fails', () {
    final channel = Channel<Object?>();
    final connection = createConnection(_ConnectionConfig(cancelResult: false));
    final sender = makeSender<Object?>(channel, connection.connection);

    final request = sender.request<Object?>('slow');
    final requestId = firstRequestId(channel);

    request.cancel();

    expect(connection.cancelIds, <int>[200]);
    expect(connection.notifyBodies, <Object?>[
      <String, Object?>{'cancel': requestId},
    ]);
    expect(channel.requests, isEmpty);
  });

  test(
    'makeSender.request cancel notifies by request id when send id missing',
    () {
      final channel = Channel<Object?>();
      final connection = createConnection(
        _ConnectionConfig(sendReturnsNull: true),
      );
      final sender = makeSender<Object?>(channel, connection.connection);

      final request = sender.request<Object?>('slow');
      final requestId = firstRequestId(channel);

      request.cancel();

      expect(connection.cancelIds, isEmpty);
      expect(connection.notifyBodies, <Object?>[
        <String, Object?>{'cancel': requestId},
      ]);
      expect(channel.requests, isEmpty);
    },
  );

  test('process stop clears pending requests', () async {
    final channel = Channel<Map<String, Object?>>()
      ..post('wait', (ChannelBody<Map<String, Object?>> body) {
        body.task?.onCancel(() {});
        return Completer<Object?>().future;
      });

    final sender = channel.process(state: <String, Object?>{});

    final errors = <Object?>[];
    for (var i = 0; i < 10; i += 1) {
      sender.request<Object?>('wait').response.catchError((Object e) {
        errors.add(e);
        return null;
      });
    }

    await tick();
    expect(sender.requests.length, greaterThan(0));
    expect(channel.requests.length, greaterThan(0));

    sender.stop();
    await tick();

    expect(sender.requests, isEmpty);
    expect(channel.requests, isEmpty);
    // Stopped calls settle instead of hanging forever.
    expect(errors, List<Object?>.filled(10, 'disconnected'));
  });

  test('listen stop cleans up subscription publishers', () async {
    final updates = Subscription();
    final channel = Channel<Map<String, Object?>>()
      ..events(<String, Object?>{'updates': updates});

    expect(updates.publishers.length, 0);

    final first = await channel.listen(0);
    expect(updates.publishers.length, 1);

    await first.stop();
    expect(updates.publishers.length, 0);

    final second = await channel.listen(0);
    expect(updates.publishers.length, 1);

    await second.stop();
    expect(updates.publishers.length, 0);
  });

  test(
    'values cancel clears pending stream request even without transport cancel support',
    () async {
      var nextId = 100;
      final transportSendBodies = <Object?>[];
      final transportCancelIds = <int>[];

      final channel = Channel<Object?>();
      final sender = makeSender<Object?>(
        channel,
        _TestConnection(
          send: (Object? body) {
            transportSendBodies.add(body);
            return nextId++;
          },
          sent: (int _) {},
          cancel: (int id) {
            transportCancelIds.add(id);
            return false;
          },
          notify: (Object? _) {},
          addTopic: (String _, EventBody _) =>
              () => true,
          stop: () {},
        ),
      );

      final subscription = sender
          .values<Object?>('stream/stuck')
          .listen((Object? _) {});
      await tick();

      expect(channel.requests.length, 1);

      await subscription.cancel();
      await tick();

      expect(transportCancelIds, <int>[100]);
      expect(
        transportSendBodies.whereType<Map<String, Object?>>().any((
          Map<String, Object?> body,
        ) {
          return body.containsKey('cancel');
        }),
        isTrue,
      );
      expect(channel.requests, isEmpty);
    },
  );

  test(
    'values cancel sends cancel by request id when transport send id is missing',
    () async {
      final transportSendBodies = <Object?>[];
      final channel = Channel<Object?>();
      final sender = makeSender<Object?>(
        channel,
        _TestConnection(
          send: (Object? body) {
            transportSendBodies.add(body);
            return null;
          },
          sent: (int _) {},
          cancel: (int _) => false,
          notify: (Object? _) {},
          addTopic: (String _, EventBody _) =>
              () => true,
          stop: () {},
        ),
      );

      final subscription = sender
          .values<Object?>('stream/missing')
          .listen((_) {});
      await tick();
      await subscription.cancel();
      await tick();

      expect(
        transportSendBodies.whereType<Map<String, Object?>>().any((
          Map<String, Object?> body,
        ) {
          return body.containsKey('cancel');
        }),
        isTrue,
      );
      expect(channel.requests, isEmpty);
    },
  );

  test('values emits error when remote response contains error', () async {
    final channel = Channel<Object?>();
    final connection = createConnection();
    final sender = makeSender<Object?>(channel, connection.connection);
    final errors = <Object?>[];

    sender
        .values<Object?>('stream/with-error')
        .listen(
          (_) {},
          onError: (Object error) {
            errors.add(error);
          },
        );

    final requestId = firstRequestId(channel);
    final pack = createController<Object?>(_StubSender<Object?>(), null);
    channel.receiveOne(<String, Object?>{
      'id': requestId,
      'error': 'stream failed',
    }, pack.controller);
    await tick();

    expect(errors, <Object?>['stream failed']);
  });

  test('Subscription.send with explicit null body falls back to resolver', () {
    final events = <SubscriptionEvent>[];
    final subscription = Subscription()
      ..topic((Object? _) => 't')
      ..body((Object? request) => 'fallback:$request')
      ..publishers.add(
        _TestPublisher((SubscriptionEvent event) {
          events.add(event);
        }),
      );

    subscription.send('x', null);

    expect(events, hasLength(1));
    expect(events.single.body, 'fallback:x');
  });

  test('Channel default publish callback is no-op', () {
    final channel = Channel<Object?>();
    expect(() => channel.publish('updates', 1), returnsNormally);
  });

  test('Channel.receiveOne handles synchronous post throw', () {
    final channel = Channel<Map<String, Object?>>()
      ..post('boom', (ChannelBody<Map<String, Object?>> _) {
        throw StateError('sync boom');
      });
    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 11,
      'path': 'boom',
    }, pack.controller);

    expect(
      pack.responses.any((Map<String, Object?> response) {
        final error = '${response['error']}';
        return response['id'] == 11 && error.contains('sync boom');
      }),
      isTrue,
    );
    expect(sender.requests, isEmpty);
  });

  test('Channel.receiveOne stream request reports missing api with id', () {
    final channel = Channel<Map<String, Object?>>();
    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 7,
      'stream': 'missing',
    }, pack.controller);

    expect(
      pack.responses.any((Map<String, Object?> response) {
        return response['id'] == 7 && response['error'] == 'api not found';
      }),
      isTrue,
    );
  });

  test(
    'Channel.receiveOne subscription async resolver failure returns error',
    () async {
      final updates = Subscription()
        ..topic((Object? _) async => throw StateError('async topic failure'))
        ..body((Object? _) async => 1);
      final channel = Channel<Map<String, Object?>>()
        ..events(<String, Object?>{'updates': updates});
      final sender = _StubSender<Map<String, Object?>>();
      final pack = createController<Map<String, Object?>>(
        sender,
        <String, Object?>{},
      );

      channel.receiveOne(<String, Object?>{
        'id': 3,
        'sub': 'updates',
      }, pack.controller);
      await tick();
      await tick();

      expect(
        pack.responses.any((Map<String, Object?> response) {
          final error = '${response['error']}';
          return response['id'] == 3 && error.contains('async topic failure');
        }),
        isTrue,
      );
    },
  );

  test(
    'Channel.receiveOne subscription sync resolver failure returns error',
    () {
      final updates = Subscription()
        ..topic((Object? _) => throw StateError('sync topic failure'))
        ..body((Object? _) => 1);
      final channel = Channel<Map<String, Object?>>()
        ..events(<String, Object?>{'updates': updates});
      final sender = _StubSender<Map<String, Object?>>();
      final pack = createController<Map<String, Object?>>(
        sender,
        <String, Object?>{},
      );

      channel.receiveOne(<String, Object?>{
        'id': 4,
        'sub': 'updates',
      }, pack.controller);

      expect(
        pack.responses.any((Map<String, Object?> response) {
          final error = '${response['error']}';
          return response['id'] == 4 && error.contains('sync topic failure');
        }),
        isTrue,
      );
    },
  );

  test('Channel._streamRequest reports stream event error', () async {
    final channel = Channel<Map<String, Object?>>()
      ..stream('fails', (ChannelBody<Map<String, Object?>> _) async* {
        throw 'stream event failure';
      });
    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 21,
      'stream': 'fails',
    }, pack.controller);
    await tick();
    await tick();

    expect(
      pack.responses.any((Map<String, Object?> response) {
        return response['id'] == 21 &&
            response['error'] == 'stream event failure';
      }),
      isTrue,
    );
  });

  test('Channel._streamRequest catches stream handler throw', () async {
    final channel = Channel<Map<String, Object?>>()
      ..stream('throws', (ChannelBody<Map<String, Object?>> _) {
        throw StateError('stream factory failure');
      });
    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 22,
      'stream': 'throws',
    }, pack.controller);
    await tick();

    expect(
      pack.responses.any((Map<String, Object?> response) {
        final error = '${response['error']}';
        return response['id'] == 22 && error.contains('stream factory failure');
      }),
      isTrue,
    );
  });

  test(
    'Channel.merge combines events, posts and streams with prefix',
    () async {
      final updates = Subscription()
        ..topic((Object? request) => '$request')
        ..body((Object? _) => 'ok');
      final source = Channel<Map<String, Object?>>()
        ..post('hello', (_) => 'world')
        ..stream('numbers', (_) async* {
          yield 9;
        })
        ..events(<String, Object?>{'updates': updates});
      final merged = Channel<Map<String, Object?>>()
        ..merge(source, prefix: 'v1');
      final sender = _StubSender<Map<String, Object?>>();
      final pack = createController<Map<String, Object?>>(
        sender,
        <String, Object?>{},
      );

      merged.receiveOne(<String, Object?>{
        'id': 1,
        'path': 'v1/hello',
      }, pack.controller);
      merged.receiveOne(<String, Object?>{
        'id': 2,
        'stream': 'v1/numbers',
      }, pack.controller);
      merged.receiveOne(<String, Object?>{
        'id': 3,
        'sub': 'v1/updates',
        'body': 'ready',
      }, pack.controller);
      await tick();

      expect(
        pack.responses.any((Map<String, Object?> response) {
          return response['id'] == 1 && response['body'] == 'world';
        }),
        isTrue,
      );
      expect(
        pack.responses.any((Map<String, Object?> response) {
          return response['id'] == 2 && response['body'] == 9;
        }),
        isTrue,
      );
      expect(
        pack.responses.any((Map<String, Object?> response) {
          return response['id'] == 2 && response['done'] == true;
        }),
        isTrue,
      );
      expect(
        pack.responses.any((Map<String, Object?> response) {
          return response['id'] == 3 &&
              response['topic'] == 'v1/updates/ready' &&
              response['body'] == 'ok';
        }),
        isTrue,
      );
      expect(pack.subscriptions, contains('v1/updates/ready'));
    },
  );

  test('Channel.api parses raw handlers and dynamic nested map keys', () async {
    final ChannelPostHandler<Map<String, Object?>> directPost =
        (ChannelBody<Map<String, Object?>> body) => 'p:${body.path}';
    final ChannelStreamHandler<Map<String, Object?>> directStream =
        (ChannelBody<Map<String, Object?>> body) async* {
          yield 's:${body.path}';
        };
    final ChannelPostHandler<Map<String, Object?>> nestedPost =
        (ChannelBody<Map<String, Object?>> body) => 'n:${body.path}';
    final ChannelStreamHandler<Map<String, Object?>> nestedStream =
        (ChannelBody<Map<String, Object?>> body) async* {
          yield 'm:${body.path}';
        };

    final channel = Channel<Map<String, Object?>>()
      ..api(<String, Object?>{
        'post': directPost,
        'stream': directStream,
        'nested': <Object, Object>{'inner': nestedPost, 7: nestedStream},
      });
    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 31,
      'path': 'post',
    }, pack.controller);
    channel.receiveOne(<String, Object?>{
      'id': 32,
      'stream': 'stream',
    }, pack.controller);
    channel.receiveOne(<String, Object?>{
      'id': 33,
      'path': 'nested/inner',
    }, pack.controller);
    channel.receiveOne(<String, Object?>{
      'id': 34,
      'stream': 'nested/7',
    }, pack.controller);
    await tick();
    await tick();

    expect(
      pack.responses.any((Map<String, Object?> response) {
        return response['id'] == 31 && response['body'] == 'p:post';
      }),
      isTrue,
    );
    expect(
      pack.responses.any((Map<String, Object?> response) {
        return response['id'] == 32 && response['body'] == 's:stream';
      }),
      isTrue,
    );
    expect(
      pack.responses.any((Map<String, Object?> response) {
        return response['id'] == 33 && response['body'] == 'n:nested/inner';
      }),
      isTrue,
    );
    expect(
      pack.responses.any((Map<String, Object?> response) {
        return response['id'] == 34 && response['body'] == 'm:nested/7';
      }),
      isTrue,
    );
  });

  test('Channel.receiveOne accepts Map<String, dynamic> and num id', () {
    final channel = Channel<Map<String, Object?>>()
      ..post('num-id', (_) => 'ok');
    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, dynamic>{
      'id': 8.8,
      'path': 'num-id',
    }, pack.controller);

    expect(
      pack.responses.any((Map<String, Object?> response) {
        return response['id'] == 8 && response['body'] == 'ok';
      }),
      isTrue,
    );
  });

  test('Channel.events parses nested dynamic maps', () {
    final nested = Subscription()
      ..topic((Object? request) => '$request')
      ..body((Object? _) => 5);
    final channel = Channel<Map<String, Object?>>()
      ..events(<Object, Object>{
        'outer': <Object, Object>{'inner': nested},
      });
    final sender = _StubSender<Map<String, Object?>>();
    final pack = createController<Map<String, Object?>>(
      sender,
      <String, Object?>{},
    );

    channel.receiveOne(<String, Object?>{
      'id': 41,
      'sub': 'outer/inner',
      'body': 'leaf',
    }, pack.controller);

    expect(pack.subscriptions, <String>['outer/inner/leaf']);
    expect(
      pack.responses.any((Map<String, Object?> response) {
        return response['id'] == 41 &&
            response['topic'] == 'outer/inner/leaf' &&
            response['body'] == 5;
      }),
      isTrue,
    );
  });

  test('defaultStateValue returns map for Map<Object?, Object?> state', () {
    final state = defaultStateValue<Map<Object?, Object?>>();
    expect(state, isEmpty);
  });

  test('process subscribe path hits no-op event callback', () async {
    final updates = Subscription()..body((Object? _) => 1);
    final channel = Channel<Map<String, Object?>>()
      ..events(<String, Object?>{'updates': updates});
    final sender = channel.process(state: <String, Object?>{});

    final subscription = await sender.subscribe<int>(
      'updates',
      body: 'ready',
      onEvent: (int _) {},
    );
    subscription.cancel();
    sender.stop();
  });
}

class _TestPublisher implements SubscriptionPublisher {
  _TestPublisher(this.onPublish);

  final void Function(SubscriptionEvent event) onPublish;

  @override
  void publish(SubscriptionEvent event) {
    onPublish(event);
  }
}
