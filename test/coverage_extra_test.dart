import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:channel_dart/channel.dart';
import 'package:channel_dart/client.dart';
import 'package:channel_dart/process.dart';
import 'package:channel_dart/server.dart';
import 'package:test/test.dart';

Future<void> waitFor(
  bool Function() condition, {
  Duration timeout = const Duration(seconds: 2),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (DateTime.now().isBefore(deadline)) {
    if (condition()) {
      return;
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  throw TimeoutException('Condition was not met before timeout');
}

Future<HttpServer> startWsServer(
  FutureOr<void> Function(WebSocket socket) onSocket,
) async {
  final server = await HttpServer.bind('127.0.0.1', 0);
  unawaited(
    server.forEach((HttpRequest request) async {
      final socket = await WebSocketTransformer.upgrade(request);
      await onSocket(socket);
    }),
  );
  return server;
}

class _CapturePublisher implements SubscriptionPublisher {
  final List<SubscriptionEvent> events = <SubscriptionEvent>[];

  @override
  void publish(SubscriptionEvent event) {
    events.add(event);
  }
}

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
  }) async {
    throw UnimplementedError();
  }

  @override
  SendingRequest<T> request<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    throw UnimplementedError();
  }

  @override
  Stream<T> values<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    throw UnimplementedError();
  }

  @override
  Future<Cancellable> subscribe<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
    required void Function(T event) onEvent,
  }) async {
    throw UnimplementedError();
  }

  @override
  void stop() {}
}

class _ConnectionSpy implements ConnectionInterface {
  int _next = 1000;
  final List<Object?> sentBodies = <Object?>[];
  final List<Object?> notifiedBodies = <Object?>[];
  bool cancelResult = false;

  @override
  bool Function() addTopic(String topic, EventBody event) {
    return () => true;
  }

  @override
  bool cancel(int id) => cancelResult;

  @override
  void notify(Object? body) {
    notifiedBodies.add(body);
  }

  @override
  int send(Object? body) {
    sentBodies.add(body);
    return _next++;
  }

  @override
  void sent(int id) {}

  @override
  void stop() {}
}

ChannelController<Object?> loopbackController(Sender<Object?> sender) {
  return ChannelController<Object?>(
    response: (_) {},
    subscribe: (_) {},
    unsubscribe: (_) {},
    event: (_, __) {},
    sender: sender,
    state: null,
  );
}

void main() {
  group('json engine coverage', () {
    test('jsonEngineFor covers both selectors and codecs', () {
      final dartEngine = jsonEngineFor(JsonEngineKind.dart);
      final fastEngine = jsonEngineFor(JsonEngineKind.fast);

      final source = <String, Object?>{'id': 1, 'text': 'ok'};
      final dartDecoded = dartEngine.decode(dartEngine.encode(source)) as Map;
      final fastDecoded = fastEngine.decode(fastEngine.encode(source)) as Map;

      expect(dartDecoded['id'], 1);
      expect(fastDecoded['text'], 'ok');
    });

    test('fast json throws on malformed input', () {
      expect(
        () => const FastJsonEngine().decode('{'),
        throwsA(isA<FormatException>()),
      );
    });
  });

  group('core additional coverage', () {
    test('onCancel after cancellation runs action immediately', () {
      var onCancelled = 0;
      var actionRuns = 0;
      final request = BasicCancellableRequest(
        onCancelled: () {
          onCancelled += 1;
        },
      );

      request.cancel();
      request.onCancel(() {
        actionRuns += 1;
      });

      expect(onCancelled, 1);
      expect(actionRuns, 1);
    });

    test('apiPost and apiStream registration helpers work', () async {
      final channel = Channel<Map<String, Object?>>()
        ..api(<String, Object?>{
          'root': <String, Object?>{
            'post': apiPost<Map<String, Object?>>((_) => 'ok'),
            'stream': apiStream<Map<String, Object?>>((_) async* {
              yield 10;
            }),
          },
        });

      final sender = _StubSender<Map<String, Object?>>();
      final responses = <Map<String, Object?>>[];
      final controller = ChannelController<Map<String, Object?>>(
        response: responses.add,
        subscribe: (_) {},
        unsubscribe: (_) {},
        event: (_, __) {},
        sender: sender,
        state: <String, Object?>{},
      );

      channel.receiveOne(<String, Object?>{
        'id': 1,
        'path': 'root/post',
      }, controller);
      channel.receiveOne(<String, Object?>{
        'id': 2,
        'stream': 'root/stream',
      }, controller);
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(
        responses.any(
          (Map<String, Object?> response) =>
              response['id'] == 1 && response['body'] == 'ok',
        ),
        isTrue,
      );
      expect(
        responses.any(
          (Map<String, Object?> response) =>
              response['id'] == 2 && response['done'] == true,
        ),
        isTrue,
      );
    });

    test('defaultStateValue covers map and throw branches', () {
      final m1 = defaultStateValue<Map<String, Object?>>();
      final m2 = defaultStateValue<Map<Object?, Object?>>();

      expect(m1, isA<Map<String, Object?>>());
      expect(m2, isA<Map<Object?, Object?>>());
      expect(() => defaultStateValue<int>(), throwsStateError);
    });

    test(
      'receiveOne with async no-id post and untyped maps does not throw',
      () async {
        final channel = Channel<Map<String, Object?>>()
          ..post('fire-and-forget', (_) => Future<Object?>.error('ignored'))
          ..post(
            'echo-id',
            (ChannelBody<Map<String, Object?>> body) => body.body,
          );

        final sender = _StubSender<Map<String, Object?>>();
        final responses = <Map<String, Object?>>[];
        final controller = ChannelController<Map<String, Object?>>(
          response: responses.add,
          subscribe: (_) {},
          unsubscribe: (_) {},
          event: (_, __) {},
          sender: sender,
          state: <String, Object?>{},
        );

        channel.receiveOne(<String, dynamic>{
          'path': 'fire-and-forget',
        }, controller);
        channel.receiveOne(<dynamic, dynamic>{
          'id': '9',
          'path': 'echo-id',
          'body': 77,
        }, controller);
        await Future<void>.delayed(const Duration(milliseconds: 10));

        expect(
          responses.any(
            (Map<String, Object?> response) =>
                response['id'] == 9 && response['body'] == 77,
          ),
          isTrue,
        );
      },
    );

    test('subscribe missing topic completes with error', () async {
      final channel = Channel<Object?>();
      final connection = _ConnectionSpy();
      final sender = makeSender<Object?>(channel, connection);

      final pending = sender.subscribe<Object?>('x', onEvent: (_) {});

      final requestId = channel.requests.keys.first;
      channel.receiveOne(
        <String, Object?>{'id': requestId, 'body': 'no-topic'},
        ChannelController<Object?>(
          response: (_) {},
          subscribe: (_) {},
          unsubscribe: (_) {},
          event: (_, __) {},
          sender: sender,
          state: null,
        ),
      );

      await expectLater(pending, throwsA(isA<StateError>()));
    });

    test('subscription async send and async failure path', () async {
      final subscription = Subscription()
        ..topic((Object? request) async => 't/$request')
        ..body((Object? request) async => 'b/$request');
      final publisher = _CapturePublisher();
      subscription.publishers.add(publisher);

      subscription.send('ok');
      await Future<void>.delayed(const Duration(milliseconds: 10));

      expect(publisher.events.length, 1);
      expect(publisher.events.first.topic, 't/ok');
      expect(publisher.events.first.body, 'b/ok');

      subscription.body(
        (Object? _) => Future<Object?>.error(StateError('boom')),
      );
      subscription.send('fail');
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(publisher.events.length, 1);
    });

    test('request/stream/subscription envelopes include context', () {
      final request = ChannelRequest(
        id: 1,
        path: 'p',
        body: <String, Object?>{'a': 1},
        context: <String, Object?>{'ctx': true},
      );
      final stream = ChannelStreamRequest(
        id: 2,
        stream: 's',
        body: <String, Object?>{'a': 2},
        context: <String, Object?>{'ctx': true},
      );
      final sub = ChannelSubscriptionRequest(
        id: 3,
        sub: 'e',
        body: <String, Object?>{'a': 3},
        context: <String, Object?>{'ctx': true},
      );

      expect(request.toJson()['context'], isNotNull);
      expect(stream.toJson()['context'], isNotNull);
      expect(sub.toJson()['context'], isNotNull);
    });

    test('send/request decode failures and stream decode error path', () async {
      final channel = Channel<Object?>();
      final connection = _ConnectionSpy();
      final sender = makeSender<Object?>(channel, connection);
      final controller = loopbackController(sender);

      final sendFuture = sender.send<int>(
        'a',
        decode: (_) => throw StateError('send decode'),
      );
      final sendId = channel.requests.keys.first;
      channel.receiveOne(<String, Object?>{
        'id': sendId,
        'body': 'x',
      }, controller);
      await expectLater(sendFuture, throwsStateError);

      final request = sender.request<int>(
        'b',
        decode: (_) => throw StateError('request decode'),
      );
      final requestId = channel.requests.keys.first;
      channel.receiveOne(<String, Object?>{
        'id': requestId,
        'body': 'x',
      }, controller);
      await expectLater(request.response, throwsStateError);

      final streamErrors = <Object>[];
      final stream = sender.values<int>(
        'c',
        decode: (_) => throw StateError('stream decode'),
      );
      final sub = stream.listen(
        (_) {},
        onError: (Object error) => streamErrors.add(error),
      );
      await Future<void>.delayed(const Duration(milliseconds: 10));
      final streamId = channel.requests.keys.first;
      channel.receiveOne(<String, Object?>{
        'id': streamId,
        'body': 'x',
      }, controller);
      await waitFor(() => streamErrors.isNotEmpty);
      expect(streamErrors.first, isA<StateError>());
      await sub.cancel();
    });
  });

  group('process extension coverage', () {
    test('process default state and idempotent stop', () async {
      final updates = Subscription();
      final channel = Channel<Object?>()
        ..post('hello', (_) => 'world')
        ..events(<String, Object?>{'updates': updates});

      final sender = channel.process();
      final value = await sender.send<String>('hello');
      expect(value, 'world');

      final cancellable = await sender.subscribe<Object?>(
        'updates',
        body: 'a',
        onEvent: (_) {},
      );
      cancellable.cancel();

      sender.stop();
      sender.stop();
    });

    test('process request completion and cancel-notify path', () async {
      final never = Completer<Object?>();
      final channel = Channel<Object?>()
        ..post('hello', (_) => 'world')
        ..post('slow', (ChannelBody<Object?> body) async {
          body.task?.onCancel(() {});
          return never.future;
        });

      final sender = channel.process();

      final req = sender.request<String>('hello');
      expect(await req.response, 'world');

      final slow = sender.request<Object?>('slow');
      unawaited(slow.response.catchError((Object _) => null));
      slow.cancel();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      sender.stop();
    });
  });

  group('client extension and websocket client coverage', () {
    test('connect validates address type', () {
      final channel = Channel<Object?>();
      expect(() => channel.connect(Object()), throwsArgumentError);
    });

    test('ClientSender getters and request proxy', () async {
      final server =
          await (Channel<Map<String, Object?>>()..post('hello', (_) => 'world'))
              .listen(0);
      final client = Channel<Map<String, Object?>>().connect(
        server.httpServer.port,
        options: ClientOptions<Map<String, Object?>>(
          state: <String, Object?>{},
        ),
      );

      expect(client.streams, isA<Map<int, StreamCanceller>>());
      expect(client.requests, isA<Map<int, CancellableRequest>>());

      final req = client.request<String>('hello');
      expect(await req.response, 'world');

      client.stop();
      await server.stop();
    });

    test('onConnect callback and publisher cleanup on stop', () async {
      final server =
          await (Channel<Map<String, Object?>>()..post('ok', (_) => true))
              .listen(0);
      final events = <String, Subscription>{'topic': Subscription()};
      final channel = Channel<Map<String, Object?>>()..events(events);

      final connected = Completer<void>();
      final client = channel.connect(
        server.httpServer.port,
        options: ClientOptions<Map<String, Object?>>(
          state: <String, Object?>{},
          onConnect: (_) {
            connected.complete();
          },
        ),
      );

      await connected.future.timeout(const Duration(seconds: 2));
      expect(events['topic']!.publishers.length, 1);

      client.stop();
      client.ws.onClose?.call();
      expect(events['topic']!.publishers, isEmpty);
      await server.stop();
    });

    test(
      'WebSocketClient handles binary messages and topic lifecycle',
      () async {
        final server = await startWsServer((WebSocket socket) async {
          final body = jsonEncode(<String, Object?>{
            'topic': 'hello',
            'body': 42,
          });
          socket.add(utf8.encode(body));
        });

        final received = Completer<dynamic>();
        final client = WebSocketClient(
          'ws://127.0.0.1:${server.port}',
          reconnectDelay: const Duration(milliseconds: 20),
        );

        client.onMessage = (dynamic message) {
          if (!received.isCompleted) {
            received.complete(message);
          }
        };

        final dynamicMessage = await received.future.timeout(
          const Duration(seconds: 2),
        );
        final decoded = dynamicMessage as Map;
        expect(decoded['body'], 42);

        var calls = 0;
        final remove1 = client.addTopic('t', (_) {
          calls += 1;
        });
        final remove2 = client.addTopic('t', (_) {
          calls += 1;
        });
        client.receivedEvent('t', 1);
        expect(calls, 2);
        expect(remove1(), isFalse);
        expect(remove2(), isTrue);
        client.receivedEvent('missing', 1);

        final id = client.send(<String, Object?>{'a': 1});
        client.sent(id);
        client.throttle();
        client.send(<String, Object?>{'b': 2});
        expect(client.cancel(id), isFalse);

        client.stop();
        await server.close(force: true);
      },
    );

    test('WebSocketClient reconnect scheduling and offline cancel', () async {
      final client = WebSocketClient(
        'ws://127.0.0.1:1',
        reconnectDelay: const Duration(milliseconds: 20),
      );

      final id = client.send(<String, Object?>{'ping': true});
      expect(client.cancel(id), isTrue);

      await Future<void>.delayed(const Duration(milliseconds: 80));
      expect(client.isRunning, isTrue);

      client.stop();
      expect(client.isRunning, isFalse);
    });

    test('WebSocketClient closes socket when stopped during connect', () async {
      final accepted = Completer<WebSocket>();
      final closed = Completer<void>();
      final server = await startWsServer((WebSocket socket) async {
        if (!accepted.isCompleted) {
          accepted.complete(socket);
        }
        socket.listen(
          (_) {},
          onDone: () {
            if (!closed.isCompleted) {
              closed.complete();
            }
          },
        );
      });

      final headersCalled = Completer<void>();
      final headersGate = Completer<Map<String, String>>();
      final client = WebSocketClient(
        'ws://127.0.0.1:${server.port}',
        headers: () async {
          if (!headersCalled.isCompleted) {
            headersCalled.complete();
          }
          return headersGate.future;
        },
        reconnectDelay: const Duration(milliseconds: 20),
      );

      await headersCalled.future.timeout(const Duration(seconds: 2));
      client.stop();
      headersGate.complete(<String, String>{});

      await accepted.future.timeout(const Duration(seconds: 2));
      await closed.future.timeout(const Duration(seconds: 2));

      await server.close(force: true);
    });

    test(
      'WebSocketClient send queue branches and disconnect callback',
      () async {
        final sockets = <WebSocket>[];
        final server = await startWsServer((WebSocket socket) async {
          sockets.add(socket);
        });

        final closed = Completer<void>();
        final client = WebSocketClient(
          'ws://127.0.0.1:${server.port}',
          reconnectDelay: const Duration(milliseconds: 20),
        );
        client.onClose = () {
          if (!closed.isCompleted) {
            closed.complete();
          }
        };

        await waitFor(() => client.isConnected);

        for (var i = 0; i < 4105; i += 1) {
          client.send(<String, Object?>{'i': i});
        }
        await Future<void>.delayed(const Duration(milliseconds: 20));

        client.throttle();
        client.send(<String, Object?>{'q': 1});
        client.send(<String, Object?>{'q': 2});
        await Future<void>.delayed(const Duration(milliseconds: 20));

        await sockets.first.close();
        await closed.future.timeout(const Duration(seconds: 2));

        client.stop();
        await server.close(force: true);
      },
    );
  });

  group('server extension coverage', () {
    test('listen validates address argument', () async {
      final channel = Channel<Object?>();
      await expectLater(channel.listen('bad-address'), throwsArgumentError);
      await expectLater(channel.listen(Object()), throwsArgumentError);
    });

    test('listen parses host:port and query/header state input', () async {
      Map<String, String>? capturedHeaders;
      final server = await Channel<Map<String, Object?>>().listen(
        '127.0.0.1:0',
        options: ListenOptions<Map<String, Object?>>(
          state: (Map<String, String> headers) {
            capturedHeaders = headers;
            return <String, Object?>{};
          },
        ),
      );

      final ws = await WebSocket.connect(
        'ws://127.0.0.1:${server.httpServer.port}/?token=q1',
        headers: <String, dynamic>{'x-test': 'h1'},
      );
      await ws.close();

      await waitFor(() => capturedHeaders != null);
      expect(capturedHeaders!['token'], 'q1');
      expect(capturedHeaders!['x-test'], 'h1');
      await server.stop();
    });

    test('non websocket request returns 400', () async {
      final server = await Channel<Object?>().listen(0);
      final client = HttpClient();
      final request = await client.getUrl(
        Uri.parse('http://127.0.0.1:${server.httpServer.port}/'),
      );
      final response = await request.close();
      expect(response.statusCode, HttpStatus.badRequest);
      client.close(force: true);
      await server.stop();
    });

    test('state factory failure rejects websocket upgrade', () async {
      final server = await Channel<Object?>().listen(
        0,
        options: ListenOptions<Object?>(
          state: (_) => throw StateError('no state'),
        ),
      );

      await expectLater(
        WebSocket.connect('ws://127.0.0.1:${server.httpServer.port}'),
        throwsA(anything),
      );
      await server.stop();
    });

    test(
      'malformed websocket payload closes connection and hooks fire',
      () async {
        var connected = 0;
        var disconnected = 0;
        final server =
            await (Channel<Map<String, Object?>>()..post('ok', (_) => true))
                .listen(
                  0,
                  options: ListenOptions<Map<String, Object?>>(
                    state: (_) => <String, Object?>{},
                    onConnect: (_) {
                      connected += 1;
                    },
                    onDisconnect: (_) {
                      disconnected += 1;
                    },
                  ),
                );

        final ws = await WebSocket.connect(
          'ws://127.0.0.1:${server.httpServer.port}',
        );
        ws.add('{');
        await Future<void>.delayed(const Duration(milliseconds: 200));
        if (disconnected == 0) {
          await ws.close();
        }

        await waitFor(() => disconnected >= 1);
        expect(connected, 1);
        expect(disconnected, greaterThanOrEqualTo(1));
        await server.stop();
      },
    );

    test('Subscriptions add/remove and event dispatch', () {
      final subscriptions = Subscriptions();
      var sum = 0;

      final cancel1 = subscriptions.addTopic('a', (Object? body) {
        sum += body as int;
      });
      final cancel2 = subscriptions.addTopic('a', (Object? body) {
        sum += body as int;
      });

      subscriptions.receivedEvent('a', 3);
      subscriptions.receivedEvent('missing', 9);
      expect(sum, 6);

      expect(cancel1(), isFalse);
      expect(cancel2(), isTrue);
      expect(cancel2(), isFalse);
    });

    test(
      'server sender transport paths (send/subscribe/cancel/notify)',
      () async {
        final server =
            await (Channel<Map<String, Object?>>()
                  ..post('mirror', (ChannelBody<Map<String, Object?>> body) {
                    return body.sender.send<String>('client/hello');
                  })
                  ..post('subscribe', (
                    ChannelBody<Map<String, Object?>> body,
                  ) async {
                    await body.sender.subscribe<int>(
                      'x',
                      body: '1',
                      onEvent: (int _) {},
                    );
                    return 'subscribed';
                  })
                  ..post('cancel-remote', (
                    ChannelBody<Map<String, Object?>> body,
                  ) {
                    final req = body.sender.request<Object?>('never');
                    req.cancel();
                    return true;
                  }))
                .listen(
                  0,
                  options: ListenOptions<Map<String, Object?>>(
                    state: (_) => <String, Object?>{},
                  ),
                );

        final ws = await WebSocket.connect(
          'ws://127.0.0.1:${server.httpServer.port}',
        );

        final mirrorResponse = Completer<void>();
        final subscribeResponse = Completer<void>();
        final cancelResponse = Completer<void>();

        ws.listen((dynamic raw) {
          if (raw is! String) {
            return;
          }
          final decoded = jsonDecode(raw);
          if (decoded is! Map) {
            return;
          }
          final msg = decoded.cast<String, dynamic>();

          final path = msg['path'];
          final sub = msg['sub'];
          if (path == 'client/hello') {
            final rid = (msg['id'] as num?)?.toInt();
            if (rid == null) {
              return;
            }
            ws.add(
              jsonEncode(<String, Object?>{'id': rid, 'body': 'client world'}),
            );
            return;
          }
          if (path == 'never') {
            return;
          }
          if (sub == 'x') {
            final rid = (msg['id'] as num?)?.toInt();
            if (rid == null) {
              return;
            }
            ws.add(
              jsonEncode(<String, Object?>{
                'id': rid,
                'topic': 'x/1',
                'body': 1,
              }),
            );
            return;
          }

          if (msg['id'] == 1 && msg['body'] == 'client world') {
            if (!mirrorResponse.isCompleted) {
              mirrorResponse.complete();
            }
            return;
          }
          if (msg['id'] == 2 && msg['body'] == 'subscribed') {
            if (!subscribeResponse.isCompleted) {
              subscribeResponse.complete();
            }
            return;
          }
          if (msg['id'] == 3 && msg['body'] == true) {
            if (!cancelResponse.isCompleted) {
              cancelResponse.complete();
            }
          }
        });

        ws.add(jsonEncode(<String, Object?>{'id': 1, 'path': 'mirror'}));
        await mirrorResponse.future.timeout(const Duration(seconds: 2));

        ws.add(jsonEncode(<String, Object?>{'id': 2, 'path': 'subscribe'}));
        await subscribeResponse.future.timeout(const Duration(seconds: 2));

        ws.add(jsonEncode(<String, Object?>{'id': 3, 'path': 'cancel-remote'}));
        await cancelResponse.future.timeout(const Duration(seconds: 2));

        await ws.close();
        await server.stop();
      },
    );

    test('server routes topic events into subscription handlers', () async {
      var seen = 0;
      final server = await Channel<Map<String, Object?>>().listen(
        0,
        options: ListenOptions<Map<String, Object?>>(
          state: (_) => <String, Object?>{},
          onConnect: (BodyContext<Map<String, Object?>> connection) {
            connection.subscriptions.addTopic('evt', (Object? body) {
              seen += body as int;
            });
          },
        ),
      );

      final ws = await WebSocket.connect(
        'ws://127.0.0.1:${server.httpServer.port}',
      );
      ws.add(jsonEncode(<String, Object?>{'topic': 'evt', 'body': 3}));
      await waitFor(() => seen == 3);
      await ws.close();
      await server.stop();
    });

    test('server closes socket when event handler throws', () async {
      final server = await Channel<Map<String, Object?>>().listen(
        0,
        options: ListenOptions<Map<String, Object?>>(
          state: (_) => <String, Object?>{},
          onConnect: (BodyContext<Map<String, Object?>> connection) {
            connection.subscriptions.addTopic('boom', (Object? _) {
              throw StateError('boom');
            });
          },
        ),
      );

      final ws = await WebSocket.connect(
        'ws://127.0.0.1:${server.httpServer.port}',
      );
      final closed = Completer<void>();
      ws.listen(
        (_) {},
        onDone: () {
          if (!closed.isCompleted) {
            closed.complete();
          }
        },
      );
      ws.add(jsonEncode(<String, Object?>{'topic': 'boom', 'body': 1}));
      await closed.future.timeout(const Duration(seconds: 2));
      await server.stop();
    });

    test('server sender stop closes connection', () async {
      final server =
          await (Channel<Map<String, Object?>>()..post('close', (body) {
                body.sender.stop();
                return null;
              }))
              .listen(
                0,
                options: ListenOptions<Map<String, Object?>>(
                  state: (_) => <String, Object?>{},
                ),
              );

      final ws = await WebSocket.connect(
        'ws://127.0.0.1:${server.httpServer.port}',
      );
      final closed = Completer<void>();
      ws.listen(
        (_) {},
        onDone: () {
          if (!closed.isCompleted) {
            closed.complete();
          }
        },
      );

      ws.add(jsonEncode(<String, Object?>{'path': 'close'}));
      await closed.future.timeout(const Duration(seconds: 2));
      await server.stop();
    });
  });
}
