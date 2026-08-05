import 'dart:async';
import 'dart:io';

import '../core/channel_core.dart';
import '../json/json_engine.dart';

class BodyContext<State> {
  BodyContext({
    required this.state,
    required this.sender,
    required this.subscriptions,
  });

  final State state;
  final Sender<State> sender;
  final Subscriptions subscriptions;
}

class ListenOptions<State> {
  const ListenOptions({
    this.state,
    this.onConnect,
    this.onDisconnect,
    this.jsonEngine,
  });

  final FutureOr<State> Function(Map<String, String> headers)? state;
  final void Function(BodyContext<State> connection)? onConnect;
  final void Function(BodyContext<State> connection)? onDisconnect;
  final JsonEngine? jsonEngine;
}

class Subscriptions {
  final Map<String, Map<int, EventBody>> _subscribed =
      <String, Map<int, EventBody>>{};
  int _id = 0;

  bool Function() addTopic(String topic, EventBody event) {
    final id = ++_id;
    final map = _subscribed.putIfAbsent(topic, () => <int, EventBody>{});
    map[id] = event;

    return () {
      final handlers = _subscribed[topic];
      if (handlers == null) {
        return false;
      }
      handlers.remove(id);
      if (handlers.isEmpty) {
        _subscribed.remove(topic);
        return true;
      }
      return false;
    };
  }

  void receivedEvent(String topic, Object? event) {
    final handlers = _subscribed[topic];
    if (handlers == null || handlers.isEmpty) {
      return;
    }
    for (final handler in handlers.values.toList(growable: false)) {
      handler(event);
    }
  }
}

class ChannelServer<State> {
  ChannelServer({
    required this.httpServer,
    required this.cleanupPublisher,
    required this.connections,
  });

  final HttpServer httpServer;
  final void Function() cleanupPublisher;
  final Set<_ServerSocketContext<State>> connections;

  Future<void> stop() async {
    cleanupPublisher();
    for (final connection in connections.toList(growable: false)) {
      try {
        await connection.socket.close();
      } catch (_) {}
    }
    await httpServer.close(force: true);
  }
}

extension ChannelServerExtension<State> on Channel<State> {
  Future<ChannelServer<State>> listen(
    Object address, {
    ListenOptions<State>? options,
  }) async {
    final listenOptions = options ?? ListenOptions<State>();
    final jsonEngine = listenOptions.jsonEngine ?? const DartJsonEngine();

    var hostname = '127.0.0.1';
    late int port;
    if (address is String) {
      final pieces = address.split(':');
      if (pieces.length != 2) {
        throw ArgumentError.value(address, 'address', 'must be `host:port`');
      }
      hostname = pieces[0];
      port = int.parse(pieces[1]);
    } else if (address is int) {
      port = address;
    } else {
      throw ArgumentError.value(address, 'address', 'must be int or String');
    }

    final httpServer = await HttpServer.bind(hostname, port);
    final connections = <_ServerSocketContext<State>>{};

    final publisher = _ServerPublisher<State>(
      connections: connections,
      jsonEngine: jsonEngine,
    );

    eventsApi?.forEach((String _, Subscription subscription) {
      subscription.publishers.add(publisher);
    });

    var publisherRemoved = false;
    void cleanupPublisher() {
      if (publisherRemoved) {
        return;
      }
      publisherRemoved = true;
      eventsApi?.forEach((String _, Subscription subscription) {
        subscription.publishers.remove(publisher);
      });
    }

    unawaited(
      httpServer.forEach((HttpRequest request) async {
        if (!WebSocketTransformer.isUpgradeRequest(request)) {
          request.response.statusCode = HttpStatus.badRequest;
          await request.response.close();
          return;
        }

        final headers = <String, String>{};
        request.uri.queryParameters.forEach((String key, String value) {
          headers[key] = value;
        });
        request.headers.forEach((String key, List<String> values) {
          if (values.isNotEmpty) {
            headers[key] = values.last;
          }
        });

        late final State state;
        try {
          if (listenOptions.state != null) {
            state = await listenOptions.state!(headers);
          } else {
            state = defaultStateValue<State>();
          }
        } catch (_) {
          request.response.statusCode = HttpStatus.internalServerError;
          await request.response.close();
          return;
        }

        WebSocket socket;
        try {
          socket = await WebSocketTransformer.upgrade(request);
        } catch (_) {
          return;
        }

        final subscriptions = Subscriptions();
        final subscribedTopics = <String>{};

        final connectionInterface = _ServerConnectionInterface(
          socket: socket,
          jsonEngine: jsonEngine,
          subscriptions: subscriptions,
        );
        final sender = makeSender<State>(this, connectionInterface);

        final bodyContext = BodyContext<State>(
          state: state,
          sender: sender,
          subscriptions: subscriptions,
        );

        final serverContext = _ServerSocketContext<State>(
          socket: socket,
          subscribedTopics: subscribedTopics,
          sender: sender,
          state: state,
          bodyContext: bodyContext,
        );
        connections.add(serverContext);

        listenOptions.onConnect?.call(bodyContext);

        var closed = false;
        void closeHandler([Object? _]) {
          if (closed) {
            return;
          }
          closed = true;
          connections.remove(serverContext);
          disconnect(state, sender);
          listenOptions.onDisconnect?.call(bodyContext);
        }

        socket.listen(
          (dynamic message) {
            if (message is! String) {
              return;
            }

            dynamic received;
            try {
              received = jsonEngine.decode(message);
            } catch (_) {
              unawaited(socket.close());
              return;
            }

            final controller = ChannelController<State>(
              response: (Map<String, Object?> body) {
                if (socket.readyState == WebSocket.open) {
                  socket.add(jsonEngine.encode(body));
                }
              },
              subscribe: (String topic) {
                subscribedTopics.add(topic);
              },
              unsubscribe: (String topic) {
                subscribedTopics.remove(topic);
              },
              event: (String topic, Object? event) {
                subscriptions.receivedEvent(topic, event);
              },
              sender: sender,
              state: state,
            );

            try {
              receive(received, controller);
            } catch (_) {
              unawaited(socket.close());
            }
          },
          onError: closeHandler,
          onDone: closeHandler,
          cancelOnError: true,
        );
      }),
    );

    return ChannelServer<State>(
      httpServer: httpServer,
      cleanupPublisher: cleanupPublisher,
      connections: connections,
    );
  }
}

class _ServerConnectionInterface implements ConnectionInterface {
  _ServerConnectionInterface({
    required this.socket,
    required this.jsonEngine,
    required this.subscriptions,
  });

  final WebSocket socket;
  final JsonEngine jsonEngine;
  final Subscriptions subscriptions;

  @override
  int send(Object? body) {
    if (socket.readyState == WebSocket.open) {
      socket.add(jsonEngine.encode(body));
    }
    return 0;
  }

  @override
  void sent(int id) {}

  @override
  bool cancel(int id) => false;

  @override
  void notify(Object? body) {
    if (socket.readyState == WebSocket.open) {
      socket.add(jsonEngine.encode(body));
    }
  }

  @override
  bool Function() addTopic(String topic, EventBody event) {
    return subscriptions.addTopic(topic, event);
  }

  @override
  void stop() {
    unawaited(socket.close());
  }
}

class _ServerSocketContext<State> {
  _ServerSocketContext({
    required this.socket,
    required this.subscribedTopics,
    required this.sender,
    required this.state,
    required this.bodyContext,
  });

  final WebSocket socket;
  final Set<String> subscribedTopics;
  final Sender<State> sender;
  final State state;
  final BodyContext<State> bodyContext;
}

class _ServerPublisher<State> implements SubscriptionPublisher {
  _ServerPublisher({required this.connections, required this.jsonEngine});

  final Set<_ServerSocketContext<State>> connections;
  final JsonEngine jsonEngine;

  @override
  void publish(SubscriptionEvent event) {
    final body = event.toJson();
    final encoded = jsonEngine.encode(body);
    for (final connection in connections.toList(growable: false)) {
      if (!connection.subscribedTopics.contains(event.topic)) {
        continue;
      }
      final socket = connection.socket;
      if (socket.readyState != WebSocket.open) {
        continue;
      }
      socket.add(encoded);
    }
  }
}
