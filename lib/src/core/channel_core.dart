import 'dart:async';

typedef Decode<T> = T Function(Object? value);
typedef ChannelPostHandler<State> =
    FutureOr<Object?> Function(ChannelBody<State> body);
typedef ChannelStreamHandler<State> =
    FutureOr<Stream<Object?>> Function(ChannelBody<State> body);
typedef EventBody = void Function(Object? body);

class ChannelBody<State> {
  ChannelBody({
    required this.body,
    required this.sender,
    required this.state,
    required this.path,
    required this.context,
    this.task,
  });

  final Object? body;
  final Sender<State> sender;
  final State state;
  final String path;
  final Object? context;
  final CancellableRequest? task;
}

class ChannelController<State> {
  ChannelController({
    required this.response,
    required this.subscribe,
    required this.unsubscribe,
    required this.event,
    required this.sender,
    required this.state,
  });

  final void Function(Map<String, Object?> response) response;
  final void Function(String topic) subscribe;
  final void Function(String topic) unsubscribe;
  final void Function(String topic, Object? event) event;
  final Sender<State> sender;
  final State state;
}

abstract interface class Cancellable {
  void cancel();
}

abstract interface class CancellableRequest implements Cancellable {
  bool get isCancelled;
  void onCancel(void Function() action);
  void cancelSilently();
}

class BasicCancellableRequest implements CancellableRequest {
  BasicCancellableRequest({required this.onCancelled});

  final void Function() onCancelled;
  bool _isCancelled = false;
  final List<void Function()> _cancelActions = <void Function()>[];

  @override
  bool get isCancelled => _isCancelled;

  @override
  void onCancel(void Function() action) {
    if (_isCancelled) {
      try {
        action();
      } catch (_) {}
      return;
    }
    _cancelActions.add(action);
  }

  @override
  void cancel() {
    if (_isCancelled) {
      return;
    }
    _isCancelled = true;
    for (final action in _cancelActions) {
      try {
        action();
      } catch (_) {}
    }
    onCancelled();
  }

  @override
  void cancelSilently() {
    if (_isCancelled) {
      return;
    }
    _isCancelled = true;
    for (final action in _cancelActions) {
      try {
        action();
      } catch (_) {}
    }
  }
}

class StreamCanceller {
  StreamCanceller(this._cancel);

  final Future<void> Function() _cancel;

  Future<void> cancel() => _cancel();

  factory StreamCanceller.fromSubscription(
    StreamSubscription<dynamic> subscription,
  ) {
    return StreamCanceller(() => subscription.cancel());
  }
}

abstract interface class Sender<State> {
  Map<int, StreamCanceller> get streams;
  Map<int, CancellableRequest> get requests;

  Future<T> send<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  });

  SendingRequest<T> request<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  });

  Stream<T> values<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  });

  Future<Cancellable> subscribe<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
    required void Function(T event) onEvent,
  });

  void stop();
}

class SendingRequest<T> implements Cancellable {
  SendingRequest({required this.response, required void Function() onCancel})
    : _onCancel = onCancel;

  final Future<T> response;
  final void Function() _onCancel;

  @override
  void cancel() {
    _onCancel();
  }
}

abstract interface class ConnectionInterface {
  int? send(Object? body);
  void sent(int id);
  bool cancel(int id);
  void notify(Object? body);
  bool Function() addTopic(String topic, EventBody event);
  void stop();
}

Sender<State> makeSender<State>(
  Channel<State> channel,
  ConnectionInterface connection,
) {
  return ChannelSender<State>(channel, connection);
}

class ChannelSender<State> implements Sender<State> {
  ChannelSender(this._channel, this._connection);

  final Channel<State> _channel;
  final ConnectionInterface _connection;

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
    final completer = Completer<T>();
    int? sentId;
    final request = _channel.makeRequest(
      path,
      body: body,
      context: context,
      response: (Map<String, Object?> response) {
        final error = response['error'];
        if (error != null) {
          if (!completer.isCompleted) {
            completer.completeError(error);
          }
        } else {
          final value = response['body'];
          if (!completer.isCompleted) {
            try {
              completer.complete(_decode<T>(value, decode));
            } catch (error, stackTrace) {
              completer.completeError(error, stackTrace);
            }
          }
        }
        if (sentId != null) {
          final requestSentId = sentId;
          _connection.sent(requestSentId);
        }
      },
      sender: this,
    );
    sentId = _connection.send(request.toJson());
    return completer.future;
  }

  @override
  SendingRequest<T> request<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    final completer = Completer<T>();
    int? sentId;
    int? requestId;

    final request = _channel.makeRequest(
      path,
      body: body,
      context: context,
      response: (Map<String, Object?> response) {
        final error = response['error'];
        if (error != null) {
          if (!completer.isCompleted) {
            completer.completeError(error);
          }
        } else {
          final value = response['body'];
          if (!completer.isCompleted) {
            try {
              completer.complete(_decode<T>(value, decode));
            } catch (error, stackTrace) {
              completer.completeError(error, stackTrace);
            }
          }
        }
        if (sentId != null) {
          final requestSentId = sentId!;
          _connection.sent(requestSentId);
          sentId = null;
          requestId = null;
        }
      },
      sender: this,
    );

    requestId = request.id;
    sentId = _connection.send(request.toJson());

    return SendingRequest<T>(
      response: completer.future,
      onCancel: () {
        final localSentId = sentId;
        final localRequestId = requestId;
        if (localSentId != null) {
          final cancelled = _connection.cancel(localSentId);
          if (!cancelled && localRequestId != null) {
            _connection.notify(<String, Object?>{'cancel': localRequestId});
          }
        } else if (localRequestId != null) {
          _connection.notify(<String, Object?>{'cancel': localRequestId});
        }

        if (localRequestId != null) {
          _channel.cancel(localRequestId);
        }
        sentId = null;
        requestId = null;
      },
    );
  }

  @override
  Stream<T> values<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    late StreamController<T> controller;
    int? sentId;
    int? requestId;
    var closed = false;

    Future<void> cancelRemote() async {
      final rid = requestId;
      if (rid == null) {
        return;
      }
      final cid = sentId;
      if (cid != null) {
        if (!_connection.cancel(cid)) {
          _connection.send(<String, Object?>{'cancel': rid});
        }
      } else {
        _connection.send(<String, Object?>{'cancel': rid});
      }
      _channel.cancel(rid);
      requestId = null;
      sentId = null;
    }

    void onResponse(Map<String, Object?> response) {
      if (closed) {
        return;
      }
      final error = response['error'];
      if (error != null) {
        closed = true;
        controller.addError(error);
        unawaited(controller.close());
        requestId = null;
        sentId = null;
        return;
      }
      if (response['done'] == true) {
        closed = true;
        unawaited(controller.close());
        requestId = null;
        sentId = null;
        return;
      }

      try {
        final value = _decode<T>(response['body'], decode);
        controller.add(value);
      } catch (error, stackTrace) {
        closed = true;
        controller.addError(error, stackTrace);
        unawaited(controller.close());
        requestId = null;
        sentId = null;
      }
    }

    controller = StreamController<T>(
      onListen: () {
        if (closed) {
          return;
        }
        final request = _channel.makeStream(
          path,
          body: body,
          context: context,
          response: onResponse,
          sender: this,
        );
        requestId = request.id;
        sentId = _connection.send(request.toJson());
      },
      onCancel: () async {
        await cancelRemote();
      },
    );

    return controller.stream;
  }

  @override
  Future<Cancellable> subscribe<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
    required void Function(T event) onEvent,
  }) {
    final completer = Completer<Cancellable>();
    final request = _channel.makeSubscription(
      path,
      body: body,
      context: context,
      response: (Map<String, Object?> response) {
        final error = response['error'];
        if (error != null) {
          if (!completer.isCompleted) {
            completer.completeError(error);
          }
          return;
        }

        final topic = response['topic'] as String?;
        if (topic == null) {
          if (!completer.isCompleted) {
            completer.completeError(StateError('missing topic'));
          }
          return;
        }

        final removeLocalTopic = _connection.addTopic(topic, (Object? event) {
          final value = _decode<T>(event, decode);
          onEvent(value);
        });

        if (!completer.isCompleted) {
          completer.complete(
            _SubscriptionCancellation(() {
              final shouldUnsubscribeRemote = removeLocalTopic();
              if (shouldUnsubscribeRemote) {
                _connection.notify(<String, Object?>{'unsub': topic});
              }
            }),
          );
        }
      },
      sender: this,
    );

    _connection.send(request.toJson());
    return completer.future;
  }

  @override
  void stop() {
    _connection.stop();
    _channel.failPending(this, 'disconnected');
  }
}

class _SubscriptionCancellation implements Cancellable {
  _SubscriptionCancellation(this._onCancel);

  final void Function() _onCancel;

  @override
  void cancel() {
    _onCancel();
  }
}

class ChannelRequest {
  ChannelRequest({
    required this.id,
    required this.path,
    this.body,
    this.context,
  });

  final int id;
  final String path;
  final Object? body;
  final Object? context;

  Map<String, Object?> toJson() {
    final map = <String, Object?>{'id': id, 'path': path};
    if (body != null) {
      map['body'] = body;
    }
    if (context != null) {
      map['context'] = context;
    }
    return map;
  }
}

class ChannelStreamRequest {
  ChannelStreamRequest({
    required this.id,
    required this.stream,
    this.body,
    this.context,
  });

  final int id;
  final String stream;
  final Object? body;
  final Object? context;

  Map<String, Object?> toJson() {
    final map = <String, Object?>{'id': id, 'stream': stream};
    if (body != null) {
      map['body'] = body;
    }
    if (context != null) {
      map['context'] = context;
    }
    return map;
  }
}

class ChannelSubscriptionRequest {
  ChannelSubscriptionRequest({
    required this.id,
    required this.sub,
    this.body,
    this.context,
  });

  final int id;
  final String sub;
  final Object? body;
  final Object? context;

  Map<String, Object?> toJson() {
    final map = <String, Object?>{'id': id, 'sub': sub};
    if (body != null) {
      map['body'] = body;
    }
    if (context != null) {
      map['context'] = context;
    }
    return map;
  }
}

class SubscriptionEvent {
  SubscriptionEvent({required this.topic, required this.body});

  final String topic;
  final Object? body;

  Map<String, Object?> toJson() => <String, Object?>{
    'topic': topic,
    'body': body,
  };
}

abstract interface class SubscriptionPublisher {
  void publish(SubscriptionEvent event);
}

class Subscription {
  final List<SubscriptionPublisher> publishers = <SubscriptionPublisher>[];
  String prefix = '';

  FutureOr<String> Function(Object? request) _topic = (Object? request) =>
      request == null ? '' : '$request';
  FutureOr<Object?> Function(Object? request) _body = (Object? request) => null;

  void topic(FutureOr<String> Function(Object? request) make) {
    _topic = make;
  }

  void body(FutureOr<Object?> Function(Object? request) make) {
    _body = make;
  }

  FutureOr<String> resolveTopic(Object? request) {
    return _topic(request);
  }

  FutureOr<Object?> resolveBody(Object? request) {
    return _body(request);
  }

  static const Object _unset = Object();

  void send(Object? request, [Object? body = _unset]) {
    final topicValue = _topic(request);
    final bodyValue = identical(body, _unset)
        ? _body(request)
        : (body ?? _body(request));

    if (topicValue is Future<String> || bodyValue is Future<Object?>) {
      Future.wait<Object?>(<Future<Object?>>[
            Future<String>.value(topicValue),
            Future<Object?>.value(bodyValue),
          ])
          .then((List<Object?> values) {
            publish(values[0] as String, values[1]);
          })
          .catchError((Object _) {});
      return;
    }

    publish(topicValue, bodyValue);
  }

  void publish(String topic, [Object? body]) {
    // TS parity: `if (!body) return` drops every falsy body (null, false, 0, NaN, '').
    if (isFalsy(body)) {
      return;
    }
    final fullTopic = prefix.isEmpty
        ? topic
        : (topic.isEmpty ? prefix : '$prefix/$topic');
    final event = SubscriptionEvent(topic: fullTopic, body: body);
    for (final publisher in publishers) {
      publisher.publish(event);
    }
  }
}

/// JavaScript truthiness of a channel body.
bool isFalsy(Object? value) {
  if (value == null) return true;
  if (value is bool) return !value;
  if (value is num) return value == 0 || value.isNaN;
  if (value is String) return value.isEmpty;
  return false;
}

class ApiPost<State> {
  ApiPost(this.handler);

  final ChannelPostHandler<State> handler;
}

class ApiStream<State> {
  ApiStream(this.handler);

  final ChannelStreamHandler<State> handler;
}

ApiPost<State> apiPost<State>(ChannelPostHandler<State> handler) =>
    ApiPost<State>(handler);
ApiStream<State> apiStream<State>(ChannelStreamHandler<State> handler) =>
    ApiStream<State>(handler);

class Channel<State> {
  int _id = 0;

  void Function(String topic, Object? body) publish = (String _, Object? _) {};

  final Map<int, _PendingRequest<State>> requests =
      <int, _PendingRequest<State>>{};

  final Map<String, ChannelPostHandler<State>> postApi =
      <String, ChannelPostHandler<State>>{};
  final List<_OtherPostApi<State>> otherPostApi = <_OtherPostApi<State>>[];

  final Map<String, ChannelStreamHandler<State>> streamApi =
      <String, ChannelStreamHandler<State>>{};
  final List<_OtherStreamApi<State>> otherStreamApi =
      <_OtherStreamApi<State>>[];

  void Function(State state, Sender<State> sender)? _onDisconnect;
  Map<String, Subscription>? eventsApi;

  Channel<State> post(String path, ChannelPostHandler<State> request) {
    postApi[path] = request;
    return this;
  }

  Channel<State> postOther(
    bool Function(String path) path,
    ChannelPostHandler<State> request,
  ) {
    otherPostApi.add(_OtherPostApi<State>(path: path, request: request));
    return this;
  }

  Channel<State> stream(String path, ChannelStreamHandler<State> request) {
    streamApi[path] = request;
    return this;
  }

  Channel<State> streamOther(
    bool Function(String path) path,
    ChannelStreamHandler<State> request,
  ) {
    otherStreamApi.add(_OtherStreamApi<State>(path: path, request: request));
    return this;
  }

  Channel<State> onDisconnect(
    void Function(State state, Sender<State> sender) action,
  ) {
    _onDisconnect = action;
    return this;
  }

  /// Runs the disconnect hook, stops incoming streams/tasks of [sender] and settles its outgoing calls
  /// with a `disconnected` error. A reconnecting client passes [keepPending] so its outgoing calls stay
  /// registered and are matched when the transport resends them after reconnecting.
  void disconnect(
    State state,
    Sender<State> sender, {
    bool keepPending = false,
  }) {
    try {
      _onDisconnect?.call(state, sender);
    } catch (_) {}

    for (final stream in sender.streams.values.toList()) {
      try {
        unawaited(stream.cancel());
      } catch (_) {}
    }

    for (final request in sender.requests.values.toList()) {
      request.cancelSilently();
    }

    // Outgoing calls of this sender can't be answered any more: settle them instead of leaving them hanging.
    if (!keepPending) {
      failPending(sender, 'disconnected');
    }

    sender.streams.clear();
    sender.requests.clear();
  }

  /// Settles every outgoing request, stream and subscription made through [sender] with [error].
  /// Used when the transport closes or receives a reply it can't decode, so no caller waits forever.
  void failPending(Sender<State> sender, Object error) {
    final failed = <int, _PendingRequest<State>>{};
    requests.forEach((int key, _PendingRequest<State> request) {
      if (identical(request.request.sender, sender)) {
        failed[key] = request;
      }
    });
    for (final id in failed.keys) {
      requests.remove(id);
    }
    failed.forEach((int id, _PendingRequest<State> request) {
      try {
        request.response(<String, Object?>{'id': id, 'error': error});
      } catch (_) {}
    });
  }

  ChannelRequest makeRequest(
    String path, {
    Object? body,
    Object? context,
    required void Function(Map<String, Object?> response) response,
    Sender<State>? sender,
  }) {
    final id = _id++;
    final request = ChannelRequest(
      id: id,
      path: path,
      body: body,
      context: context,
    );
    requests[id] = _PendingRequest<State>(
      request: _RequestDescriptor<State>(
        id: id,
        kind: _RequestKind.request,
        sender: sender,
      ),
      response: response,
    );
    return request;
  }

  ChannelStreamRequest makeStream(
    String stream, {
    Object? body,
    Object? context,
    required void Function(Map<String, Object?> response) response,
    Sender<State>? sender,
  }) {
    final id = _id++;
    final request = ChannelStreamRequest(
      id: id,
      stream: stream,
      body: body,
      context: context,
    );
    requests[id] = _PendingRequest<State>(
      request: _RequestDescriptor<State>(
        id: id,
        kind: _RequestKind.stream,
        sender: sender,
      ),
      response: response,
    );
    return request;
  }

  ChannelSubscriptionRequest makeSubscription(
    String sub, {
    Object? body,
    Object? context,
    required void Function(Map<String, Object?> response) response,
    Sender<State>? sender,
  }) {
    final id = _id++;
    final request = ChannelSubscriptionRequest(
      id: id,
      sub: sub,
      body: body,
      context: context,
    );
    requests[id] = _PendingRequest<State>(
      request: _RequestDescriptor<State>(
        id: id,
        kind: _RequestKind.subscription,
        sender: sender,
      ),
      response: response,
    );
    return request;
  }

  void cancel(int id) {
    requests.remove(id);
  }

  ChannelPostHandler<State>? resolvePost(String path) {
    final direct = postApi[path];
    if (direct != null) {
      return direct;
    }

    for (final candidate in otherPostApi) {
      if (candidate.path(path)) {
        return candidate.request;
      }
    }
    return null;
  }

  FutureOr<Object?> invokePost(
    ChannelController<State> controller,
    String path, {
    Object? body,
    Object? context,
    CancellableRequest? task,
  }) {
    final api = resolvePost(path);
    if (api == null) {
      throw 'api not found';
    }

    return api(
      ChannelBody<State>(
        body: body,
        context: context ?? <String, Object?>{},
        sender: controller.sender,
        state: controller.state,
        path: path,
        task: task,
      ),
    );
  }

  void receive(dynamic some, ChannelController<State> controller) {
    if (some is List) {
      for (final value in some) {
        receiveOne(value, controller);
      }
      return;
    }
    receiveOne(some, controller);
  }

  void receiveOne(dynamic someValue, ChannelController<State> controller) {
    final some = _toMap(someValue);
    if (some == null) {
      return;
    }

    final path = some['path'];
    if (path is String) {
      final id = _asInt(some['id']);
      BasicCancellableRequest? task;
      try {
        if (id != null) {
          task = BasicCancellableRequest(
            onCancelled: () {
              controller.response(_responseWithId(id, error: 'cancelled'));
            },
          );
        }

        final result = invokePost(
          controller,
          path,
          body: some['body'],
          context: some['context'],
          task: task,
        );

        if (id == null) {
          if (result is Future<Object?>) {
            result.catchError((Object _) => null);
          }
          return;
        }

        if (result is Future<Object?>) {
          if (task != null) {
            controller.sender.requests[id] = task;
          }

          result
              .then((Object? result) {
                if (!(task?.isCancelled ?? false)) {
                  controller.response(_responseWithId(id, body: result));
                }
              })
              .catchError((Object error) {
                if (!(task?.isCancelled ?? false)) {
                  controller.response(_responseWithId(id, error: '$error'));
                }
              })
              .whenComplete(() {
                controller.sender.requests.remove(id);
              });
        } else {
          controller.response(_responseWithId(id, body: result));
          controller.sender.requests.remove(id);
        }
      } catch (error) {
        if (id != null && !(task?.isCancelled ?? false)) {
          controller.response(_responseWithId(id, error: '$error'));
        }
        if (id != null && task != null) {
          controller.sender.requests.remove(id);
        }
      }
      return;
    }

    final streamPath = some['stream'];
    if (streamPath is String) {
      final id = _asInt(some['id']);
      try {
        ChannelStreamHandler<State>? api = streamApi[streamPath];
        if (api == null) {
          for (final candidate in otherStreamApi) {
            if (candidate.path(streamPath)) {
              api = candidate.request;
              break;
            }
          }
        }

        if (api == null) {
          throw 'api not found';
        }
        if (id == null) {
          throw 'stream requires id';
        }

        unawaited(
          _streamRequest(
            id,
            controller,
            streamPath,
            some['body'],
            some['context'] ?? <String, Object?>{},
            api,
          ),
        );
      } catch (error) {
        if (id != null) {
          controller.response(_responseWithId(id, error: '$error'));
        }
      }
      return;
    }

    if (some.containsKey('cancel')) {
      final id = _asInt(some['cancel']);
      if (id == null) {
        return;
      }
      final stream = controller.sender.streams.remove(id);
      if (stream != null) {
        unawaited(stream.cancel());
      }
      final request = controller.sender.requests.remove(id);
      request?.cancel();
      return;
    }

    final subscriptionName = some['sub'];
    if (subscriptionName is String) {
      final id = _asInt(some['id']);
      final subscription = eventsApi?[subscriptionName];
      try {
        if (subscription == null) {
          throw 'subscription not found';
        }

        final rawTopic = subscription.resolveTopic(some['body']);
        final rawBody = subscription.resolveBody(some['body']);
        final isAsync =
            rawTopic is Future<String> || rawBody is Future<Object?>;

        if (isAsync) {
          Future.wait<Object?>(<Future<Object?>>[
                Future<String>.value(rawTopic),
                Future<Object?>.value(rawBody),
              ])
              .then((List<Object?> value) {
                final topic = value[0] as String;
                final body = value[1];
                final fullTopic = _joinTopic(subscription.prefix, topic);
                controller.subscribe(fullTopic);
                if (id != null) {
                  controller.response(
                    _responseWithId(id, topic: fullTopic, body: body),
                  );
                }
              })
              .catchError((Object error) {
                if (id != null) {
                  controller.response(_responseWithId(id, error: '$error'));
                }
              });
          return;
        }

        final topic = rawTopic;
        final body = rawBody;
        final fullTopic = _joinTopic(subscription.prefix, topic);
        controller.subscribe(fullTopic);
        if (id != null) {
          controller.response(
            _responseWithId(id, topic: fullTopic, body: body),
          );
        }
      } catch (error) {
        if (id != null) {
          controller.response(_responseWithId(id, error: '$error'));
        }
      }
      return;
    }

    if (some['unsub'] is String) {
      controller.unsubscribe(some['unsub'] as String);
      return;
    }

    if (some['topic'] is String) {
      final topic = some['topic'] as String;
      final id = _asInt(some['id']);
      if (id != null) {
        final request = requests[id];
        requests.remove(id);
        request?.response(some);
      }
      // TS parity: `if (some.body)` - falsy event bodies are not delivered.
      if (!isFalsy(some['body'])) {
        controller.event(topic, some['body']);
      }
      return;
    }

    final id = _asInt(some['id']);
    if (id == null) {
      return;
    }

    final request = requests[id];
    if (request == null) {
      return;
    }

    final isStreamRequest = request.request.kind == _RequestKind.stream;
    final done = some['done'] == true;
    final hasError = some['error'] != null;
    if (!(isStreamRequest && !done && !hasError)) {
      requests.remove(id);
    }

    request.response(some);
  }

  Future<void> _streamRequest(
    int id,
    ChannelController<State> controller,
    String path,
    Object? body,
    Object? context,
    ChannelStreamHandler<State> stream,
  ) async {
    try {
      final values = await stream(
        ChannelBody<State>(
          body: body,
          sender: controller.sender,
          state: controller.state,
          path: path,
          context: context,
        ),
      );

      final done = Completer<void>();

      final subscription = values.listen(
        (Object? value) {
          controller.response(_responseWithId(id, body: value));
        },
        onError: (Object error, StackTrace _) {
          controller.response(_responseWithId(id, error: '$error'));
          if (!done.isCompleted) {
            done.complete();
          }
        },
        onDone: () {
          controller.response(_responseWithId(id, done: true));
          if (!done.isCompleted) {
            done.complete();
          }
        },
      );

      controller.sender.streams[id] = StreamCanceller.fromSubscription(
        subscription,
      );
      await done.future;
    } catch (error) {
      controller.response(_responseWithId(id, error: '$error'));
    } finally {
      controller.sender.streams.remove(id);
    }
  }

  Channel<State> events(Object events, {String prefix = ''}) {
    eventsApi ??= <String, Subscription>{};
    final map = eventsApi!;

    if (events is Map) {
      _parseEvents(events, prefix, map);
    }
    return this;
  }

  Channel<State> api(Map<String, Object?> api) {
    _parseApi(api, '');
    return this;
  }

  Channel<State> merge(Channel<State> channel, {String prefix = ''}) {
    final resolvedPrefix = prefix.isEmpty ? '' : '$prefix/';

    if (channel.eventsApi != null) {
      events(channel.eventsApi!, prefix: prefix);
    }

    channel.postApi.forEach((String key, ChannelPostHandler<State> value) {
      post('$resolvedPrefix$key', value);
    });
    channel.streamApi.forEach((String key, ChannelStreamHandler<State> value) {
      stream('$resolvedPrefix$key', value);
    });

    bool Function(String) scoped(bool Function(String) match) =>
        resolvedPrefix.isEmpty
        ? match
        : (String path) =>
              path.startsWith(resolvedPrefix) &&
              match(path.substring(resolvedPrefix.length));
    for (final route in channel.otherPostApi) {
      postOther(scoped(route.path), route.request);
    }
    for (final route in channel.otherStreamApi) {
      streamOther(scoped(route.path), route.request);
    }
    return this;
  }

  void _parseApi(Map<String, Object?> api, String prefix) {
    api.forEach((String key, Object? value) {
      if (value is ApiPost<State>) {
        final path = key == '_'
            ? prefix
            : (prefix.isEmpty ? key : '$prefix/$key');
        post(path, value.handler);
        return;
      }

      if (value is ApiStream<State>) {
        final path = key == '_'
            ? prefix
            : (prefix.isEmpty ? key : '$prefix/$key');
        stream(path, value.handler);
        return;
      }

      if (value is ChannelStreamHandler<State>) {
        final path = key == '_'
            ? prefix
            : (prefix.isEmpty ? key : '$prefix/$key');
        stream(path, value);
        return;
      }

      if (value is ChannelPostHandler<State>) {
        final path = key == '_'
            ? prefix
            : (prefix.isEmpty ? key : '$prefix/$key');
        post(path, value);
        return;
      }

      if (value is Map<String, Object?>) {
        _parseApi(value, prefix.isEmpty ? key : '$prefix/$key');
      } else if (value is Map) {
        final nested = <String, Object?>{};
        value.forEach((Object? nestedKey, Object? nestedValue) {
          if (nestedKey != null) {
            nested['$nestedKey'] = nestedValue;
          }
        });
        _parseApi(nested, prefix.isEmpty ? key : '$prefix/$key');
      }
    });
  }
}

class _PendingRequest<State> {
  _PendingRequest({required this.request, required this.response});

  final _RequestDescriptor<State> request;
  final void Function(Map<String, Object?> response) response;
}

class _RequestDescriptor<State> {
  _RequestDescriptor({
    required this.id,
    required this.kind,
    required this.sender,
  });

  final int id;
  final _RequestKind kind;
  final Sender<State>? sender;
}

enum _RequestKind { request, stream, subscription }

class _OtherPostApi<State> {
  _OtherPostApi({required this.path, required this.request});

  final bool Function(String path) path;
  final ChannelPostHandler<State> request;
}

class _OtherStreamApi<State> {
  _OtherStreamApi({required this.path, required this.request});

  final bool Function(String path) path;
  final ChannelStreamHandler<State> request;
}

Map<String, Object?>? _toMap(dynamic value) {
  if (value is Map<String, Object?>) {
    return value;
  }
  if (value is Map) {
    final map = <String, Object?>{};
    for (final entry in value.entries) {
      final key = entry.key;
      if (key is String) {
        map[key] = entry.value;
      }
    }
    return map;
  }
  return null;
}

int? _asInt(Object? value) {
  if (value is int) {
    return value;
  }
  if (value is num) {
    return value.toInt();
  }
  if (value is String) {
    return int.tryParse(value);
  }
  return null;
}

String _joinTopic(String prefix, String topic) {
  if (prefix.isEmpty) {
    return topic;
  }
  if (topic.isEmpty) {
    return prefix;
  }
  return '$prefix/$topic';
}

Map<String, Object?> _responseWithId(
  int id, {
  Object? topic,
  Object? body,
  Object? error,
  bool? done,
}) {
  final response = <String, Object?>{'id': id};
  if (topic != null) {
    response['topic'] = topic;
  }
  if (body != null) {
    response['body'] = body;
  }
  if (error != null) {
    response['error'] = error;
  }
  if (done != null) {
    response['done'] = done;
  }
  return response;
}

void _parseEvents(
  Map<dynamic, dynamic> object,
  String prefix,
  Map<String, Subscription> output,
) {
  object.forEach((dynamic keyValue, dynamic value) {
    if (keyValue == null) {
      return;
    }
    final key = '$keyValue';
    final path = prefix.isEmpty ? key : '$prefix/$key';
    if (value is Subscription) {
      value.prefix = path;
      output[path] = value;
      return;
    }
    if (value is Map<dynamic, dynamic>) {
      _parseEvents(value, path, output);
    }
  });
}

T _decode<T>(Object? value, Decode<T>? decode) {
  if (decode != null) {
    return decode(value);
  }
  if (value is T) {
    return value;
  }
  return value as T;
}

State defaultStateValue<State>() {
  if (null is State) {
    return null as State;
  }
  if (<String, Object?>{} is State) {
    return <String, Object?>{} as State;
  }
  throw StateError(
    'State of type $State requires explicit initial state. '
    'Provide `state:` in connect/listen/process options.',
  );
}
