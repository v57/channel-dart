import 'dart:async';

import '../core/channel_core.dart';
import '../json/json_engine.dart';
import 'web_socket_client.dart';

class ClientOptions<State> {
  const ClientOptions({
    this.headers,
    this.onConnect,
    this.state,
    this.jsonEngine,
  });

  final FutureOr<Map<String, String>> Function()? headers;
  final FutureOr<void> Function(Sender<State> sender)? onConnect;
  final State? state;
  final JsonEngine? jsonEngine;
}

class ClientSender<State> implements Sender<State> {
  ClientSender(this._sender, this.ws);

  final Sender<State> _sender;
  final WebSocketClient ws;

  @override
  Map<int, StreamCanceller> get streams => _sender.streams;

  @override
  Map<int, CancellableRequest> get requests => _sender.requests;

  @override
  Future<T> send<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    return _sender.send<T>(path, body: body, context: context, decode: decode);
  }

  @override
  SendingRequest<T> request<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    return _sender.request<T>(
      path,
      body: body,
      context: context,
      decode: decode,
    );
  }

  @override
  Stream<T> values<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
  }) {
    return _sender.values<T>(
      path,
      body: body,
      context: context,
      decode: decode,
    );
  }

  @override
  Future<Cancellable> subscribe<T>(
    String path, {
    Object? body,
    Object? context,
    Decode<T>? decode,
    required void Function(T event) onEvent,
  }) {
    return _sender.subscribe<T>(
      path,
      body: body,
      context: context,
      decode: decode,
      onEvent: onEvent,
    );
  }

  @override
  void stop() {
    _sender.stop();
  }
}

extension ChannelClient<State> on Channel<State> {
  ClientSender<State> connect(Object address, {ClientOptions<State>? options}) {
    final clientOptions = options ?? ClientOptions<State>();
    final resolvedAddress = switch (address) {
      int port => 'ws://127.0.0.1:$port',
      String value => value,
      _ => throw ArgumentError.value(
        address,
        'address',
        'must be int or String',
      ),
    };

    final ws = WebSocketClient(
      resolvedAddress,
      headers: clientOptions.headers,
      jsonEngine: clientOptions.jsonEngine,
    );

    final topics = <String>{};
    final sender = makeSender<State>(this, ws);
    final state = clientOptions.state ?? defaultStateValue<State>();

    final onConnect = clientOptions.onConnect;
    if (onConnect != null) {
      ws.onOpen = () async {
        await onConnect(sender);
      };
    }

    final controller = ChannelController<State>(
      response: (Map<String, Object?> body) {
        ws.notify(body);
      },
      subscribe: (String topic) {
        topics.add(topic);
      },
      unsubscribe: (String topic) {
        topics.remove(topic);
      },
      event: (String topic, Object? body) {
        ws.receivedEvent(topic, body);
      },
      sender: sender,
      state: state,
    );

    final publishers = <_ClientPublisherBinding>[];
    var publishersRemoved = false;

    void cleanupPublishers() {
      if (publishersRemoved) {
        return;
      }
      publishersRemoved = true;
      for (final binding in publishers) {
        binding.subscription.publishers.remove(binding.publisher);
      }
      publishers.clear();
    }

    ws.onClose = () {
      if (!ws.isRunning) {
        cleanupPublishers();
      }
      disconnect(state, sender);
    };

    ws.onMessage = (dynamic message) {
      receive(message, controller);
    };

    eventsApi?.forEach((String _, Subscription subscription) {
      final publisher = _ClientPublisher(topics: topics, ws: ws);
      subscription.publishers.add(publisher);
      publishers.add(
        _ClientPublisherBinding(
          subscription: subscription,
          publisher: publisher,
        ),
      );
    });

    return ClientSender<State>(sender, ws);
  }
}

class _ClientPublisherBinding {
  _ClientPublisherBinding({
    required this.subscription,
    required this.publisher,
  });

  final Subscription subscription;
  final _ClientPublisher publisher;
}

class _ClientPublisher implements SubscriptionPublisher {
  _ClientPublisher({required this.topics, required this.ws});

  final Set<String> topics;
  final WebSocketClient ws;

  @override
  void publish(SubscriptionEvent event) {
    if (!topics.contains(event.topic)) {
      return;
    }
    ws.notify(event.toJson());
  }
}
