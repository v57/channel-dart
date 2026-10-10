import '../core/channel_core.dart';

extension ChannelProcess<State> on Channel<State> {
  Sender<State> process({State? state}) {
    final connection = _ProcessConnection<State>(this);
    final sender = makeSender<State>(this, connection);
    final resolvedState = state ?? defaultStateValue<State>();
    void ignoreTopic(String _) {}

    late final ChannelController<State> controller;
    controller = ChannelController<State>(
      response: (Map<String, Object?> body) {
        receive(body, controller);
      },
      subscribe: ignoreTopic,
      unsubscribe: ignoreTopic,
      event: (String _, Object? _) {},
      sender: sender,
      state: resolvedState,
    );
    connection.controller = controller;
    return sender;
  }
}

class _ProcessConnection<State> implements ConnectionInterface {
  _ProcessConnection(this._channel);

  final Channel<State> _channel;
  ChannelController<State>? controller;

  @override
  int send(Object? body) {
    final current = controller;
    if (current != null) {
      _channel.receive(body, current);
    }
    return 0;
  }

  @override
  void sent(int id) {}

  @override
  bool cancel(int id) => false;

  @override
  void notify(Object? body) {
    final current = controller;
    if (current != null) {
      _channel.receive(body, current);
    }
  }

  @override
  bool Function() addTopic(String topic, EventBody event) {
    return () => false;
  }

  @override
  void stop() {
    final current = controller;
    if (current == null) {
      return;
    }
    controller = null;
    _channel.disconnect(current.state, current.sender);
  }
}
