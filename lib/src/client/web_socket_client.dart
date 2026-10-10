import 'dart:async';
import 'dart:convert';
import 'dart:io';

import '../core/channel_core.dart';
import '../json/json_engine.dart';

class WebSocketClient implements ConnectionInterface {
  WebSocketClient(
    this.address, {
    FutureOr<Map<String, String>> Function()? headers,
    JsonEngine? jsonEngine,
    this.reconnectDelay = const Duration(milliseconds: 100),
  }) : _headers = headers,
       _jsonEngine = jsonEngine ?? const DartJsonEngine() {
    unawaited(start());
  }

  final String address;
  final FutureOr<Map<String, String>> Function()? _headers;
  final JsonEngine _jsonEngine;
  final Duration reconnectDelay;

  Future<void> Function()? onOpen;
  void Function()? onClose;
  void Function(dynamic message)? onMessage;

  /// Called when the server sends a reply this client can't decode. Pending calls can't be matched to their
  /// reply any more, so the owner should settle them with this error instead of letting them wait forever.
  void Function(String error)? onProtocolError;

  final Map<int, Object?> pending = <int, Object?>{};

  int _id = 0;
  int _isWaiting = 0;
  int _isWaitingLength = 0;
  final List<Object?> _queue = <Object?>[];

  WebSocket? _ws;
  Timer? _reconnectTimer;
  bool _isStarting = false;
  bool isConnected = false;
  bool isRunning = true;

  final Map<String, Map<int, EventBody>> _subscribed =
      <String, Map<int, EventBody>>{};
  int _nextEventId = 0;

  Future<void> start() async {
    if (!isRunning || _isStarting) {
      return;
    }
    _isStarting = true;

    try {
      final headers = await _headers?.call();
      final socket = await WebSocket.connect(address, headers: headers);
      if (!isRunning) {
        await socket.close();
        return;
      }

      isConnected = true;
      _ws = socket;
      _isStarting = false;

      await onOpen?.call();

      if (pending.isNotEmpty) {
        _write(pending.values.toList(growable: false));
      }

      void handleDisconnected([Object? error]) {
        if (error != null && error is! SocketException) {
          // Protocol violation from the peer (invalid UTF-8, bad frame...): the pending calls would be
          // resent into the same failure forever.
          _reportProtocolError('invalid message from server');
        }
        _handleDisconnected();
      }

      socket.listen(
        (dynamic message) {
          dynamic decoded;
          try {
            if (message is String) {
              decoded = _jsonEngine.decode(message);
            } else if (message is List<int>) {
              decoded = _jsonEngine.decode(utf8.decode(message));
            } else {
              return;
            }
          } catch (_) {
            // One malformed reply must not kill the process (AUDIT2 §1.7), nor strand the pending calls.
            _reportProtocolError('invalid message from server');
            return;
          }
          try {
            onMessage?.call(decoded);
          } catch (_) {
            // Errors thrown by response/event callbacks stay local to that message.
          }
        },
        onError: handleDisconnected,
        onDone: handleDisconnected,
        cancelOnError: true,
      );
    } catch (error) {
      _isStarting = false;
      if (error is WebSocketException) {
        // The server answered but refused the upgrade (e.g. HTTP 500): fail queued calls, keep retrying.
        _reportProtocolError('connection rejected by server');
      }
      _scheduleReconnect();
    }
  }

  void _reportProtocolError(String error) {
    try {
      onProtocolError?.call(error);
    } catch (_) {}
  }

  void _handleDisconnected() {
    if (_ws == null && !isConnected) {
      return;
    }
    isConnected = false;
    _ws = null;
    onClose?.call();
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    if (!isRunning) {
      return;
    }
    _reconnectTimer?.cancel();
    _reconnectTimer = Timer(reconnectDelay, () {
      unawaited(start());
    });
  }

  void _write(Object? body) {
    final socket = _ws;
    if (socket == null) {
      return;
    }
    try {
      socket.add(_jsonEngine.encode(body));
    } catch (_) {}
  }

  @override
  int send(Object? body) {
    final id = _id++;
    pending[id] = body;

    if (_ws == null) {
      return id;
    }

    switch (_isWaiting) {
      case 0:
        _write(body);
        _isWaiting = 1;
        _isWaitingLength = 0;
        Timer(const Duration(milliseconds: 10), () {
          if (_isWaitingLength > 4000) {
            _isWaiting = 3;
          }
        });
      case 1:
        _write(body);
        _isWaitingLength += 1;
        if (_isWaitingLength > 4000) {
          _isWaiting = 3;
        }
      case 2:
        _queue.add(body);
      case 3:
        _write(body);
        _isWaiting = 2;
        Timer(const Duration(milliseconds: 1), () {
          _isWaiting = 3;
          if (_queue.isNotEmpty) {
            _write(_queue.toList(growable: false));
            _queue.clear();
          }
        });
    }

    return id;
  }

  @override
  bool cancel(int id) {
    if (isConnected) {
      return false;
    }
    if (!pending.containsKey(id)) {
      return false;
    }
    pending.remove(id);
    return true;
  }

  @override
  void notify(Object? body) {
    _write(body);
  }

  @override
  void sent(int id) {
    pending.remove(id);
  }

  @override
  bool Function() addTopic(String topic, EventBody event) {
    final id = ++_nextEventId;
    final map = _subscribed.putIfAbsent(topic, () => <int, EventBody>{});
    map[id] = event;

    return () {
      final handlers = _subscribed[topic];
      if (handlers == null) {
        return false;
      }
      handlers.remove(id);
      if (handlers.isNotEmpty) {
        return false;
      }
      _subscribed.remove(topic);
      return true;
    };
  }

  void receivedEvent(String topic, Object? event) {
    final handlers = _subscribed[topic];
    if (handlers == null || handlers.isEmpty) {
      return;
    }
    for (final listener in handlers.values.toList(growable: false)) {
      listener(event);
    }
  }

  void throttle() {
    _isWaiting = 3;
  }

  @override
  void stop() {
    if (!isRunning) {
      return;
    }
    isRunning = false;
    pending.clear();
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
    unawaited(_ws?.close());
    _ws = null;
    isConnected = false;
    // The socket's onDone is ignored once `_ws` is cleared, so report the close here (TS: stop -> onclose).
    onClose?.call();
  }
}
