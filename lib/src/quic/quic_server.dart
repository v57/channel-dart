import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

import '../core/channel_core.dart';
import '../json/json_engine.dart';
import '../server/frame_limit.dart';
import '../server/server_extension.dart';
import 'frames.dart';
import 'openssl.dart';
import 'poller.dart';

typedef _AlpnSelectNative =
    Int Function(Ptr, Pointer<Pointer<Uint8>>, Pointer<Uint8>, Pointer<Uint8>, UnsignedInt, Ptr);

/// Options of `listenQuic`: the certificate and key (PEM, required: QUIC has no plaintext mode) and the same
/// connection options as `listen`.
class QuicListenOptions<State> {
  const QuicListenOptions({
    required this.cert,
    required this.key,
    this.state,
    this.onConnect,
    this.onDisconnect,
    this.jsonEngine,
    this.maxMessageSize = defaultMaxMessageSize,
  });

  /// Certificate chain, leaf first.
  final String cert;
  final String key;

  /// Builds a connection's state from the client's hello headers (names lowercased).
  final FutureOr<State> Function(Map<String, String> headers)? state;
  final void Function(BodyContext<State> connection)? onConnect;
  final void Function(BodyContext<State> connection)? onDisconnect;
  final JsonEngine? jsonEngine;

  /// Largest incoming frame in bytes; a bigger one closes the connection. Default 16 MiB, like TS/Bun.
  final int maxMessageSize;
}

/// A running QUIC listener.
class QuicChannelServer<State> {
  QuicChannelServer._(this._server);

  final _QuicServer<State> _server;

  /// The UDP port it listens on (the real one when asked for port 0).
  int get port => _server.port;

  /// Open connections (hello received).
  int get connectionCount => _server.connections.where((c) => c.channelOpen).length;

  /// Closes every connection (CONNECTION_CLOSE, without waiting for the peers) and frees the port.
  Future<void> stop() => _server.stop();
}

Future<QuicChannelServer<State>> listenQuicServer<State>(
  Channel<State> channel,
  Object address,
  QuicListenOptions<State> options,
) async {
  var host = '127.0.0.1';
  int port;
  if (address is int) {
    port = address;
  } else if (address is String) {
    final colon = address.lastIndexOf(':');
    if (colon <= 0) {
      throw ArgumentError.value(address, 'address', 'must be `host:port`');
    }
    host = address.substring(0, colon);
    if (host.startsWith('[') && host.endsWith(']')) {
      host = host.substring(1, host.length - 1);
    }
    port = int.parse(address.substring(colon + 1));
  } else {
    throw ArgumentError.value(address, 'address', 'must be int or String');
  }
  if (port < 0 || port > 65535) {
    throw ArgumentError.value(address, 'address', 'port out of range');
  }
  final server = _QuicServer<State>(channel, options);
  await server.start(host, port);
  return QuicChannelServer<State>._(server);
}

final class _QuicServer<State> {
  _QuicServer(this.channel, this.options) : jsonEngine = options.jsonEngine ?? const DartJsonEngine();

  final Channel<State> channel;
  final QuicListenOptions<State> options;
  final JsonEngine jsonEngine;
  final OpenSsl ossl = OpenSsl.instance;
  final List<_QuicServerConnection<State>> connections = [];
  final Stopwatch clock = Stopwatch()..start();

  Ptr _ctx = nullptr;
  Ptr _listener = nullptr;
  QuicPoller? _poller;
  NativeCallable<_AlpnSelectNative>? _alpnCallback;
  Pointer<Uint8> _alpn = nullptr;
  int port = 0;
  bool running = false;
  _QuicPublisher<State>? _publisher;

  final Pointer<Uint8> chunk = Native.alloc(65536);
  final Pointer<Size> count = Native.alloc(8).cast<Size>();

  bool _servicing = false;
  bool _again = false;
  bool _scheduled = false;

  Future<void> start(String host, int requestedPort) async {
    try {
      _ctx = ossl.ctxNew(ossl.quicServerMethod());
      if (_ctx.address == 0) {
        throw StateError(ossl.error('failed to create a quic context'));
      }
      _loadIdentity();
      ossl.ctxSetVerify(_ctx, sslVerifyNone, nullptr);
      _alpn = Native.bytes([quicAlpn.length, ...ascii.encode(quicAlpn)]);
      _alpnCallback = NativeCallable<_AlpnSelectNative>.isolateLocal(_selectAlpn, exceptionalReturn: sslTlsextErrAlertFatal);
      ossl.ctxSetAlpnSelectCb(_ctx, _alpnCallback!.nativeFunction.cast(), nullptr);
      // OpenSSL's default max_idle_timeout is the protocol's 30 s.

      _listener = ossl.newListener(_ctx, 0);
      if (_listener.address == 0) {
        throw StateError(ossl.error('failed to create a quic listener'));
      }
      final fd = _bind(host, requestedPort);
      if (ossl.setBlockingMode(_listener, 0) != 1 || ossl.listen(_listener) != 1) {
        throw StateError(ossl.error('failed to listen'));
      }
      running = true;
      _poller = await QuicPoller.start(fd, service);
    } catch (_) {
      _free();
      rethrow;
    }

    final publisher = _QuicPublisher<State>(this);
    _publisher = publisher;
    channel.eventsApi?.forEach((String _, Subscription subscription) {
      subscription.publishers.add(publisher);
    });
    _arm(force: true);
  }

  int _selectAlpn(Ptr ssl, Pointer<Pointer<Uint8>> out, Pointer<Uint8> outLength, Pointer<Uint8> input, int inputLength, Ptr arg) {
    final result = ossl.selectNextProto(out, outLength, _alpn, quicAlpn.length + 1, input, inputLength);
    return result == opensslNpnNegotiated ? sslTlsextErrOk : sslTlsextErrAlertFatal;
  }

  void _loadIdentity() {
    final certificates = ossl.readCertificates(options.cert);
    if (certificates.isEmpty || ossl.ctxUseCertificate(_ctx, certificates.first) != 1) {
      certificates.forEach(ossl.x509Free);
      throw ArgumentError.value('<pem>', 'cert', 'invalid certificate');
    }
    ossl.x509Free(certificates.first);
    for (final chain in certificates.skip(1)) {
      // SSL_CTX_add0_chain_cert takes ownership
      if (ossl.ctxCtrl(_ctx, sslCtrlChainCert, 0, chain) != 1) {
        ossl.x509Free(chain);
      }
    }
    final key = ossl.readPrivateKey(options.key);
    final ok = key.address != 0 && ossl.ctxUsePrivateKey(_ctx, key) == 1 && ossl.ctxCheckPrivateKey(_ctx) == 1;
    if (key.address != 0) {
      ossl.pkeyFree(key);
    }
    ossl.errClear();
    if (!ok) {
      throw ArgumentError.value('<pem>', 'key', "invalid key or doesn't match the certificate");
    }
  }

  int _bind(String host, int requestedPort) {
    final hostText = Native.string(host);
    final portText = Native.string('$requestedPort');
    final results = Native.alloc(8).cast<Ptr>();
    try {
      if (ossl.bioLookupEx(hostText, portText, bioLookupServer, afUnspec, sockDgram, ipprotoUdp, results) != 1) {
        throw StateError(ossl.error('failed to resolve the listen address'));
      }
      final first = results.value;
      final fd = ossl.bioSocket(ossl.addrinfoFamily(first), sockDgram, ipprotoUdp, 0);
      final bound = fd >= 0 &&
          ossl.bioBind(fd, ossl.addrinfoAddress(first), bioSockReuseaddr) == 1 &&
          ossl.bioSocketNbio(fd, 1) == 1;
      ossl.addrinfoFree(first);
      if (!bound) {
        if (fd >= 0) {
          ossl.bioCloseSocket(fd);
        }
        ossl.errClear();
        throw StateError('failed to bind the quic server address $host:$requestedPort');
      }
      final local = ossl.addrNew();
      final info = Native.alloc(8).cast<Ptr>();
      info.value = local;
      if (ossl.bioSockInfo(fd, bioSockInfoAddress, info) == 1) {
        final raw = ossl.addrRawport(local);
        port = ((raw & 0xff) << 8) | (raw >> 8); // network byte order
      }
      Native.free(info);
      ossl.addrFree(local);
      if (port <= 0) {
        port = requestedPort;
      }
      final bio = ossl.bioNewDgram(fd, bioClose);
      if (bio.address == 0) {
        ossl.bioCloseSocket(fd);
        throw StateError(ossl.error('failed to open the quic socket'));
      }
      ossl.setBio(_listener, bio, bio);
      return fd;
    } finally {
      Native.free(hostText);
      Native.free(portText);
      Native.free(results);
    }
  }

  /// Runs [service] once this turn of the event loop is done (sends coalesce).
  void schedule() {
    if (_scheduled || !running) {
      return;
    }
    _scheduled = true;
    scheduleMicrotask(() {
      _scheduled = false;
      service(fromTick: false);
    });
  }

  void service({bool fromTick = true}) {
    if (!running) {
      return;
    }
    if (_servicing) {
      _again = true;
      return;
    }
    _servicing = true;
    try {
      do {
        _again = false;
        ossl.handleEvents(_listener);
        while (true) {
          final ssl = ossl.acceptConnection(_listener, sslAcceptConnectionNoBlock);
          if (ssl.address == 0) {
            break;
          }
          final connection = _QuicServerConnection<State>(this, ssl);
          if (ossl.setBlockingMode(ssl, 0) != 1 ||
              ossl.setDefaultStreamMode(ssl, sslDefaultStreamModeNone) != 1 ||
              ossl.setIncomingStreamPolicy(ssl, sslIncomingStreamPolicyAccept, 0) != 1) {
            connection.close();
          }
          connections.add(connection);
        }
        ossl.errClear();
        for (final connection in connections.toList(growable: false)) {
          if (!connection.closing) {
            connection.service();
          }
        }
        _reap();
      } while (_again && running);
    } finally {
      _servicing = false;
    }
    _arm(force: fromTick);
  }

  void _reap() {
    connections.removeWhere((connection) {
      if (!connection.closing) {
        return false;
      }
      connection.detach();
      final done = ossl.shutdownEx(connection.ssl, sslShutdownFlagNoBlock, nullptr, 0) != 0 ||
          clock.elapsedMilliseconds > connection.closingDeadline;
      ossl.errClear();
      if (done) {
        connection.free();
      }
      return done;
    });
  }

  void _arm({required bool force}) {
    final poller = _poller;
    if (poller == null || !running) {
      return;
    }
    var timeout = ossl.eventTimeoutMs(_listener);
    // Closing connections are checked again until they finish
    if (connections.any((c) => c.closing) && (timeout == null || timeout > 50)) {
      timeout = 50;
    }
    poller.arm(timeout, wantWrite: ossl.netWriteDesired(_listener) == 1, force: force);
  }

  Future<void> stop() async {
    if (!running) {
      return;
    }
    running = false;
    _poller?.close();
    _poller = null;
    final publisher = _publisher;
    if (publisher != null) {
      channel.eventsApi?.forEach((String _, Subscription subscription) {
        subscription.publishers.remove(publisher);
      });
    }
    // Every connection gets CONNECTION_CLOSE right away, the ones OpenSSL accepted but this server hasn't
    // taken yet included; then the port is freed. All in this turn of the event loop, so no client gets to
    // reconnect to a listener about to go.
    final unaccepted = <Ptr>[];
    while (true) {
      final ssl = ossl.acceptConnection(_listener, sslAcceptConnectionNoBlock);
      if (ssl.address == 0) {
        break;
      }
      unaccepted.add(ssl);
    }
    for (final connection in connections) {
      connection.detach();
    }
    final closing = [...connections.map((c) => c.ssl), ...unaccepted];
    for (final ssl in closing) {
      ossl.shutdownEx(ssl, sslShutdownFlagRapid | sslShutdownFlagNoBlock, nullptr, 0);
    }
    for (var round = 0; round < 3 && closing.isNotEmpty; round += 1) {
      ossl.handleEvents(_listener);
    }
    unaccepted.forEach(ossl.sslFree);
    for (final connection in connections) {
      connection.free();
    }
    connections.clear();
    _free();
  }

  void _free() {
    running = false;
    _poller?.close();
    _poller = null;
    ossl.errClear();
    if (_listener.address != 0) {
      ossl.sslFree(_listener);
      _listener = nullptr;
    }
    if (_ctx.address != 0) {
      ossl.ctxFree(_ctx);
      _ctx = nullptr;
    }
    _alpnCallback?.close();
    _alpnCallback = null;
    Native.free(_alpn);
    _alpn = nullptr;
  }
}

/// One QUIC connection: its first bidirectional stream carries the channel connection, which starts once the
/// hello frame arrives.
final class _QuicServerConnection<State> implements ConnectionInterface {
  _QuicServerConnection(this.server, this.ssl) : _reader = QuicFrameReader(server.options.maxMessageSize);

  final _QuicServer<State> server;
  final Ptr ssl;
  final QuicFrameReader _reader;
  bool get _readerFailed => _reader.failed;
  final NativeQueue _tx = NativeQueue();
  final Subscriptions subscriptions = Subscriptions();
  final Set<String> subscribedTopics = <String>{};
  Ptr _stream = nullptr;

  bool closing = false;
  int closingDeadline = 0;
  bool _helloSeen = false;

  /// Set once the hello's state is built; frames that arrive before wait in [_waiting].
  bool channelOpen = false;
  bool _detached = false;
  final List<Uint8List> _waiting = [];
  late State _state;
  late Sender<State> _sender;
  late BodyContext<State> _context;
  late ChannelController<State> _controller;

  OpenSsl get _ossl => server.ossl;

  void service() {
    _acceptStreams();
    if (_stream.address == 0) {
      // The client went away before opening its stream
      final info = Native.alloc(64);
      if (_ossl.getConnCloseInfo(ssl, info.cast(), 64) == 1) {
        close();
      }
      Native.free(info);
      _ossl.errClear();
      return;
    }
    for (var iteration = 0; iteration < 32 && !closing; iteration += 1) {
      final data = _ossl.read(_stream, server.chunk, 65536, server.count);
      if (data == null) {
        close();
        return;
      }
      if (data.isNotEmpty) {
        List<Uint8List> frames;
        try {
          frames = _reader.push(data);
        } on QuicFrameTooLarge {
          close();
          return;
        }
        for (final frame in frames) {
          if (closing) {
            return;
          }
          _frame(frame);
        }
        if (_readerFailed) {
          close();
          return;
        }
      }
      final pending = _tx.length;
      if (pending > 0 && !_ossl.flush(_stream, _tx, server.count)) {
        close();
        return;
      }
      if (data.isEmpty && _tx.length == pending) {
        break;
      }
    }
  }

  /// One stream per connection: the client's first bidirectional one. Later and unidirectional streams are
  /// refused.
  void _acceptStreams() {
    while (true) {
      final stream = _ossl.acceptStream(ssl, sslAcceptStreamNoBlock);
      if (stream.address == 0) {
        break;
      }
      if (_stream.address == 0 && _ossl.getStreamType(stream) == sslStreamTypeBidi) {
        _stream = stream;
        _ossl.sslCtrl(stream, sslCtrlMode, sslModeEnablePartialWrite | sslModeAcceptMovingWriteBuffer, nullptr);
        _ossl.setIncomingStreamPolicy(ssl, sslIncomingStreamPolicyReject, 0);
      } else {
        // OpenSSL can't lower the stream limits: any later stream is reset (as in channel-c and channel-zig);
        // the first one keeps working
        final args = Native.alloc(8);
        args.cast<Uint64>().value = 0;
        _ossl.streamReset(stream, args.cast(), 8);
        Native.free(args);
        _ossl.sslFree(stream);
      }
    }
    _ossl.errClear();
  }

  void _frame(Uint8List frame) {
    if (!_helloSeen) {
      _helloSeen = true;
      final headers = parseQuicHello(frame);
      if (headers == null) {
        close();
        return;
      }
      _open(headers);
      return;
    }
    if (!channelOpen) {
      _waiting.add(frame);
      return;
    }
    _receive(frame);
  }

  void _open(Map<String, String> headers) {
    final factory = server.options.state;
    FutureOr<State> state;
    try {
      state = factory == null ? defaultStateValue<State>() : factory(headers);
    } catch (_) {
      close();
      return;
    }
    if (state is Future<State>) {
      state.then(_start, onError: (Object _) => close());
    } else {
      _start(state);
    }
  }

  void _start(State state) {
    if (closing) {
      return;
    }
    _state = state;
    _sender = makeSender<State>(server.channel, this);
    _context = BodyContext<State>(state: state, sender: _sender, subscriptions: subscriptions);
    _controller = ChannelController<State>(
      response: (Map<String, Object?> body) => notify(body),
      subscribe: subscribedTopics.add,
      unsubscribe: subscribedTopics.remove,
      event: subscriptions.receivedEvent,
      sender: _sender,
      state: state,
    );
    channelOpen = true;
    server.options.onConnect?.call(_context);
    final waiting = List<Uint8List>.of(_waiting);
    _waiting.clear();
    for (final frame in waiting) {
      if (closing) {
        return;
      }
      _receive(frame);
    }
    server.schedule();
  }

  void _receive(Uint8List frame) {
    dynamic received;
    try {
      received = server.jsonEngine.decode(utf8.decode(frame));
    } catch (_) {
      // Invalid JSON closes the connection
      close();
      return;
    }
    try {
      server.channel.receive(received, _controller);
    } catch (_) {
      close();
    }
  }

  void sendText(String text) {
    if (closing) {
      return;
    }
    _tx.append(encodeQuicFrame(utf8.encode(text)));
    server.schedule();
  }

  @override
  int send(Object? body) {
    sendText(server.jsonEngine.encode(body));
    return 0;
  }

  @override
  void notify(Object? body) {
    sendText(server.jsonEngine.encode(body));
  }

  @override
  void sent(int id) {}

  @override
  bool cancel(int id) => false;

  @override
  bool Function() addTopic(String topic, EventBody event) => subscriptions.addTopic(topic, event);

  @override
  void stop() => close();

  /// Ends the connection: what the handlers already sent still goes out before CONNECTION_CLOSE.
  void close() {
    if (closing) {
      return;
    }
    closing = true;
    closingDeadline = server.clock.elapsedMilliseconds + 2000;
    if (_stream.address != 0) {
      _ossl.flush(_stream, _tx, server.count);
      _ossl.streamConclude(_stream, 0);
      _ossl.errClear();
    }
    server.schedule();
  }

  /// Ends the channel connection once: pending requests fail, onDisconnect runs.
  void detach() {
    if (_detached) {
      return;
    }
    _detached = true;
    if (!channelOpen) {
      return;
    }
    channelOpen = false;
    server.channel.disconnect(_state, _sender);
    server.options.onDisconnect?.call(_context);
  }

  void free() {
    detach();
    _ossl.errClear();
    if (_stream.address != 0) {
      _ossl.sslFree(_stream);
      _stream = nullptr;
    }
    _ossl.sslFree(ssl);
    _tx.dispose();
  }
}

class _QuicPublisher<State> implements SubscriptionPublisher {
  _QuicPublisher(this.server);

  final _QuicServer<State> server;

  @override
  void publish(SubscriptionEvent event) {
    String? encoded;
    for (final connection in server.connections) {
      if (connection.closing || !connection.channelOpen || !connection.subscribedTopics.contains(event.topic)) {
        continue;
      }
      connection.sendText(encoded ??= server.jsonEngine.encode(event.toJson()));
    }
  }
}
