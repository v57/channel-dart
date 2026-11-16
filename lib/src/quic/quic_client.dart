import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

import '../client/web_socket_client.dart';
import 'frames.dart';
import 'openssl.dart';
import 'poller.dart';

/// TLS settings for `quic://` addresses. Without [ca] or [fingerprint] the server certificate must be trusted
/// by the system's roots.
class QuicOptions {
  const QuicOptions({this.ca, this.fingerprint, this.servername});

  /// PEM certificate (or CA) to trust, for self-signed servers. The address must be in its subject
  /// alternative names.
  final String? ca;

  /// SHA-256 fingerprint of the server certificate (hex, colons optional) to pin instead of verifying it.
  final String? fingerprint;

  /// Name sent and checked instead of the address host.
  final String? servername;
}

/// The handshake didn't complete: unreachable, refused certificate, wrong ALPN, timeout.
class QuicConnectException implements Exception {
  QuicConnectException(this.message);

  final String message;

  @override
  String toString() => 'QUIC connect failed: $message';
}

/// The server broke the wire rules (a frame over the limit, a message that isn't UTF-8).
class QuicProtocolException implements Exception {
  QuicProtocolException(this.message);

  final String message;

  @override
  String toString() => message;
}

/// A [ClientDialer] for `quic://` addresses: each call dials one QUIC connection from its own UDP socket.
ClientDialer quicDialer(QuicOptions options, {int maxPayload = 16 * 1024 * 1024}) {
  // Checked once, up front: a bad fingerprint is the caller's mistake, not something to retry
  final pin = options.fingerprint == null ? null : parseQuicFingerprint(options.fingerprint!);
  return (String address, Map<String, String>? headers) async {
    final connection = _QuicClientConnection(address, options, pin, headers ?? const {}, maxPayload);
    await connection.open();
    return ClientLink(messages: connection.messages, add: connection.add, close: connection.close);
  };
}

/// A QUIC connection that sends raw bytes and no hello, for testing a server's wire rules. Messages are the
/// incoming frames as text; the stream ends when the server closes the connection.
final class RawQuicConnection {
  RawQuicConnection._(this._connection);

  final _QuicClientConnection _connection;

  static Future<RawQuicConnection> connect(String address, QuicOptions options) async {
    final pin = options.fingerprint == null ? null : parseQuicFingerprint(options.fingerprint!);
    final connection = _QuicClientConnection(address, options, pin, const {}, 16 * 1024 * 1024, raw: true);
    await connection.open();
    return RawQuicConnection._(connection);
  }

  Stream<dynamic> get messages => _connection.messages;

  /// Writes [bytes] as they are on the connection's stream.
  void write(List<int> bytes) => _connection.addRaw(bytes);

  /// Opens one more stream and writes [bytes] on it.
  void writeOnNewStream(List<int> bytes, {bool unidirectional = false}) =>
      _connection.addOnNewStream(bytes, unidirectional: unidirectional);

  /// Whether the server refused the stream [writeOnNewStream] opened ([index] counts them from 0).
  bool extraStreamRefused(int index) {
    final ossl = OpenSsl.instance;
    final stream = _connection._extraStreams[index];
    _connection._service(fromTick: false);
    final sending = ossl.getStreamWriteState(stream);
    return sending == sslStreamStateResetLocal ||
        sending == sslStreamStateResetRemote ||
        sending == sslStreamStateConnClosed ||
        ossl.getStreamReadState(stream) == sslStreamStateResetRemote;
  }

  void close() => _connection._closeNow();
}

enum _Phase { connecting, open, closed }

final class _QuicClientConnection {
  _QuicClientConnection(this.address, this.options, this.pin, this.headers, int maxPayload, {this.raw = false})
    : _reader = QuicFrameReader(maxPayload);

  final bool raw;
  final List<Ptr> _extraStreams = [];

  final String address;
  final QuicOptions options;
  final Uint8List? pin;
  final Map<String, String> headers;
  final QuicFrameReader _reader;
  bool get _readerFailed => _reader.failed;

  final OpenSsl _ossl = OpenSsl.instance;
  final StreamController<dynamic> _messages = StreamController<dynamic>();
  final Completer<void> _opened = Completer<void>();
  final NativeQueue _tx = NativeQueue();

  Stream<dynamic> get messages => _messages.stream;

  _Phase _phase = _Phase.connecting;
  Ptr _ctx = nullptr;
  Ptr _ssl = nullptr;
  Ptr _stream = nullptr;
  int _fd = -1;
  QuicPoller? _poller;
  Timer? _handshakeTimer;
  int _lastSentMs = 0;
  final Stopwatch _clock = Stopwatch()..start();

  // Reused native scratch space
  final Pointer<Uint8> _chunk = Native.alloc(65536);
  final Pointer<Size> _count = Native.alloc(8).cast<Size>();

  bool _servicing = false;
  bool _again = false;
  bool _scheduled = false;

  Future<void> open() async {
    try {
      _configure();
      _poller = await QuicPoller.start(_fd, _service);
    } catch (error) {
      _fail(error is QuicConnectException ? error : QuicConnectException('$error'));
      return _opened.future;
    }
    _handshakeTimer = Timer(const Duration(seconds: 10), () {
      if (_phase == _Phase.connecting) {
        _fail(QuicConnectException('handshake timed out'));
      }
    });
    _service();
    return _opened.future;
  }

  void _configure() {
    final target = parseQuicAddress(address);
    final serverName = (options.servername?.isNotEmpty ?? false) ? options.servername! : null;
    final byIp = serverName == null && InternetAddress.tryParse(target.host) != null;
    final checkName = serverName ?? target.host;

    _ctx = _ossl.ctxNew(_ossl.quicClientMethod());
    if (_ctx.address == 0) {
      throw QuicConnectException(_ossl.error('failed to create a quic context'));
    }
    if (pin != null) {
      // Pinning replaces the other checks: the fingerprint is compared after the handshake
      _ossl.ctxSetVerify(_ctx, sslVerifyNone, nullptr);
    } else {
      _ossl.ctxSetVerify(_ctx, sslVerifyPeer, nullptr);
      final ca = options.ca;
      if (ca != null && ca.isNotEmpty) {
        final certificates = _ossl.readCertificates(ca);
        if (certificates.isEmpty) {
          throw ArgumentError.value(ca, 'ca', 'has no PEM certificate');
        }
        final store = _ossl.ctxGetCertStore(_ctx);
        for (final certificate in certificates) {
          _ossl.storeAddCert(store, certificate);
          _ossl.x509Free(certificate);
        }
        _ossl.errClear();
      } else if (_ossl.ctxSetDefaultVerifyPaths(_ctx) != 1) {
        throw QuicConnectException(_ossl.error('failed to load the system certificates'));
      }
    }

    _ssl = _ossl.sslNew(_ctx);
    if (_ssl.address == 0) {
      throw QuicConnectException(_ossl.error('failed to create a quic connection'));
    }
    final name = Native.string(checkName);
    try {
      if (!byIp && _ossl.sslCtrl(_ssl, sslCtrlSetTlsextHostname, tlsextNametypeHostName, name.cast()) != 1) {
        throw QuicConnectException(_ossl.error('invalid server name'));
      }
      if (pin == null) {
        final set = byIp
            ? _ossl.paramSet1IpAsc(_ossl.get0Param(_ssl), name)
            : _ossl.set1Host(_ssl, name);
        if (set != 1) {
          throw QuicConnectException(_ossl.error('invalid server name'));
        }
      }
    } finally {
      Native.free(name);
    }
    final alpn = Native.bytes([quicAlpn.length, ...ascii.encode(quicAlpn)]);
    try {
      if (_ossl.setAlpnProtos(_ssl, alpn, quicAlpn.length + 1) != 0 ||
          _ossl.setValueUint(_ssl, sslValueClassFeatureRequest, sslValueQuicIdleTimeout, quicIdleTimeoutMs) != 1 ||
          _ossl.setDefaultStreamMode(_ssl, sslDefaultStreamModeNone) != 1 ||
          _ossl.setIncomingStreamPolicy(_ssl, sslIncomingStreamPolicyReject, 0) != 1 ||
          _ossl.setBlockingMode(_ssl, 0) != 1) {
        throw QuicConnectException(_ossl.error('failed to configure the quic connection'));
      }
    } finally {
      Native.free(alpn);
    }
    _socket(target.host, target.port);
  }

  /// A connected UDP socket of its own (one per connection, reconnects included).
  void _socket(String host, int port) {
    final hostText = Native.string(host);
    final portText = Native.string('$port');
    final results = Native.alloc(8).cast<Ptr>();
    try {
      if (_ossl.bioLookupEx(hostText, portText, bioLookupClient, afUnspec, sockDgram, ipprotoUdp, results) != 1) {
        throw QuicConnectException(_ossl.error('failed to resolve the quic host'));
      }
      final first = results.value;
      try {
        // IPv4 first: listeners on 0.0.0.0 don't answer on ::1
        var chosen = first;
        for (var info = first; info.address != 0; info = _ossl.addrinfoNext(info)) {
          if (_ossl.addrinfoFamily(info) == afInet) {
            chosen = info;
            break;
          }
        }
        final peer = _ossl.addrinfoAddress(chosen);
        final fd = _ossl.bioSocket(_ossl.addrinfoFamily(chosen), sockDgram, ipprotoUdp, 0);
        if (fd < 0) {
          throw QuicConnectException(_ossl.error('failed to open a udp socket'));
        }
        if (_ossl.bioConnect(fd, peer, 0) != 1 || _ossl.bioSocketNbio(fd, 1) != 1) {
          _ossl.bioCloseSocket(fd);
          throw QuicConnectException(_ossl.error('failed to open a udp socket'));
        }
        final bio = _ossl.bioNewDgram(fd, bioClose);
        if (bio.address == 0) {
          _ossl.bioCloseSocket(fd);
          throw QuicConnectException(_ossl.error('failed to open a udp socket'));
        }
        _fd = fd;
        _ossl.setBio(_ssl, bio, bio);
        if (_ossl.set1InitialPeerAddr(_ssl, peer) != 1) {
          throw QuicConnectException(_ossl.error('failed to set the quic peer address'));
        }
      } finally {
        _ossl.addrinfoFree(first);
      }
    } finally {
      Native.free(hostText);
      Native.free(portText);
      Native.free(results);
    }
  }

  void add(String text) {
    if (_phase == _Phase.closed) {
      return;
    }
    _tx.append(encodeQuicFrame(utf8.encode(text)));
    // Everything sent in one turn of the event loop goes out together
    if (!_scheduled) {
      _scheduled = true;
      scheduleMicrotask(() {
        _scheduled = false;
        _service(fromTick: false);
      });
    }
  }

  void addRaw(List<int> bytes) {
    _tx.append(bytes);
    _service(fromTick: false);
  }

  void addOnNewStream(List<int> bytes, {required bool unidirectional}) {
    final stream = _ossl.newStream(_ssl, sslStreamFlagNoBlock | (unidirectional ? 1 : 0));
    if (stream.address == 0) {
      throw StateError(_ossl.error('failed to open a quic stream'));
    }
    _extraStreams.add(stream);
    final queue = NativeQueue()..append(bytes);
    _ossl.flush(stream, queue, _count);
    queue.dispose();
    _service(fromTick: false);
  }

  Future<void> close() async {
    _closeNow();
  }

  void _service({bool fromTick = true}) {
    if (_phase == _Phase.closed) {
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
        _ossl.handleEvents(_ssl);
        if (_phase == _Phase.connecting) {
          _handshake();
        }
        if (_phase == _Phase.open) {
          _pump();
        }
      } while (_again && _phase != _Phase.closed);
    } finally {
      _servicing = false;
    }
    _arm(force: fromTick);
  }

  void _handshake() {
    final result = _ossl.connect(_ssl);
    if (result == 1) {
      _openStream();
      return;
    }
    final code = _ossl.getError(_ssl, result);
    if (code == sslErrorWantRead || code == sslErrorWantWrite) {
      _ossl.errClear();
      return;
    }
    final verify = _ossl.getVerifyResult(_ssl);
    if (verify != 0) {
      _ossl.errClear();
      _fail(QuicConnectException('certificate verify failed: ${Native.readString(_ossl.verifyErrorString(verify))}'));
    } else {
      _fail(QuicConnectException(_ossl.error('quic handshake failed')));
    }
  }

  bool _pinMatches() {
    final certificate = _ossl.get0PeerCertificate(_ssl);
    if (certificate.address == 0) {
      return false;
    }
    final digest = Native.alloc(64);
    final length = Native.alloc(4).cast<UnsignedInt>();
    try {
      if (_ossl.x509Digest(certificate, _ossl.sha256(), digest, length) != 1 || length.value != 32) {
        return false;
      }
      final got = digest.asTypedList(32);
      for (var index = 0; index < 32; index += 1) {
        if (got[index] != pin![index]) {
          return false;
        }
      }
      return true;
    } finally {
      Native.free(digest);
      Native.free(length);
    }
  }

  void _openStream() {
    if (pin != null && !_pinMatches()) {
      _fail(QuicConnectException('certificate fingerprint mismatch'));
      return;
    }
    _stream = _ossl.newStream(_ssl, sslStreamFlagNoBlock);
    if (_stream.address == 0) {
      _fail(QuicConnectException(_ossl.error('failed to open a quic stream')));
      return;
    }
    _ossl.sslCtrl(_stream, sslCtrlMode, sslModeEnablePartialWrite | sslModeAcceptMovingWriteBuffer, nullptr);
    // The hello goes first; nothing else can be queued before the link is handed out
    if (!raw) {
      _tx.append(encodeQuicHello(headers));
    }
    _phase = _Phase.open;
    _lastSentMs = _clock.elapsedMilliseconds;
    _handshakeTimer?.cancel();
    _opened.complete();
  }

  void _pump() {
    for (var iteration = 0; iteration < 32 && _phase == _Phase.open; iteration += 1) {
      if (!raw && _tx.length == 0 && _clock.elapsedMilliseconds - _lastSentMs >= quicKeepaliveMs) {
        _tx.append(const [0]);
      }
      final pending = _tx.length;
      if (pending > 0) {
        if (!_ossl.flush(_stream, _tx, _count)) {
          _disconnect();
          return;
        }
        if (_tx.length != pending) {
          _lastSentMs = _clock.elapsedMilliseconds;
        }
      }
      final data = _ossl.read(_stream, _chunk, 65536, _count);
      if (data == null) {
        _disconnect();
        return;
      }
      if (data.isEmpty) {
        // Data written above may have freed flow-control credit; one more round only if something moved
        if (_tx.length == pending || _tx.length == 0) {
          return;
        }
        continue;
      }
      List<Uint8List> frames;
      try {
        frames = _reader.push(data);
      } on QuicFrameTooLarge {
        _protocolError('message too big');
        return;
      }
      if (frames.isEmpty && _readerFailed) {
        _protocolError('message too big');
        return;
      }
      for (final frame in frames) {
        String text;
        try {
          text = utf8.decode(frame);
        } on FormatException {
          _protocolError('invalid message from server');
          return;
        }
        _messages.add(text);
      }
      if (_readerFailed) {
        _protocolError('message too big');
        return;
      }
    }
  }

  void _arm({required bool force}) {
    final poller = _poller;
    if (poller == null || _phase == _Phase.closed) {
      return;
    }
    var timeout = _ossl.eventTimeoutMs(_ssl);
    if (_phase == _Phase.open) {
      final keepalive = quicKeepaliveMs - (_clock.elapsedMilliseconds - _lastSentMs);
      timeout = timeout == null || keepalive < timeout ? keepalive : timeout;
    }
    poller.arm(timeout, wantWrite: _ossl.netWriteDesired(_ssl) == 1, force: force);
  }

  void _protocolError(String message) {
    _messages.addError(QuicProtocolException(message));
    _disconnect();
  }

  void _disconnect() {
    _closeNow();
  }

  void _fail(Object error) {
    _closeNow();
    if (!_opened.isCompleted) {
      _opened.completeError(error);
    }
  }

  /// Sends CONNECTION_CLOSE right away, without waiting for the peer, and frees the QUIC objects.
  void _closeNow() {
    if (_phase == _Phase.closed) {
      return;
    }
    _phase = _Phase.closed;
    _handshakeTimer?.cancel();
    _poller?.close();
    _poller = null;
    if (_stream.address != 0) {
      _ossl.flush(_stream, _tx, _count);
      _ossl.streamConclude(_stream, 0);
    }
    if (_ssl.address != 0) {
      // Queues CONNECTION_CLOSE; the next round of events sends it. The draining period isn't waited for.
      _ossl.shutdownEx(_ssl, sslShutdownFlagRapid | sslShutdownFlagNoBlock, nullptr, 0);
      _ossl.handleEvents(_ssl);
    }
    _ossl.errClear();
    _extraStreams.forEach(_ossl.sslFree);
    _extraStreams.clear();
    if (_stream.address != 0) {
      _ossl.sslFree(_stream);
    }
    if (_ssl.address != 0) {
      _ossl.sslFree(_ssl);
    } else if (_fd >= 0) {
      _ossl.bioCloseSocket(_fd);
    }
    if (_ctx.address != 0) {
      _ossl.ctxFree(_ctx);
    }
    _stream = nullptr;
    _ssl = nullptr;
    _ctx = nullptr;
    _tx.dispose();
    Native.free(_chunk);
    Native.free(_count);
    unawaited(_messages.close());
  }
}
