import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

/// Default cap for one incoming WebSocket message (TS/Bun's default max payload).
const int defaultMaxMessageSize = 16 * 1024 * 1024;

/// Upgrades [request] to a WebSocket whose incoming frames are checked against [maxMessageSize].
///
/// `dart:io`'s `WebSocketTransformer` has no size limit: a peer could declare a 1 GB frame and keep the
/// connection buffering (AUDIT §3.1). This does the RFC 6455 handshake itself and puts a frame-header
/// inspector between the raw socket and `WebSocket.fromUpgradedSocket`. A frame or fragmented message
/// larger than the cap closes the connection with 1009 before its payload is buffered.
Future<WebSocket> upgradeWithFrameLimit(
  HttpRequest request, {
  int maxMessageSize = defaultMaxMessageSize,
}) async {
  final key = request.headers.value('sec-websocket-key');
  if (key == null) {
    throw const WebSocketException('missing Sec-WebSocket-Key');
  }
  final response = request.response;
  response.statusCode = HttpStatus.switchingProtocols;
  response.headers.add(HttpHeaders.connectionHeader, 'Upgrade');
  response.headers.add(HttpHeaders.upgradeHeader, 'websocket');
  response.headers.add('Sec-WebSocket-Accept', webSocketAcceptKey(key));
  response.contentLength = 0;
  final raw = await response.detachSocket();
  final limited = FrameLimitSocket(raw, maxMessageSize);
  final socket = WebSocket.fromUpgradedSocket(
    limited,
    serverSide: true,
    compression: CompressionOptions.compressionOff,
  );
  limited.onViolation = () {
    // Close frame 1009 goes out through the WebSocket's own sink; the peer's reply can't be read any more
    // (incoming bytes are no longer forwarded), so drop the TCP connection shortly after.
    unawaited(socket.close(WebSocketStatus.messageTooBig).catchError((_) {}));
    Timer(const Duration(milliseconds: 500), raw.destroy);
  };
  return socket;
}

/// `base64(sha1(key + GUID))` from RFC 6455 §4.2.2.
String webSocketAcceptKey(String key) {
  return base64.encode(
    _sha1(ascii.encode('${key.trim()}258EAFA5-E914-47DA-95CA-C5AB0DC85B11')),
  );
}

/// A [Socket] that forwards everything to [_socket] but watches incoming WebSocket frame headers.
class FrameLimitSocket extends Stream<Uint8List> implements Socket {
  FrameLimitSocket(this._socket, this.maxMessageSize);

  final Socket _socket;
  final int maxMessageSize;

  /// Called once when an incoming frame or message exceeds [maxMessageSize].
  void Function()? onViolation;

  final List<int> _header = <int>[];
  int _payloadLeft = 0;
  int _messageSize = 0;
  bool _violated = false;

  @override
  StreamSubscription<Uint8List> listen(
    void Function(Uint8List event)? onData, {
    Function? onError,
    void Function()? onDone,
    bool? cancelOnError,
  }) {
    return _socket
        .transform(
          StreamTransformer<Uint8List, Uint8List>.fromHandlers(
            handleData: (Uint8List data, EventSink<Uint8List> sink) {
              if (_violated) {
                return;
              }
              if (!_inspect(data)) {
                _violated = true;
                final callback = onViolation;
                if (callback != null) {
                  callback();
                } else {
                  _socket.destroy();
                }
                return;
              }
              sink.add(data);
            },
          ),
        )
        .listen(
          onData,
          onError: onError,
          onDone: onDone,
          cancelOnError: cancelOnError,
        );
  }

  /// Walks the frame headers in [data]; returns false once a frame/message exceeds the cap.
  bool _inspect(Uint8List data) {
    var offset = 0;
    while (offset < data.length) {
      if (_payloadLeft > 0) {
        final take = _payloadLeft < data.length - offset
            ? _payloadLeft
            : data.length - offset;
        _payloadLeft -= take;
        offset += take;
        continue;
      }

      _header.add(data[offset]);
      offset += 1;
      if (_header.length < 2) {
        continue;
      }
      final len7 = _header[1] & 0x7f;
      final masked = (_header[1] & 0x80) != 0;
      final extra = len7 == 126 ? 2 : (len7 == 127 ? 8 : 0);
      final needed = 2 + extra + (masked ? 4 : 0);
      if (_header.length < needed) {
        continue;
      }

      var length = len7;
      if (len7 == 126) {
        length = (_header[2] << 8) | _header[3];
      } else if (len7 == 127) {
        if (_header[2] & 0x80 != 0) {
          return false; // > 2^63-1: invalid per RFC 6455
        }
        length = 0;
        for (var i = 2; i < 10; i++) {
          length = (length << 8) | _header[i];
        }
      }

      final fin = (_header[0] & 0x80) != 0;
      final opcode = _header[0] & 0x0f;
      _header.clear();
      if (length > maxMessageSize) {
        return false;
      }
      if (opcode < 8) {
        _messageSize += length;
        if (_messageSize > maxMessageSize) {
          return false;
        }
        if (fin) {
          _messageSize = 0;
        }
      }
      _payloadLeft = length;
    }
    return true;
  }

  @override
  Encoding get encoding => _socket.encoding;

  @override
  set encoding(Encoding value) => _socket.encoding = value;

  @override
  void add(List<int> data) => _socket.add(data);

  @override
  void addError(Object error, [StackTrace? stackTrace]) =>
      _socket.addError(error, stackTrace);

  @override
  Future<void> addStream(Stream<List<int>> stream) => _socket.addStream(stream);

  @override
  Future<void> flush() => _socket.flush();

  @override
  Future<void> close() => _socket.close();

  @override
  Future<void> get done => _socket.done;

  @override
  void destroy() => _socket.destroy();

  @override
  void write(Object? object) => _socket.write(object);

  @override
  void writeAll(Iterable<Object?> objects, [String separator = '']) =>
      _socket.writeAll(objects, separator);

  @override
  void writeln([Object? object = '']) => _socket.writeln(object);

  @override
  void writeCharCode(int charCode) => _socket.writeCharCode(charCode);

  @override
  InternetAddress get address => _socket.address;

  @override
  InternetAddress get remoteAddress => _socket.remoteAddress;

  @override
  int get port => _socket.port;

  @override
  int get remotePort => _socket.remotePort;

  @override
  bool setOption(SocketOption option, bool enabled) =>
      _socket.setOption(option, enabled);

  @override
  Uint8List getRawOption(RawSocketOption option) =>
      _socket.getRawOption(option);

  @override
  void setRawOption(RawSocketOption option) => _socket.setRawOption(option);
}

Uint8List _sha1(List<int> message) {
  final ml = message.length * 8;
  final padded = <int>[...message, 0x80];
  while (padded.length % 64 != 56) {
    padded.add(0);
  }
  for (var i = 7; i >= 0; i--) {
    padded.add((ml >> (i * 8)) & 0xff);
  }

  var h0 = 0x67452301, h1 = 0xEFCDAB89, h2 = 0x98BADCFE;
  var h3 = 0x10325476, h4 = 0xC3D2E1F0;
  int rotl(int x, int n) => ((x << n) | (x >> (32 - n))) & 0xffffffff;
  final w = List<int>.filled(80, 0);

  for (var chunk = 0; chunk < padded.length; chunk += 64) {
    for (var i = 0; i < 16; i++) {
      final j = chunk + i * 4;
      w[i] =
          (padded[j] << 24) |
          (padded[j + 1] << 16) |
          (padded[j + 2] << 8) |
          padded[j + 3];
    }
    for (var i = 16; i < 80; i++) {
      w[i] = rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }
    var a = h0, b = h1, c = h2, d = h3, e = h4;
    for (var i = 0; i < 80; i++) {
      int f, k;
      if (i < 20) {
        f = (b & c) | ((~b) & d);
        k = 0x5A827999;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8F1BBCDC;
      } else {
        f = b ^ c ^ d;
        k = 0xCA62C1D6;
      }
      final temp = (rotl(a, 5) + (f & 0xffffffff) + e + k + w[i]) & 0xffffffff;
      e = d;
      d = c;
      c = rotl(b, 30);
      b = a;
      a = temp;
    }
    h0 = (h0 + a) & 0xffffffff;
    h1 = (h1 + b) & 0xffffffff;
    h2 = (h2 + c) & 0xffffffff;
    h3 = (h3 + d) & 0xffffffff;
    h4 = (h4 + e) & 0xffffffff;
  }

  final out = Uint8List(20);
  final view = ByteData.view(out.buffer);
  view.setUint32(0, h0);
  view.setUint32(4, h1);
  view.setUint32(8, h2);
  view.setUint32(12, h3);
  view.setUint32(16, h4);
  return out;
}
