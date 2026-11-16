import 'dart:convert';
import 'dart:typed_data';

/// ALPN of the QUIC transport (`channel/docs/quic.md`).
const String quicAlpn = 'channel/1';

/// Thrown by [QuicFrameReader] when a frame's length is over its limit.
class QuicFrameTooLarge implements Exception {
  const QuicFrameTooLarge(this.length, this.limit);

  final int length;
  final int limit;

  @override
  String toString() => 'QUIC frame of $length bytes is over the $limit-byte limit';
}

/// One frame: the payload's length as a QUIC variable-length integer (RFC 9000 §16), then the payload.
Uint8List encodeQuicFrame(List<int> payload) {
  final length = payload.length;
  final header = length < 0x40
      ? 1
      : length < 0x4000
      ? 2
      : length < 0x40000000
      ? 4
      : 8;
  final out = Uint8List(header + length);
  var value = length;
  for (var index = header - 1; index >= 0; index -= 1) {
    out[index] = value & 0xff;
    value >>= 8;
  }
  out[0] |= switch (header) {
    1 => 0x00,
    2 => 0x40,
    4 => 0x80,
    _ => 0xc0,
  };
  out.setRange(header, header + length, payload);
  return out;
}

/// The first frame a client sends: `{"hello": headers}`.
Uint8List encodeQuicHello(Map<String, String> headers) {
  return encodeQuicFrame(utf8.encode(jsonEncode({'hello': headers})));
}

/// Headers from a hello frame, names lowercased (a later name that lowercases the same wins), string values
/// only. Null when [frame] isn't a hello.
Map<String, String>? parseQuicHello(Uint8List frame) {
  Object? decoded;
  try {
    decoded = jsonDecode(utf8.decode(frame));
  } catch (_) {
    return null;
  }
  if (decoded is! Map) {
    return null;
  }
  final hello = decoded['hello'];
  if (hello is! Map) {
    return null;
  }
  final headers = <String, String>{};
  hello.forEach((Object? name, Object? value) {
    if (name is String && value is String) {
      headers[name.toLowerCase()] = value;
    }
  });
  return headers;
}

/// Splits a QUIC stream back into frames. Zero-length frames are keepalives and are skipped.
class QuicFrameReader {
  QuicFrameReader([this.maxPayload = 16 * 1024 * 1024]);

  /// Largest payload accepted; a longer frame throws [QuicFrameTooLarge] as soon as its length is read.
  final int maxPayload;

  Uint8List _buffer = Uint8List(0);
  int _start = 0;
  int _end = 0;

  QuicFrameTooLarge? _tooLarge;

  /// A frame over the limit was seen: nothing more comes out of this reader.
  bool get failed => _tooLarge != null;

  /// Adds [data] and returns the frames it completed. A frame over the limit throws [QuicFrameTooLarge],
  /// after the frames before it were returned (on the next call when this one returns some).
  List<Uint8List> push(List<int> data) {
    final failed = _tooLarge;
    if (failed != null) {
      throw failed;
    }
    _append(data);
    final frames = <Uint8List>[];
    while (true) {
      final available = _end - _start;
      if (available == 0) {
        break;
      }
      final size = 1 << (_buffer[_start] >> 6);
      if (available < size) {
        break;
      }
      var length = _buffer[_start] & 0x3f;
      for (var index = 1; index < size; index += 1) {
        length = (length << 8) | _buffer[_start + index];
      }
      // Refused as soon as the length is known, before buffering the payload
      if (length > maxPayload || length < 0) {
        final error = _tooLarge = QuicFrameTooLarge(length, maxPayload);
        if (frames.isEmpty) {
          throw error;
        }
        return frames;
      }
      if (available - size < length) {
        break;
      }
      _start += size;
      if (length > 0) {
        frames.add(Uint8List.fromList(_buffer.sublist(_start, _start + length)));
        _start += length;
      }
    }
    if (_start == _end) {
      _start = 0;
      _end = 0;
    }
    return frames;
  }

  void _append(List<int> data) {
    if (data.isEmpty) {
      return;
    }
    if (_buffer.length - _end < data.length) {
      final used = _end - _start;
      var capacity = _buffer.isEmpty ? 4096 : _buffer.length;
      while (capacity - used < data.length) {
        capacity *= 2;
      }
      final grown = capacity == _buffer.length ? _buffer : Uint8List(capacity);
      grown.setRange(0, used, _buffer, _start);
      _buffer = grown;
      _start = 0;
      _end = used;
    }
    _buffer.setRange(_end, _end + data.length, data);
    _end += data.length;
  }
}
