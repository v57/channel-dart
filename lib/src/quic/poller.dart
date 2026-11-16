// Dart can't watch a file descriptor it didn't open, and OpenSSL owns the QUIC socket. So a helper isolate
// blocks in poll(2) on it and wakes the owner's isolate, which does all the OpenSSL work:
//
//   helper: poll(udp, pipe, timeout) -> tick to the owner -> wait for the owner's next message on the pipe
//   owner:  on tick, SSL_handle_events and the rest, then writes the next timeout to the pipe
//
// The owner also writes a new timeout whenever its own work (a send) moved the next deadline. Every message
// on the pipe is one int64: 0 for no timeout or the timeout in ms + 1, with bit 40 set when the socket must be
// writable; or [_quit].
import 'dart:async';
import 'dart:ffi';
import 'dart:isolate';

const int _quit = -2;
const int _wantWrite = 1 << 40;
const int _pollIn = 0x1;
const int _pollOut = 0x4;

final class _Libc {
  _Libc() : _lib = DynamicLibrary.process();

  final DynamicLibrary _lib;
  late final int Function(Pointer<Int32>, int, int) poll = _lib
      .lookupFunction<Int Function(Pointer<Int32>, UnsignedLong, Int), int Function(Pointer<Int32>, int, int)>('poll');
  late final int Function(Pointer<Int32>) pipe = _lib.lookupFunction<Int Function(Pointer<Int32>), int Function(Pointer<Int32>)>(
    'pipe',
  );
  late final int Function(int, Pointer<Int64>, int) read = _lib
      .lookupFunction<IntPtr Function(Int, Pointer<Int64>, Size), int Function(int, Pointer<Int64>, int)>('read');
  late final int Function(int, Pointer<Int64>, int) write = _lib
      .lookupFunction<IntPtr Function(Int, Pointer<Int64>, Size), int Function(int, Pointer<Int64>, int)>('write');
  late final int Function(int) close = _lib.lookupFunction<Int Function(Int), int Function(int)>('close');
  late final Pointer<Uint8> Function(int) malloc = _lib
      .lookupFunction<Pointer<Uint8> Function(Size), Pointer<Uint8> Function(int)>('malloc');
  late final void Function(Pointer<Uint8>) free = _lib
      .lookupFunction<Void Function(Pointer<Uint8>), void Function(Pointer<Uint8>)>('free');
}

/// Wakes [onTick] (on the creating isolate) when the UDP socket [fd] is readable or the timeout set with [arm]
/// passes.
final class QuicPoller {
  QuicPoller._(this._libc, this._writeFd, this._port, this._message);

  final _Libc _libc;
  final int _writeFd;
  final ReceivePort _port;
  final Pointer<Int64> _message;
  bool _closed = false;
  int? _armed;

  static Future<QuicPoller> start(int fd, void Function() onTick) async {
    final libc = _Libc();
    final fds = libc.malloc(8).cast<Int32>();
    if (libc.pipe(fds) != 0) {
      libc.free(fds.cast());
      throw StateError('pipe() failed');
    }
    final readFd = fds[0];
    final writeFd = fds[1];
    libc.free(fds.cast());

    final port = ReceivePort();
    final poller = QuicPoller._(libc, writeFd, port, libc.malloc(8).cast<Int64>());
    port.listen((Object? _) {
      if (!poller._closed) {
        onTick();
      }
    });
    await Isolate.spawn(_run, [fd, readFd, port.sendPort], debugName: 'channel-quic-poller');
    return poller;
  }

  /// Sets the next timeout ([timeoutMs] null: none) and acknowledges a tick. Repeating the last value is
  /// skipped unless [force].
  void arm(int? timeoutMs, {bool wantWrite = false, bool force = false}) {
    if (_closed) {
      return;
    }
    var value = timeoutMs == null ? 0 : (timeoutMs < 0 ? 0 : timeoutMs) + 1;
    if (wantWrite) {
      value |= _wantWrite;
    }
    if (!force && value == _armed) {
      return;
    }
    _armed = value;
    _send(value);
  }

  void _send(int value) {
    _message.value = value;
    _libc.write(_writeFd, _message, 8);
  }

  /// Stops the helper isolate. The socket stays open (OpenSSL closes it).
  void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    _send(_quit);
    // The helper closes the read end once it has seen _quit
    _libc.close(_writeFd);
    _libc.free(_message.cast());
    _port.close();
  }
}

void _run(List<Object?> args) {
  final fd = args[0]! as int;
  final readFd = args[1]! as int;
  final sendPort = args[2]! as SendPort;
  final libc = _Libc();
  final fds = libc.malloc(16).cast<Int32>();
  final message = libc.malloc(8).cast<Int64>();
  var timeout = -1;
  var wantWrite = false;

  // Reads one message; false on _quit or a closed pipe.
  bool receive() {
    if (libc.read(readFd, message, 8) != 8 || message.value < 0) {
      return false;
    }
    final value = message.value;
    wantWrite = value & _wantWrite != 0;
    final ms = value & (_wantWrite - 1);
    timeout = ms == 0 ? -1 : ms - 1;
    return true;
  }

  try {
    // The owner arms the first timeout once its SSL object exists
    if (!receive()) {
      return;
    }
    while (true) {
      // struct pollfd {int fd; short events; short revents;} x2
      fds[0] = fd;
      fds[1] = (wantWrite ? (_pollIn | _pollOut) : _pollIn);
      fds[2] = readFd;
      fds[3] = _pollIn;
      final ready = libc.poll(fds, 2, timeout > 0x7fffffff ? 0x7fffffff : timeout);
      if (ready < 0) {
        continue; // EINTR
      }
      final pipeReady = (fds[3] >> 16) & 0xffff;
      final socketReady = (fds[1] >> 16) & 0xffff;
      if (pipeReady != 0) {
        if (!receive()) {
          return;
        }
        if (socketReady == 0) {
          continue;
        }
      }
      // The socket is ready or the timeout passed: the owner works, then sends the next timeout
      sendPort.send(null);
      if (!receive()) {
        return;
      }
    }
  } finally {
    libc.close(readFd);
    libc.free(fds.cast());
    libc.free(message.cast());
  }
}
