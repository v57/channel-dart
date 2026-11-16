// dart:ffi bindings to OpenSSL 3.5's native QUIC (the API channel-c and channel-zig use). Only
// package:channel_dart/quic.dart reaches this file, so a WebSocket-only program never loads OpenSSL.
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';
import 'dart:typed_data';

typedef Ptr = Pointer<Void>;

// From OpenSSL 3.5's headers (checked by test/quic/openssl_constants_test.dart).
const int sslErrorWantRead = 2;
const int sslErrorWantWrite = 3;
const int sslVerifyNone = 0;
const int sslVerifyPeer = 1;
const int sslStreamFlagNoBlock = 2;
const int sslAcceptStreamNoBlock = 1;
const int sslAcceptConnectionNoBlock = 1;
const int sslModeEnablePartialWrite = 1;
const int sslModeAcceptMovingWriteBuffer = 2;
const int sslCtrlMode = 33;
const int sslCtrlSetTlsextHostname = 55;
const int tlsextNametypeHostName = 0;
const int sslCtrlChainCert = 89;
const int sslDefaultStreamModeNone = 0;
const int sslIncomingStreamPolicyAccept = 1;
const int sslIncomingStreamPolicyReject = 2;
const int sslValueClassFeatureRequest = 1;
const int sslValueQuicIdleTimeout = 5;
const int sslShutdownFlagRapid = 1;
const int sslShutdownFlagNoBlock = 4;
const int sslStreamTypeBidi = 3;
const int sslStreamStateResetLocal = 4;
const int sslStreamStateResetRemote = 5;
const int sslStreamStateConnClosed = 6;
const int sslTlsextErrOk = 0;
const int sslTlsextErrAlertFatal = 2;
const int opensslNpnNegotiated = 1;
const int bioClose = 1;
const int bioLookupClient = 0;
const int bioLookupServer = 1;
const int bioSockReuseaddr = 1;
const int bioSockInfoAddress = 0;
const int afUnspec = 0;
const int afInet = 2;
const int sockDgram = 2;
const int ipprotoUdp = 17;

/// The QUIC idle timeout both sides set, and the client's keepalive interval.
const int quicIdleTimeoutMs = 30000;
const int quicKeepaliveMs = 15000;

/// libc's malloc/free, so this file needs no package:ffi.
final class Native {
  Native._();

  static final DynamicLibrary _libc = DynamicLibrary.process();
  static final Pointer<Uint8> Function(int) _malloc = _libc
      .lookupFunction<Pointer<Uint8> Function(Size), Pointer<Uint8> Function(int)>('malloc');
  static final void Function(Pointer<Void>) _free = _libc
      .lookupFunction<Void Function(Pointer<Void>), void Function(Pointer<Void>)>('free');

  static Pointer<Uint8> alloc(int size) {
    final pointer = _malloc(size < 1 ? 1 : size);
    if (pointer.address == 0) {
      throw StateError('out of memory');
    }
    return pointer;
  }

  static void free(Pointer pointer) {
    if (pointer.address != 0) {
      _free(pointer.cast());
    }
  }

  /// A NUL-terminated copy of [text] (free it).
  static Pointer<Uint8> string(String text) {
    final bytes = utf8.encode(text);
    final pointer = alloc(bytes.length + 1);
    pointer.asTypedList(bytes.length + 1)
      ..setRange(0, bytes.length, bytes)
      ..[bytes.length] = 0;
    return pointer;
  }

  static Pointer<Uint8> bytes(List<int> data) {
    final pointer = alloc(data.length);
    pointer.asTypedList(data.length).setRange(0, data.length, data);
    return pointer;
  }

  static String readString(Pointer<Uint8> pointer) {
    if (pointer.address == 0) {
      return '';
    }
    var length = 0;
    while (pointer[length] != 0) {
      length += 1;
    }
    return utf8.decode(pointer.asTypedList(length), allowMalformed: true);
  }
}

/// A byte queue in native memory: appended at the end, written to OpenSSL from the front.
final class NativeQueue {
  Pointer<Uint8> _data = Pointer.fromAddress(0);
  int _capacity = 0;
  int _start = 0;
  int _end = 0;

  int get length => _end - _start;
  Pointer<Uint8> get front => _data + _start;

  void append(List<int> bytes) {
    if (bytes.isEmpty) {
      return;
    }
    if (_capacity - _end < bytes.length) {
      final used = _end - _start;
      var capacity = _capacity == 0 ? 4096 : _capacity;
      while (capacity - used < bytes.length) {
        capacity *= 2;
      }
      final grown = Native.alloc(capacity);
      if (used > 0) {
        grown.asTypedList(used).setRange(0, used, _data.asTypedList(_end), _start);
      }
      Native.free(_data);
      _data = grown;
      _capacity = capacity;
      _start = 0;
      _end = used;
    }
    _data.asTypedList(_capacity).setRange(_end, _end + bytes.length, bytes);
    _end += bytes.length;
  }

  void consume(int count) {
    _start += count;
    if (_start >= _end) {
      _start = 0;
      _end = 0;
    }
  }

  void clear() {
    _start = 0;
    _end = 0;
  }

  void dispose() {
    Native.free(_data);
    _data = Pointer.fromAddress(0);
    _capacity = 0;
    clear();
  }
}

/// Thrown when OpenSSL 3.5 (libssl and libcrypto) can't be loaded.
class QuicUnavailable implements Exception {
  QuicUnavailable(this.message);

  final String message;

  @override
  String toString() => 'QUIC is unavailable: $message';
}

DynamicLibrary _open(String variable, List<String> candidates) {
  final override = Platform.environment[variable];
  final names = [?override, ...candidates];
  final errors = <String>[];
  for (final name in names) {
    try {
      return DynamicLibrary.open(name);
    } catch (error) {
      errors.add('$name: $error');
    }
  }
  throw QuicUnavailable('OpenSSL 3.5+ not found (set $variable). ${errors.join('; ')}');
}

List<String> _candidates(String name) {
  if (Platform.isMacOS) {
    return [
      '/opt/homebrew/opt/openssl@3/lib/lib$name.3.dylib',
      '/usr/local/opt/openssl@3/lib/lib$name.3.dylib',
      'lib$name.3.dylib',
    ];
  }
  if (Platform.isWindows) {
    return ['lib$name-3-x64.dll', 'lib$name-3-arm64.dll', 'lib$name-3.dll'];
  }
  return ['lib$name.so.3', 'lib$name.so'];
}

/// The OpenSSL functions the QUIC transport calls, loaded on first use.
final class OpenSsl {
  OpenSsl._(this._ssl, this._crypto) {
    final version = _crypto.lookupFunction<UnsignedLong Function(), int Function()>('OpenSSL_version_num')();
    // 0xMNN00PP0L: QUIC servers (SSL_new_listener) need 3.5
    if (version < 0x30500000) {
      throw QuicUnavailable('OpenSSL ${(version >> 28) & 0xf}.${(version >> 20) & 0xff} is too old: QUIC needs 3.5+');
    }
  }

  static OpenSsl? _instance;

  static OpenSsl get instance {
    return _instance ??= OpenSsl._(
      _open('CHANNEL_QUIC_LIBSSL', _candidates('ssl')),
      _open('CHANNEL_QUIC_LIBCRYPTO', _candidates('crypto')),
    );
  }

  final DynamicLibrary _ssl;
  final DynamicLibrary _crypto;

  // libssl
  late final Ptr Function() quicClientMethod = _ssl.lookupFunction<Ptr Function(), Ptr Function()>(
    'OSSL_QUIC_client_method',
  );
  late final Ptr Function() quicServerMethod = _ssl.lookupFunction<Ptr Function(), Ptr Function()>(
    'OSSL_QUIC_server_method',
  );
  late final Ptr Function(Ptr) ctxNew = _ssl.lookupFunction<Ptr Function(Ptr), Ptr Function(Ptr)>('SSL_CTX_new');
  late final void Function(Ptr) ctxFree = _ssl.lookupFunction<Void Function(Ptr), void Function(Ptr)>(
    'SSL_CTX_free',
  );
  late final void Function(Ptr, int, Ptr) ctxSetVerify = _ssl
      .lookupFunction<Void Function(Ptr, Int, Ptr), void Function(Ptr, int, Ptr)>('SSL_CTX_set_verify');
  late final Ptr Function(Ptr) ctxGetCertStore = _ssl.lookupFunction<Ptr Function(Ptr), Ptr Function(Ptr)>(
    'SSL_CTX_get_cert_store',
  );
  late final int Function(Ptr) ctxSetDefaultVerifyPaths = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>(
    'SSL_CTX_set_default_verify_paths',
  );
  late final int Function(Ptr, Ptr) ctxUseCertificate = _ssl
      .lookupFunction<Int Function(Ptr, Ptr), int Function(Ptr, Ptr)>('SSL_CTX_use_certificate');
  late final int Function(Ptr, Ptr) ctxUsePrivateKey = _ssl
      .lookupFunction<Int Function(Ptr, Ptr), int Function(Ptr, Ptr)>('SSL_CTX_use_PrivateKey');
  late final int Function(Ptr) ctxCheckPrivateKey = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>(
    'SSL_CTX_check_private_key',
  );
  late final int Function(Ptr, int, int, Ptr) ctxCtrl = _ssl
      .lookupFunction<Long Function(Ptr, Int, Long, Ptr), int Function(Ptr, int, int, Ptr)>('SSL_CTX_ctrl');
  late final void Function(Ptr, Ptr, Ptr) ctxSetAlpnSelectCb = _ssl
      .lookupFunction<Void Function(Ptr, Ptr, Ptr), void Function(Ptr, Ptr, Ptr)>('SSL_CTX_set_alpn_select_cb');
  late final int Function(Pointer<Pointer<Uint8>>, Pointer<Uint8>, Pointer<Uint8>, int, Pointer<Uint8>, int)
  selectNextProto = _ssl
      .lookupFunction<
        Int Function(Pointer<Pointer<Uint8>>, Pointer<Uint8>, Pointer<Uint8>, UnsignedInt, Pointer<Uint8>, UnsignedInt),
        int Function(Pointer<Pointer<Uint8>>, Pointer<Uint8>, Pointer<Uint8>, int, Pointer<Uint8>, int)
      >('SSL_select_next_proto');

  late final Ptr Function(Ptr) sslNew = _ssl.lookupFunction<Ptr Function(Ptr), Ptr Function(Ptr)>('SSL_new');
  late final void Function(Ptr) sslFree = _ssl.lookupFunction<Void Function(Ptr), void Function(Ptr)>('SSL_free');
  late final int Function(Ptr, int, int, Ptr) sslCtrl = _ssl
      .lookupFunction<Long Function(Ptr, Int, Long, Ptr), int Function(Ptr, int, int, Ptr)>('SSL_ctrl');
  late final int Function(Ptr, Pointer<Uint8>) set1Host = _ssl
      .lookupFunction<Int Function(Ptr, Pointer<Uint8>), int Function(Ptr, Pointer<Uint8>)>('SSL_set1_host');
  late final Ptr Function(Ptr) get0Param = _ssl.lookupFunction<Ptr Function(Ptr), Ptr Function(Ptr)>(
    'SSL_get0_param',
  );
  late final int Function(Ptr, Pointer<Uint8>, int) setAlpnProtos = _ssl
      .lookupFunction<Int Function(Ptr, Pointer<Uint8>, UnsignedInt), int Function(Ptr, Pointer<Uint8>, int)>(
        'SSL_set_alpn_protos',
      );
  late final int Function(Ptr, int, int, int) setValueUint = _ssl
      .lookupFunction<Int Function(Ptr, Uint32, Uint32, Uint64), int Function(Ptr, int, int, int)>(
        'SSL_set_value_uint',
      );
  late final int Function(Ptr, int) setDefaultStreamMode = _ssl
      .lookupFunction<Int Function(Ptr, Uint32), int Function(Ptr, int)>('SSL_set_default_stream_mode');
  late final int Function(Ptr, int, int) setIncomingStreamPolicy = _ssl
      .lookupFunction<Int Function(Ptr, Int, Uint64), int Function(Ptr, int, int)>('SSL_set_incoming_stream_policy');
  late final int Function(Ptr, int) setBlockingMode = _ssl.lookupFunction<Int Function(Ptr, Int), int Function(Ptr, int)>(
    'SSL_set_blocking_mode',
  );
  late final void Function(Ptr, Ptr, Ptr) setBio = _ssl
      .lookupFunction<Void Function(Ptr, Ptr, Ptr), void Function(Ptr, Ptr, Ptr)>('SSL_set_bio');
  late final int Function(Ptr, Ptr) set1InitialPeerAddr = _ssl
      .lookupFunction<Int Function(Ptr, Ptr), int Function(Ptr, Ptr)>('SSL_set1_initial_peer_addr');
  late final int Function(Ptr) connect = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>('SSL_connect');
  late final int Function(Ptr, int) getError = _ssl.lookupFunction<Int Function(Ptr, Int), int Function(Ptr, int)>(
    'SSL_get_error',
  );
  late final int Function(Ptr) getVerifyResult = _ssl.lookupFunction<Long Function(Ptr), int Function(Ptr)>(
    'SSL_get_verify_result',
  );
  late final Ptr Function(Ptr) get0PeerCertificate = _ssl.lookupFunction<Ptr Function(Ptr), Ptr Function(Ptr)>(
    'SSL_get0_peer_certificate',
  );
  late final Ptr Function(Ptr, int) newStream = _ssl.lookupFunction<Ptr Function(Ptr, Uint64), Ptr Function(Ptr, int)>(
    'SSL_new_stream',
  );
  late final int Function(Ptr, Pointer<Uint8>, int, Pointer<Size>) writeEx = _ssl
      .lookupFunction<
        Int Function(Ptr, Pointer<Uint8>, Size, Pointer<Size>),
        int Function(Ptr, Pointer<Uint8>, int, Pointer<Size>)
      >('SSL_write_ex');
  late final int Function(Ptr, Pointer<Uint8>, int, Pointer<Size>) readEx = _ssl
      .lookupFunction<
        Int Function(Ptr, Pointer<Uint8>, Size, Pointer<Size>),
        int Function(Ptr, Pointer<Uint8>, int, Pointer<Size>)
      >('SSL_read_ex');
  late final int Function(Ptr) handleEvents = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>(
    'SSL_handle_events',
  );
  late final int Function(Ptr, Pointer<Uint8>, Pointer<Int>) getEventTimeout = _ssl
      .lookupFunction<Int Function(Ptr, Pointer<Uint8>, Pointer<Int>), int Function(Ptr, Pointer<Uint8>, Pointer<Int>)>(
        'SSL_get_event_timeout',
      );
  late final int Function(Ptr) netWriteDesired = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>(
    'SSL_net_write_desired',
  );
  late final int Function(Ptr, int, Ptr, int) shutdownEx = _ssl
      .lookupFunction<Int Function(Ptr, Uint64, Ptr, Size), int Function(Ptr, int, Ptr, int)>('SSL_shutdown_ex');
  late final int Function(Ptr, int) streamConclude = _ssl.lookupFunction<Int Function(Ptr, Uint64), int Function(Ptr, int)>(
    'SSL_stream_conclude',
  );
  late final int Function(Ptr, Ptr, int) streamReset = _ssl
      .lookupFunction<Int Function(Ptr, Ptr, Size), int Function(Ptr, Ptr, int)>('SSL_stream_reset');
  late final Ptr Function(Ptr, int) acceptStream = _ssl.lookupFunction<Ptr Function(Ptr, Uint64), Ptr Function(Ptr, int)>(
    'SSL_accept_stream',
  );
  late final int Function(Ptr) getStreamType = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>(
    'SSL_get_stream_type',
  );
  late final int Function(Ptr) getStreamWriteState = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>(
    'SSL_get_stream_write_state',
  );
  late final int Function(Ptr) getStreamReadState = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>(
    'SSL_get_stream_read_state',
  );
  late final int Function(Ptr, Ptr, int) getConnCloseInfo = _ssl
      .lookupFunction<Int Function(Ptr, Ptr, Size), int Function(Ptr, Ptr, int)>('SSL_get_conn_close_info');
  late final Ptr Function(Ptr, int) newListener = _ssl.lookupFunction<Ptr Function(Ptr, Uint64), Ptr Function(Ptr, int)>(
    'SSL_new_listener',
  );
  late final int Function(Ptr) listen = _ssl.lookupFunction<Int Function(Ptr), int Function(Ptr)>('SSL_listen');
  late final Ptr Function(Ptr, int) acceptConnection = _ssl
      .lookupFunction<Ptr Function(Ptr, Uint64), Ptr Function(Ptr, int)>('SSL_accept_connection');

  // libcrypto
  late final void Function() errClear = _crypto.lookupFunction<Void Function(), void Function()>('ERR_clear_error');
  late final int Function() errPeekLast = _crypto.lookupFunction<UnsignedLong Function(), int Function()>(
    'ERR_peek_last_error',
  );
  late final void Function(int, Pointer<Uint8>, int) errString = _crypto
      .lookupFunction<Void Function(UnsignedLong, Pointer<Uint8>, Size), void Function(int, Pointer<Uint8>, int)>(
        'ERR_error_string_n',
      );
  late final Ptr Function(Pointer<Uint8>, int) bioNewMemBuf = _crypto
      .lookupFunction<Ptr Function(Pointer<Uint8>, Int), Ptr Function(Pointer<Uint8>, int)>('BIO_new_mem_buf');
  late final int Function(Ptr) bioFree = _crypto.lookupFunction<Int Function(Ptr), int Function(Ptr)>('BIO_free');
  late final Ptr Function(Ptr, Ptr, Ptr, Ptr) pemReadX509 = _crypto
      .lookupFunction<Ptr Function(Ptr, Ptr, Ptr, Ptr), Ptr Function(Ptr, Ptr, Ptr, Ptr)>('PEM_read_bio_X509');
  late final Ptr Function(Ptr, Ptr, Ptr, Ptr) pemReadPrivateKey = _crypto
      .lookupFunction<Ptr Function(Ptr, Ptr, Ptr, Ptr), Ptr Function(Ptr, Ptr, Ptr, Ptr)>('PEM_read_bio_PrivateKey');
  late final void Function(Ptr) x509Free = _crypto.lookupFunction<Void Function(Ptr), void Function(Ptr)>('X509_free');
  late final void Function(Ptr) pkeyFree = _crypto.lookupFunction<Void Function(Ptr), void Function(Ptr)>(
    'EVP_PKEY_free',
  );
  late final int Function(Ptr, Ptr) storeAddCert = _crypto
      .lookupFunction<Int Function(Ptr, Ptr), int Function(Ptr, Ptr)>('X509_STORE_add_cert');
  late final Ptr Function() sha256 = _crypto.lookupFunction<Ptr Function(), Ptr Function()>('EVP_sha256');
  late final int Function(Ptr, Ptr, Pointer<Uint8>, Pointer<UnsignedInt>) x509Digest = _crypto
      .lookupFunction<
        Int Function(Ptr, Ptr, Pointer<Uint8>, Pointer<UnsignedInt>),
        int Function(Ptr, Ptr, Pointer<Uint8>, Pointer<UnsignedInt>)
      >('X509_digest');
  late final Pointer<Uint8> Function(int) verifyErrorString = _crypto
      .lookupFunction<Pointer<Uint8> Function(Long), Pointer<Uint8> Function(int)>('X509_verify_cert_error_string');
  late final int Function(Ptr, Pointer<Uint8>) paramSet1IpAsc = _crypto
      .lookupFunction<Int Function(Ptr, Pointer<Uint8>), int Function(Ptr, Pointer<Uint8>)>(
        'X509_VERIFY_PARAM_set1_ip_asc',
      );
  late final int Function(Pointer<Uint8>, Pointer<Uint8>, int, int, int, int, Pointer<Ptr>) bioLookupEx = _crypto
      .lookupFunction<
        Int Function(Pointer<Uint8>, Pointer<Uint8>, Int, Int, Int, Int, Pointer<Ptr>),
        int Function(Pointer<Uint8>, Pointer<Uint8>, int, int, int, int, Pointer<Ptr>)
      >('BIO_lookup_ex');
  late final Ptr Function(Ptr) addrinfoNext = _crypto.lookupFunction<Ptr Function(Ptr), Ptr Function(Ptr)>(
    'BIO_ADDRINFO_next',
  );
  late final int Function(Ptr) addrinfoFamily = _crypto.lookupFunction<Int Function(Ptr), int Function(Ptr)>(
    'BIO_ADDRINFO_family',
  );
  late final Ptr Function(Ptr) addrinfoAddress = _crypto.lookupFunction<Ptr Function(Ptr), Ptr Function(Ptr)>(
    'BIO_ADDRINFO_address',
  );
  late final void Function(Ptr) addrinfoFree = _crypto.lookupFunction<Void Function(Ptr), void Function(Ptr)>(
    'BIO_ADDRINFO_free',
  );
  late final int Function(int, int, int, int) bioSocket = _crypto
      .lookupFunction<Int Function(Int, Int, Int, Int), int Function(int, int, int, int)>('BIO_socket');
  late final int Function(int, Ptr, int) bioConnect = _crypto
      .lookupFunction<Int Function(Int, Ptr, Int), int Function(int, Ptr, int)>('BIO_connect');
  late final int Function(int, Ptr, int) bioBind = _crypto
      .lookupFunction<Int Function(Int, Ptr, Int), int Function(int, Ptr, int)>('BIO_bind');
  late final int Function(int, int) bioSocketNbio = _crypto.lookupFunction<Int Function(Int, Int), int Function(int, int)>(
    'BIO_socket_nbio',
  );
  late final int Function(int) bioCloseSocket = _crypto.lookupFunction<Int Function(Int), int Function(int)>(
    'BIO_closesocket',
  );
  late final Ptr Function(int, int) bioNewDgram = _crypto.lookupFunction<Ptr Function(Int, Int), Ptr Function(int, int)>(
    'BIO_new_dgram',
  );
  late final Ptr Function() addrNew = _crypto.lookupFunction<Ptr Function(), Ptr Function()>('BIO_ADDR_new');
  late final void Function(Ptr) addrFree = _crypto.lookupFunction<Void Function(Ptr), void Function(Ptr)>(
    'BIO_ADDR_free',
  );
  late final int Function(int, int, Pointer<Ptr>) bioSockInfo = _crypto
      .lookupFunction<Int Function(Int, Int, Pointer<Ptr>), int Function(int, int, Pointer<Ptr>)>('BIO_sock_info');
  late final int Function(Ptr) addrRawport = _crypto.lookupFunction<UnsignedShort Function(Ptr), int Function(Ptr)>(
    'BIO_ADDR_rawport',
  );

  /// The last OpenSSL error as text, or [fallback]. Clears the error queue.
  String error(String fallback) {
    final code = errPeekLast();
    if (code == 0) {
      return fallback;
    }
    final buffer = Native.alloc(256);
    try {
      errString(code, buffer, 256);
      return '$fallback: ${Native.readString(buffer)}';
    } finally {
      Native.free(buffer);
      errClear();
    }
  }

  /// Every PEM certificate in [pem], in order (free them with [x509Free]).
  List<Ptr> readCertificates(String pem) {
    final text = Native.string(pem);
    final bio = bioNewMemBuf(text, -1);
    final certificates = <Ptr>[];
    try {
      while (true) {
        final certificate = pemReadX509(bio, nullptr, nullptr, nullptr);
        if (certificate.address == 0) {
          break;
        }
        certificates.add(certificate);
      }
    } finally {
      bioFree(bio);
      Native.free(text);
      errClear();
    }
    return certificates;
  }

  Ptr readPrivateKey(String pem) {
    final text = Native.string(pem);
    final bio = bioNewMemBuf(text, -1);
    try {
      return pemReadPrivateKey(bio, nullptr, nullptr, nullptr);
    } finally {
      bioFree(bio);
      Native.free(text);
      errClear();
    }
  }

  /// Milliseconds until [ssl] needs SSL_handle_events again (null: no timer).
  int? eventTimeoutMs(Ptr ssl) {
    final tv = Native.alloc(16);
    final infinite = Native.alloc(4).cast<Int>();
    try {
      if (getEventTimeout(ssl, tv, infinite) != 1 || infinite.value != 0) {
        return null;
      }
      // struct timeval: a 64-bit tv_sec, then tv_usec (64-bit on Linux, 32-bit on macOS; little-endian)
      final seconds = tv.cast<Int64>().value;
      final micros = (tv + 8).cast<Int32>().value;
      return seconds * 1000 + (micros + 999) ~/ 1000;
    } finally {
      Native.free(tv);
      Native.free(infinite);
    }
  }

  /// Writes as much of [queue] to [stream] as it takes. False on a stream error.
  bool flush(Ptr stream, NativeQueue queue, Pointer<Size> written) {
    while (queue.length > 0) {
      if (writeEx(stream, queue.front, queue.length, written) == 1) {
        queue.consume(written.value);
        continue;
      }
      final code = getError(stream, 0);
      errClear();
      return code == sslErrorWantRead || code == sslErrorWantWrite;
    }
    return true;
  }

  /// Reads what [stream] has. Null when the stream ended, was reset or the connection closed; otherwise the bytes
  /// (possibly none).
  Uint8List? read(Ptr stream, Pointer<Uint8> chunk, int chunkSize, Pointer<Size> count) {
    BytesBuilder? builder;
    while (true) {
      if (readEx(stream, chunk, chunkSize, count) == 1) {
        (builder ??= BytesBuilder(copy: true)).add(chunk.asTypedList(count.value));
        continue;
      }
      final code = getError(stream, 0);
      errClear();
      if (code == sslErrorWantRead || code == sslErrorWantWrite) {
        return builder?.takeBytes() ?? Uint8List(0);
      }
      // Stream finished or reset, or the connection closed: what came before still counts
      return builder?.takeBytes();
    }
  }
}

/// `quic://host:port[/...]`, IPv6 hosts in brackets.
({String host, int port}) parseQuicAddress(String address) {
  final uri = Uri.tryParse(address);
  if (uri == null || uri.scheme != 'quic' || uri.host.isEmpty || !uri.hasPort || uri.port <= 0 || uri.port > 65535) {
    throw ArgumentError.value(address, 'address', 'must be quic://host:port');
  }
  return (host: uri.host, port: uri.port);
}

/// A SHA-256 fingerprint in hex, colons optional.
Uint8List parseQuicFingerprint(String text) {
  final hex = text.replaceAll(':', '');
  if (hex.length != 64 || !RegExp(r'^[0-9a-fA-F]+$').hasMatch(hex)) {
    throw ArgumentError.value(text, 'fingerprint', "isn't a SHA-256 in hex");
  }
  return Uint8List.fromList([for (var index = 0; index < 64; index += 2) int.parse(hex.substring(index, index + 2), radix: 16)]);
}
