/// The experimental QUIC transport (`channel/docs/quic.md`) on OpenSSL 3.5+'s native QUIC, through dart:ffi.
///
/// Only programs that import this library load OpenSSL; `client.dart` and `server.dart` stay WebSocket-only.
/// OpenSSL is looked up as `libssl.so.3` / `libcrypto.so.3` (Homebrew's `openssl@3` on macOS); set
/// `CHANNEL_QUIC_LIBSSL` and `CHANNEL_QUIC_LIBCRYPTO` to use other paths.
library;

import 'src/client/client_extension.dart';
import 'src/client/web_socket_client.dart';
import 'src/core/channel_core.dart';
import 'src/quic/openssl.dart';
import 'src/quic/quic_client.dart';
import 'src/quic/quic_server.dart';
import 'src/server/frame_limit.dart';

export 'client.dart';
export 'server.dart';
export 'src/quic/frames.dart'
    show quicAlpn, encodeQuicFrame, encodeQuicHello, QuicFrameReader, QuicFrameTooLarge;
export 'src/quic/openssl.dart' show QuicUnavailable;
export 'src/quic/quic_client.dart'
    show QuicOptions, QuicConnectException, QuicProtocolException, quicDialer;
export 'src/quic/quic_server.dart' show QuicListenOptions, QuicChannelServer;

extension ChannelQuic<State> on Channel<State> {
  /// Connects to a `quic://host:port` address (throws [QuicUnavailable] without OpenSSL 3.5+). Like `connect`, the client reconnects with backoff and sends
  /// its unanswered requests again. [maxMessageSize] caps incoming frames (16 MiB by default).
  ClientSender<State> connectQuic(
    String address, {
    QuicOptions quic = const QuicOptions(),
    ClientOptions<State>? options,
    int maxMessageSize = defaultMaxMessageSize,
  }) {
    if (!address.startsWith('quic://')) {
      throw ArgumentError.value(address, 'address', 'must be quic://host:port');
    }
    parseQuicAddress(address);
    // A missing or old OpenSSL fails here, not on every reconnect
    OpenSsl.instance;
    final clientOptions = options ?? ClientOptions<State>();
    final ws = WebSocketClient(
      address,
      headers: clientOptions.headers,
      jsonEngine: clientOptions.jsonEngine,
      dialer: quicDialer(quic, maxPayload: maxMessageSize),
    );
    return connectClient(ws, options: clientOptions);
  }

  /// Serves this channel over QUIC on UDP [address] (`port` or `host:port`, `127.0.0.1` by default). Run
  /// `listen` next to it for WebSocket clients.
  /// Fails with [QuicUnavailable] when OpenSSL 3.5+ can't be loaded.
  Future<QuicChannelServer<State>> listenQuic(
    Object address, {
    required QuicListenOptions<State> options,
  }) {
    return listenQuicServer<State>(this, address, options);
  }
}
