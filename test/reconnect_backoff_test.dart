import 'dart:io';

import 'package:channel_dart/client.dart';
import 'package:test/test.dart';

// The client used to retry every 100 ms forever while the server was down (TS parity: channel docs/client.md).
void main() {
  test('reconnectWait doubles from the base up to the cap, with jitter', () {
    const base = Duration(milliseconds: 100);
    const max = Duration(seconds: 5);
    final waits = [
      for (var attempt = 0; attempt < 8; attempt += 1)
        WebSocketClient.reconnectWait(base, max, attempt, 1).inMilliseconds,
    ];
    expect(waits, [100, 200, 400, 800, 1600, 3200, 5000, 5000]);
    expect(WebSocketClient.reconnectWait(base, max, 0, 0).inMilliseconds, 50);
    expect(
      WebSocketClient.reconnectWait(base, max, 1000, 0).inMilliseconds,
      2500,
    );
  });

  test('client backs off while the server refuses the upgrade', () async {
    // Every request is refused with HTTP 500: each one is a failed connect attempt.
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    var attempts = 0;
    server.listen((request) {
      attempts += 1;
      request.response.statusCode = 500;
      request.response.close();
    });

    final client = WebSocketClient('ws://127.0.0.1:${server.port}');
    await Future<void>.delayed(const Duration(seconds: 2));
    client.stop();
    await server.close(force: true);

    // 100 ms retries would make ~20 attempts in 2 s; backoff allows at most 1 + 6 (waits of 50..100, 100..200, ...).
    expect(attempts, inInclusiveRange(2, 8));
  });
}
