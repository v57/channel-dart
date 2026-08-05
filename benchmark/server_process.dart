import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:channel_dart/server.dart';

Future<void> main() async {
  final port = _parsePort();
  if (port == null) {
    stderr.writeln(
      jsonEncode(<String, Object?>{
        'type': 'error',
        'message': 'BENCH_PORT must be a valid positive integer',
      }),
    );
    exit(1);
  }

  var requests = 0;
  final started = Stopwatch()..start();

  final server =
      await (Channel<Map<String, Object?>>()..post('text/count', (body) {
            requests += 1;
            final payload = body.body;
            final text = payload is Map ? payload['text'] : null;
            return text is String ? text.length : 0;
          }))
          .listen(
            port,
            options: ListenOptions<Map<String, Object?>>(
              state: (_) => <String, Object?>{},
            ),
          );

  stdout.writeln(
    jsonEncode(<String, Object?>{'type': 'ready', 'pid': pid, 'port': port}),
  );

  var stopped = false;

  Future<void> stopServer() async {
    if (stopped) {
      return;
    }
    stopped = true;

    await server.stop();
    started.stop();
    final elapsedSeconds = started.elapsedMicroseconds / 1000000.0;

    stdout.writeln(
      jsonEncode(<String, Object?>{
        'type': 'result',
        'pid': pid,
        'requests': requests,
        'elapsedSeconds': elapsedSeconds,
      }),
    );

    exit(0);
  }

  unawaited(
    ProcessSignal.sigterm.watch().listen((_) {
      unawaited(stopServer());
    }).asFuture<void>(),
  );

  unawaited(
    ProcessSignal.sigint.watch().listen((_) {
      unawaited(stopServer());
    }).asFuture<void>(),
  );

  await Completer<void>().future;
}

int? _parsePort() {
  final raw = Platform.environment['BENCH_PORT'];
  if (raw == null) {
    return null;
  }
  final port = int.tryParse(raw.trim());
  if (port == null || port <= 0) {
    return null;
  }
  return port;
}
