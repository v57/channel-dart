import 'dart:convert';
import 'dart:io';

import 'package:channel_dart/channel.dart';
import 'package:channel_dart/process.dart';

void emit(Map<String, Object?> payload) {
  stdout.writeln(jsonEncode(payload));
}

Future<(int completed, int errors, double elapsedSeconds)> runScenario(
  Sender<Map<String, Object?>> sender,
  String path,
  int durationMs,
  bool Function() stopRequested,
) async {
  final started = Stopwatch()..start();
  var completed = 0;
  var errors = 0;
  while (!stopRequested() && started.elapsedMilliseconds < durationMs) {
    try {
      await sender.send<Object?>(path);
      completed += 1;
    } catch (_) {
      errors += 1;
    }
  }
  started.stop();
  return (completed, errors, started.elapsedMicroseconds / 1000000);
}

Future<void> main() async {
  final totalRequestsRaw = Platform.environment['BENCH_TOTAL_REQUESTS'];
  final totalRequests = int.tryParse(totalRequestsRaw ?? '');
  final hasTotalRequests = totalRequestsRaw != null && totalRequestsRaw.isNotEmpty;
  if (hasTotalRequests && (totalRequests == null || totalRequests < 0)) {
    emit(<String, Object?>{
      'type': 'error',
      'message': 'BENCH_TOTAL_REQUESTS must be a valid non-negative integer',
    });
    exitCode = 1;
    return;
  }

  final durationRaw = Platform.environment['BENCH_DURATION_MS'];
  final durationMs = int.tryParse(durationRaw ?? '');
  if (!hasTotalRequests && (durationMs == null || durationMs <= 0)) {
    emit(<String, Object?>{
      'type': 'error',
      'message': 'BENCH_DURATION_MS must be a valid positive integer',
    });
    exitCode = 1;
    return;
  }

  var stop = false;
  ProcessSignal.sigterm.watch().listen((_) => stop = true);
  ProcessSignal.sigint.watch().listen((_) => stop = true);

  final sender = (Channel<Map<String, Object?>>()
        ..post('text/count', (body) {
          final payload = body.body;
          final text = payload is Map ? payload['text'] : null;
          return text is String ? text.length : 0;
        })
        ..post('sync', (_) => 1)
        ..post('async', (_) async => 1))
      .process(state: <String, Object?>{});

  emit(<String, Object?>{
    'type': 'ready',
    'pid': pid,
  });

  if (hasTotalRequests) {
    var completed = 0;
    var errors = 0;
    final started = Stopwatch()..start();
    while (!stop && (completed + errors) < (totalRequests ?? 0)) {
      try {
        final result = await sender.send<Object?>(
          'text/count',
          body: const <String, Object?>{'text': 'hello'},
        );
        if (result == 5) {
          completed += 1;
        } else {
          errors += 1;
        }
      } catch (_) {
        errors += 1;
      }
    }
    started.stop();
    sender.stop();

    emit(<String, Object?>{
      'type': 'result',
      'pid': pid,
      'completed': completed,
      'errors': errors,
      'elapsedSeconds': started.elapsedMicroseconds / 1000000,
    });
    exit(0);
  }

  final sync = await runScenario(sender, 'sync', durationMs!, () => stop);
  final async = await runScenario(sender, 'async', durationMs!, () => stop);
  sender.stop();

  emit(<String, Object?>{
    'type': 'result',
    'pid': pid,
    'syncCompleted': sync.$1,
    'syncErrors': sync.$2,
    'syncElapsedSeconds': sync.$3,
    'asyncCompleted': async.$1,
    'asyncErrors': async.$2,
    'asyncElapsedSeconds': async.$3,
    'asyncFallback': false,
  });
}
