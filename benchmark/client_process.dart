import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:channel_dart/client.dart';

Future<void> main() async {
  final config = _Config.fromEnvironment();
  if (config == null) {
    exit(1);
  }

  final channel = Channel<Map<String, Object?>>();
  final clients = <ClientSender<Map<String, Object?>>>[];
  final connected = List<bool>.filled(config.clientsCount, false);

  final targetRps = config.targetRpsPerClient * config.clientsCount;

  var connectedCount = 0;
  var nextClientIndex = 0;
  var requestsSent = 0;
  var responsesReceived = 0;
  var errors = 0;
  var inFlight = 0;
  var running = false;
  var stopped = false;
  Timer? tickTimer;

  final started = Stopwatch()..start();

  late final Timer setupTimeout;
  setupTimeout = Timer(const Duration(seconds: 10), () {
    if (connectedCount == config.clientsCount) {
      return;
    }
    stderr.writeln(
      jsonEncode(<String, Object?>{
        'type': 'error',
        'message':
            'Timed out waiting for clients to connect ($connectedCount/${config.clientsCount})',
      }),
    );
    exit(1);
  });

  void sendRequest([void Function()? onComplete]) {
    if (!running || clients.isEmpty) {
      return;
    }

    final sender = clients[nextClientIndex];
    nextClientIndex += 1;
    if (nextClientIndex == clients.length) {
      nextClientIndex = 0;
    }

    requestsSent += 1;
    inFlight += 1;

    unawaited(
      sender
          .send<Object?>(
            'text/count',
            body: const <String, Object?>{'text': 'hello'},
          )
          .then((Object? _) {
            responsesReceived += 1;
          })
          .catchError((Object _) {
            errors += 1;
            return null;
          })
          .whenComplete(() {
            inFlight -= 1;
            onComplete?.call();
          }),
    );
  }

  Future<void> stopClient() async {
    if (stopped) {
      return;
    }
    stopped = true;
    running = false;

    setupTimeout.cancel();
    tickTimer?.cancel();
    tickTimer = null;

    final deadline = DateTime.now().add(const Duration(milliseconds: 2000));
    while (inFlight > 0 && DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }

    for (final client in clients) {
      client.stop();
    }

    started.stop();
    final elapsedSeconds = started.elapsedMicroseconds / 1000000.0;

    stdout.writeln(
      jsonEncode(<String, Object?>{
        'type': 'result',
        'pid': pid,
        'requestsSent': requestsSent,
        'responsesReceived': responsesReceived,
        'errors': errors,
        'elapsedSeconds': elapsedSeconds,
      }),
    );

    exit(0);
  }

  void maybeFinishFixedMode(int targetRequests) {
    if (requestsSent >= targetRequests &&
        (responsesReceived + errors) >= targetRequests &&
        inFlight == 0) {
      unawaited(stopClient());
    }
  }

  void runFixedMode(int targetRequests) {
    if (targetRequests == 0) {
      unawaited(stopClient());
      return;
    }

    void pumpStream() {
      if (!running || stopped) return;
      while (requestsSent < targetRequests && inFlight < config.maxInFlight) {
        sendRequest(() {
          pumpStream();
          maybeFinishFixedMode(targetRequests);
        });
      }
      maybeFinishFixedMode(targetRequests);
    }

    void pumpBurstChunk() {
      if (!running || stopped) return;
      var budget = 2000;
      while (requestsSent < targetRequests && budget > 0) {
        sendRequest(() {
          maybeFinishFixedMode(targetRequests);
        });
        budget -= 1;
      }
      if (requestsSent < targetRequests) {
        scheduleMicrotask(pumpBurstChunk);
      } else {
        maybeFinishFixedMode(targetRequests);
      }
    }

    if (config.requestMode == 'stream') {
      pumpStream();
    } else {
      pumpBurstChunk();
    }
  }

  void startSending() {
    if (running) {
      return;
    }
    running = true;

    stdout.writeln(
      jsonEncode(<String, Object?>{
        'type': 'ready',
        'pid': pid,
        'clients': config.clientsCount,
      }),
    );

    if (config.totalRequests != null) {
      runFixedMode(config.totalRequests!);
      return;
    }

    const tickMs = 10;
    var carry = 0.0;

    tickTimer = Timer.periodic(const Duration(milliseconds: tickMs), (_) {
      if (!running) {
        return;
      }

      carry += (targetRps * tickMs) / 1000.0;
      final sends = carry.floor();
      carry -= sends;

      for (var i = 0; i < sends; i += 1) {
        sendRequest();
      }
    });
  }

  for (var i = 0; i < config.clientsCount; i += 1) {
    final sender = channel.connect(
      'ws://${config.host}:${config.port}',
      options: ClientOptions<Map<String, Object?>>(
        onConnect: (_) {
          if (connected[i]) {
            return;
          }

          connected[i] = true;
          connectedCount += 1;

          if (connectedCount == config.clientsCount) {
            setupTimeout.cancel();
            startSending();
          }
        },
      ),
    );
    clients.add(sender);
  }

  unawaited(
    ProcessSignal.sigterm.watch().listen((_) {
      unawaited(stopClient());
    }).asFuture<void>(),
  );

  unawaited(
    ProcessSignal.sigint.watch().listen((_) {
      unawaited(stopClient());
    }).asFuture<void>(),
  );

  await Completer<void>().future;
}

class _Config {
  _Config({
    required this.host,
    required this.port,
    required this.clientsCount,
    required this.targetRpsPerClient,
    required this.totalRequests,
    required this.requestMode,
    required this.maxInFlight,
  });

  final String host;
  final int port;
  final int clientsCount;
  final double targetRpsPerClient;
  final int? totalRequests;
  final String requestMode;
  final int maxInFlight;

  static _Config? fromEnvironment() {
    final host = (Platform.environment['BENCH_HOST'] ?? '127.0.0.1').trim();

    final portRaw = Platform.environment['BENCH_PORT'];
    final port = portRaw == null ? null : int.tryParse(portRaw.trim());
    if (port == null || port <= 0) {
      stderr.writeln(
        jsonEncode(<String, Object?>{
          'type': 'error',
          'message': 'BENCH_PORT must be a valid positive integer',
        }),
      );
      return null;
    }

    final clientsRaw = Platform.environment['BENCH_CLIENTS'];
    final clients = clientsRaw == null ? null : int.tryParse(clientsRaw.trim());
    if (clients == null || clients <= 0) {
      stderr.writeln(
        jsonEncode(<String, Object?>{
          'type': 'error',
          'message': 'BENCH_CLIENTS must be a valid positive integer',
        }),
      );
      return null;
    }

    final totalRequestsRaw = Platform.environment['BENCH_TOTAL_REQUESTS'];
    final totalRequests = totalRequestsRaw == null
        ? null
        : int.tryParse(totalRequestsRaw.trim());
    if (totalRequestsRaw != null &&
        (totalRequests == null || totalRequests < 0)) {
      stderr.writeln(
        jsonEncode(<String, Object?>{
          'type': 'error',
          'message':
              'BENCH_TOTAL_REQUESTS must be a valid non-negative integer',
        }),
      );
      return null;
    }

    final requestMode = (Platform.environment['BENCH_REQUEST_MODE'] ?? 'burst')
        .trim()
        .toLowerCase();
    if (requestMode != 'burst' && requestMode != 'stream') {
      stderr.writeln(
        jsonEncode(<String, Object?>{
          'type': 'error',
          'message': 'BENCH_REQUEST_MODE must be either burst or stream',
        }),
      );
      return null;
    }

    final maxInFlightRaw = Platform.environment['BENCH_MAX_IN_FLIGHT'];
    final maxInFlight = maxInFlightRaw == null
        ? 4096
        : int.tryParse(maxInFlightRaw.trim());
    if (totalRequests != null && (maxInFlight == null || maxInFlight <= 0)) {
      stderr.writeln(
        jsonEncode(<String, Object?>{
          'type': 'error',
          'message': 'BENCH_MAX_IN_FLIGHT must be a valid positive integer',
        }),
      );
      return null;
    }

    final rpsRaw = Platform.environment['BENCH_RPS_PER_CLIENT'];
    final rps = rpsRaw == null
        ? null
        : double.tryParse(rpsRaw.trim().replaceAll(',', '.'));
    if (totalRequests == null && (rps == null || !rps.isFinite || rps <= 0)) {
      stderr.writeln(
        jsonEncode(<String, Object?>{
          'type': 'error',
          'message': 'BENCH_RPS_PER_CLIENT must be a positive number',
        }),
      );
      return null;
    }

    return _Config(
      host: host,
      port: port,
      clientsCount: clients,
      targetRpsPerClient: rps ?? 0,
      totalRequests: totalRequests,
      requestMode: requestMode,
      maxInFlight: maxInFlight ?? 4096,
    );
  }
}
