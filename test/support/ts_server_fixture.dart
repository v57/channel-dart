import 'dart:async';
import 'dart:convert';
import 'dart:io';

class TsServerFixture {
  Process? _process;
  final List<String> _stdout = <String>[];
  final List<String> _stderr = <String>[];

  Future<void> start() async {
    if (_process != null) {
      return;
    }

    final channelDir = Directory.fromUri(
      Directory.current.uri.resolve('../channel/'),
    );

    _process = await Process.start(
      'bun',
      <String>['tests/server.ts'],
      workingDirectory: channelDir.path,
      runInShell: true,
    );

    _process!.stdout
        .transform(SystemEncoding().decoder)
        .transform(const LineSplitter())
        .listen((String line) => _stdout.add(line));
    _process!.stderr
        .transform(SystemEncoding().decoder)
        .transform(const LineSplitter())
        .listen((String line) => _stderr.add(line));

    await _waitUntilReachable();
  }

  Future<void> stop() async {
    final process = _process;
    _process = null;
    if (process == null) {
      return;
    }

    process.kill(ProcessSignal.sigterm);
    try {
      await process.exitCode.timeout(const Duration(seconds: 3));
    } on TimeoutException {
      process.kill(ProcessSignal.sigkill);
      await process.exitCode.timeout(const Duration(seconds: 2));
    }
  }

  Future<void> _waitUntilReachable() async {
    final deadline = DateTime.now().add(const Duration(seconds: 15));
    Object? lastError;

    while (DateTime.now().isBefore(deadline)) {
      final process = _process;
      if (process == null) {
        throw StateError('fixture process missing');
      }

      final exitFuture = process.exitCode;
      if (await exitFuture.timeout(
            const Duration(milliseconds: 1),
            onTimeout: () => -999,
          ) !=
          -999) {
        throw StateError(
          'TypeScript fixture exited before startup.\n'
          'stdout:\n${_stdout.join('\n')}\n\n'
          'stderr:\n${_stderr.join('\n')}',
        );
      }

      try {
        final socket = await WebSocket.connect('ws://127.0.0.1:2049');
        await socket.close();
        return;
      } catch (error) {
        lastError = error;
        await Future<void>.delayed(const Duration(milliseconds: 100));
      }
    }

    throw StateError(
      'Timeout waiting for TypeScript fixture on 127.0.0.1:2049. Last error: $lastError\n'
      'stdout:\n${_stdout.join('\n')}\n\n'
      'stderr:\n${_stderr.join('\n')}',
    );
  }
}
