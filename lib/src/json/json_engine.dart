import 'dart:convert';

import 'package:fast_json/fast_json.dart' as fast_json;

abstract interface class JsonEngine {
  dynamic decode(String source);
  String encode(Object? value);
}

class DartJsonEngine implements JsonEngine {
  const DartJsonEngine();

  @override
  dynamic decode(String source) => jsonDecode(source);

  @override
  String encode(Object? value) => jsonEncode(value);
}

class FastJsonEngine implements JsonEngine {
  const FastJsonEngine();

  @override
  dynamic decode(String source) => fast_json.parse(source);

  @override
  String encode(Object? value) => jsonEncode(value);
}

enum JsonEngineKind { dart, fast }

JsonEngine jsonEngineFor(JsonEngineKind kind) {
  switch (kind) {
    case JsonEngineKind.dart:
      return const DartJsonEngine();
    case JsonEngineKind.fast:
      return const FastJsonEngine();
  }
}
