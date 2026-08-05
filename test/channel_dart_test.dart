import 'package:channel_dart/channel.dart';
import 'package:test/test.dart';

void main() {
  test('channel package exports Channel', () {
    final channel = Channel<Object?>();
    expect(channel, isA<Channel<Object?>>());
  });
}
