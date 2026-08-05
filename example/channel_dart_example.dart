import 'package:channel_dart/channel.dart';

void main() {
  final channel = Channel<Object?>();
  print(channel.runtimeType);
}
