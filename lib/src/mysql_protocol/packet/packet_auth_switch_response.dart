import 'dart:convert';
import 'dart:typed_data';
import 'package:buffer/buffer.dart';
import 'package:mysql_client_fork/mysql_protocol.dart';

class MySQLPacketAuthSwitchResponse extends MySQLPacketPayload {
  MySQLPacketAuthSwitchResponse({required this.authData});

  factory MySQLPacketAuthSwitchResponse.createWithNativePassword({
    required String password,
    required Uint8List challenge,
  }) {
    assert(challenge.length == 20);
    final Uint8List passwordBytes = utf8.encode(password);

    final Uint8List authData = xor(sha1(passwordBytes), sha1(challenge + sha1(sha1(passwordBytes))));

    return MySQLPacketAuthSwitchResponse(authData: authData);
  }

  Uint8List authData;

  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)..write(authData);

    return buffer.toBytes();
  }
}
