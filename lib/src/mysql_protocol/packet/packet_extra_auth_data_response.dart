import 'dart:typed_data';
import 'package:buffer/buffer.dart';
import 'package:mysql_client_fork/mysql_protocol.dart';

class MySQLPacketExtraAuthDataResponse extends MySQLPacketPayload {
  MySQLPacketExtraAuthDataResponse({required this.data});

  final Uint8List data;

  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)
      ..write(data)
      ..writeUint8(0);
    return buffer.toBytes();
  }
}
