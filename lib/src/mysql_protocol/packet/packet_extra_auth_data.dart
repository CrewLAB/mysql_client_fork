import 'dart:typed_data';
import 'package:mysql_client_fork/mysql_protocol.dart';
import 'package:mysql_client_fork/mysql_protocol_extension.dart';

class MySQLPacketExtraAuthData extends MySQLPacketPayload {
  MySQLPacketExtraAuthData({required this.header, required this.pluginData});

  factory MySQLPacketExtraAuthData.decode(Uint8List buffer) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    final int header = byteData.getUint8(offset);
    offset += 1;

    final String pluginData = buffer.getUtf8StringEOF(offset);

    return MySQLPacketExtraAuthData(header: header, pluginData: pluginData);
  }

  int header;
  String pluginData;

  @override
  Uint8List encode() {
    throw UnimplementedError();
  }
}
