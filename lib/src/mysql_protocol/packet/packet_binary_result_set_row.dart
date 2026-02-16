import 'dart:typed_data';

import 'package:mysql_client_fork/exception.dart';
import 'package:mysql_client_fork/mysql_protocol.dart';
import 'package:tuple/tuple.dart';

class MySQLBinaryResultSetRowPacket extends MySQLPacketPayload {
  MySQLBinaryResultSetRowPacket({required this.values});

  factory MySQLBinaryResultSetRowPacket.decode(Uint8List buffer, List<MySQLColumnDefinitionPacket> colDefs) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    // packet header (always should by 0x00)
    final int type = byteData.getUint8(offset);
    offset += 1;

    if (type != 0) {
      throw MySQLProtocolException('Can not decode MySQLBinaryResultSetRowPacket: packet type is not 0x00');
    }

    final List<String?> values = <String?>[];

    // parse null bitmap
    final int nullBitmapSize = ((colDefs.length + 9) / 8).floor();

    final Uint8List nullBitmap = Uint8List.sublistView(buffer, offset, offset + nullBitmapSize);

    offset += nullBitmapSize;

    // parse binary data
    for (int x = 0; x < colDefs.length; x++) {
      // check null bitmap first
      final int bitmapByteIndex = ((x + 2) / 8).floor();
      final int bitmapBitIndex = (x + 2) % 8;

      final int byteToCheck = nullBitmap[bitmapByteIndex];
      final bool isNull = (byteToCheck & (1 << bitmapBitIndex)) != 0;

      if (isNull) {
        values.add(null);
      } else {
        final Tuple2<String, int> parseResult = parseBinaryColumnData(colDefs[x].type.intVal, byteData, buffer, offset);
        offset += parseResult.item2;
        values.add(parseResult.item1);
      }
    }

    return MySQLBinaryResultSetRowPacket(values: values);
  }

  List<String?> values;

  @override
  Uint8List encode() {
    throw UnimplementedError();
  }
}
