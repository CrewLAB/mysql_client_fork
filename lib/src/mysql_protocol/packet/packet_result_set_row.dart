import 'dart:typed_data';
import 'package:mysql_client_fork/mysql_protocol.dart';
import 'package:mysql_client_fork/mysql_protocol_extension.dart';
import 'package:tuple/tuple.dart';

class MySQLResultSetRowPacket extends MySQLPacketPayload {
  MySQLResultSetRowPacket({required this.values});

  factory MySQLResultSetRowPacket.decode(Uint8List buffer, int numOfCols) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    final List<String?> values = <String?>[];

    for (int x = 0; x < numOfCols; x++) {
      Tuple2<String, int> value;
      final int nextByte = byteData.getUint8(offset);

      if (nextByte == 0xfb) {
        values.add(null);
        offset += 1;
      } else {
        value = buffer.getUtf8LengthEncodedString(offset);
        values.add(value.item1);
        offset += value.item2;
      }
    }

    return MySQLResultSetRowPacket(values: values);
  }

  List<String?> values;

  @override
  Uint8List encode() {
    throw UnimplementedError();
  }
}
