import 'dart:convert';
import 'dart:typed_data';
import 'package:buffer/buffer.dart' show ByteDataWriter;
import 'package:mysql_client_fork/mysql_protocol.dart';
import 'package:mysql_client_fork/mysql_protocol_extension.dart';

class MySQLPacketCommInitDB extends MySQLPacketPayload {
  MySQLPacketCommInitDB({required this.schemaName});

  String schemaName;

  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)
      // command type
      ..writeUint8(2)
      ..write(utf8.encode(schemaName));

    return buffer.toBytes();
  }
}

class MySQLPacketCommQuery extends MySQLPacketPayload {
  MySQLPacketCommQuery({required this.query});

  String query;

  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)
      // command type
      ..writeUint8(3)
      ..write(utf8.encode(query));

    return buffer.toBytes();
  }
}

class MySQLPacketCommStmtPrepare extends MySQLPacketPayload {
  MySQLPacketCommStmtPrepare({required this.query});

  String query;

  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)
      // command type
      ..writeUint8(0x16)
      ..write(utf8.encode(query));

    return buffer.toBytes();
  }
}

class MySQLPacketCommStmtExecute extends MySQLPacketPayload {
  // (type, value)

  MySQLPacketCommStmtExecute({required this.stmtID, required this.params});

  int stmtID;
  List<dynamic> params;

  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)
      // command type
      ..writeUint8(0x17)
      // stmt id
      ..writeUint32(stmtID, Endian.little)
      // flags
      ..writeUint8(0)
      // iteration count (always 1)
      ..writeUint32(1, Endian.little);

    // params
    if (params.isNotEmpty) {
      // create null-bitmap
      final int bitmapSize = ((params.length + 7) / 8).floor();
      final Uint8List nullBitmap = Uint8List(bitmapSize);

      // write null values into null bitmap
      int paramIndex = 0;
      for (final param in params) {
        if (param == null) {
          final int paramByteIndex = ((paramIndex) / 8).floor();
          final int paramBitIndex = ((paramIndex) % 8);
          nullBitmap[paramByteIndex] = nullBitmap[paramByteIndex] | (1 << paramBitIndex);
        }
        paramIndex++;
      }

      // write null bitmap
      buffer
        ..write(nullBitmap)
        // write new-param-bound flag
        ..writeUint8(1);

      // write not null values

      // write param types
      for (final param in params) {
        if (param != null) {
          buffer
            ..writeUint8(mysqlColumnTypeVarString)
            // unsigned flag
            ..writeUint8(0);
        } else {
          buffer
            ..writeUint8(mysqlColumnTypeNull)
            ..writeUint8(0);
        }
      }
      // write param values
      for (final param in params) {
        if (param != null) {
          final String value = param.toString();
          final Uint8List encodedData = utf8.encode(value);
          buffer
            ..writeVariableEncInt(encodedData.length)
            ..write(encodedData);
        }
      }
    }

    return buffer.toBytes();
  }
}

class MySQLPacketCommQuit extends MySQLPacketPayload {
  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)
      // command type
      ..writeUint8(1);

    return buffer.toBytes();
  }
}

class MySQLPacketCommStmtClose extends MySQLPacketPayload {
  MySQLPacketCommStmtClose({required this.stmtID});

  int stmtID;

  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)
      // command type
      ..writeUint8(0x19)
      ..writeUint32(stmtID);

    return buffer.toBytes();
  }
}
