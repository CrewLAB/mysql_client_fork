import 'dart:typed_data';

import 'package:buffer/buffer.dart' show ByteDataWriter;
import 'package:crypto/crypto.dart' as crypto;
import 'package:mysql_client_fork/exception.dart';
import 'package:mysql_client_fork/mysql_protocol.dart';
import 'package:tuple/tuple.dart' show Tuple2;

const int mysqlCapFlagClientLongPassword = 0x00000001;
const int mysqlCapFlagClientFoundRows = 0x00000002;
const int mysqlCapFlagClientLongFlag = 0x00000004;
const int mysqlCapFlagClientConnectWithDB = 0x00000008;
const int mysqlCapFlagClientNoSchema = 0x00000010;
const int mysqlCapFlagClientCompress = 0x00000020;
const int mysqlCapFlagClientODBC = 0x00000040;
const int mysqlCapFlagClientLocalFiles = 0x00000080;
const int mysqlCapFlagClientIgnoreSpace = 0x00000100;
const int mysqlCapFlagClientProtocol41 = 0x00000200;
const int mysqlCapFlagClientInteractive = 0x00000400;
const int mysqlCapFlagClientSsl = 0x00000800;
const int mysqlCapFlagClientIgnoreSigPipe = 0x00001000;
const int mysqlCapFlagClientTransactions = 0x00002000;
const int mysqlCapFlagClientReserved = 0x00004000;
const int mysqlCapFlagClientSecureConnection = 0x00008000;
const int mysqlCapFlagClientMultiStatements = 0x00010000;
const int mysqlCapFlagClientMultiResults = 0x00020000;
const int mysqlCapFlagClientPsMultiResults = 0x00040000;
const int mysqlCapFlagClientPluginAuth = 0x00080000;
const int mysqlCapFlagClientPluginAuthLenEncClientData = 0x00200000;
const int mysqlCapFlagClientDeprecateEOF = 0x01000000;

const int mysqlServerFlagMoreResultsExists = 0x0008;

enum MySQLGenericPacketType { ok, error, eof, other }

abstract class MySQLPacketPayload {
  Uint8List encode();
}

class MySQLPacket {
  MySQLPacket({required this.sequenceID, required this.payload, required this.payloadLength});

  factory MySQLPacket.decodeInitialHandshake(Uint8List buffer) {
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;
    final int payloadLength = header.item1;
    final int sequenceNumber = header.item2;

    final MySQLPacketInitialHandshake payload = MySQLPacketInitialHandshake.decode(
      Uint8List.sublistView(buffer, offset),
    );

    return MySQLPacket(sequenceID: sequenceNumber, payloadLength: payloadLength, payload: payload);
  }

  factory MySQLPacket.decodeAuthSwitchRequestPacket(Uint8List buffer) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;
    final int payloadLength = header.item1;
    final int sequenceNumber = header.item2;

    final int type = byteData.getUint8(offset);

    if (type != 0xfe) {
      throw MySQLProtocolException('Can not decode AuthSwitchResponse packet: type is not 0xfe');
    }

    final MySQLPacketAuthSwitchRequest payload = MySQLPacketAuthSwitchRequest.decode(
      Uint8List.sublistView(buffer, offset),
    );

    return MySQLPacket(sequenceID: sequenceNumber, payloadLength: payloadLength, payload: payload);
  }

  factory MySQLPacket.decodeGenericPacket(Uint8List buffer) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;
    final int payloadLength = header.item1;
    final int sequenceNumber = header.item2;

    final int type = byteData.getUint8(offset);

    MySQLPacketPayload payload;

    if (type == 0x00 && payloadLength >= 7) {
      // OK packet
      payload = MySQLPacketOK.decode(Uint8List.sublistView(buffer, offset));
    } else if (type == 0xfe && payloadLength < 9) {
      // EOF packet
      payload = MySQLPacketEOF.decode(Uint8List.sublistView(buffer, offset));
    } else if (type == 0xff) {
      payload = MySQLPacketError.decode(Uint8List.sublistView(buffer, offset));
    } else if (type == 0x01) {
      payload = MySQLPacketExtraAuthData.decode(Uint8List.sublistView(buffer, offset));
    } else {
      throw MySQLProtocolException('Unsupported generic packet: $buffer');
    }

    return MySQLPacket(sequenceID: sequenceNumber, payloadLength: payloadLength, payload: payload);
  }

  factory MySQLPacket.decodeColumnCountPacket(Uint8List buffer) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;
    final int payloadLength = header.item1;
    final int sequenceNumber = header.item2;

    final int type = byteData.getUint8(offset);

    MySQLPacketPayload payload;

    if (type == 0x00) {
      // OK packet
      payload = MySQLPacketOK.decode(Uint8List.sublistView(buffer, offset));
    } else if (type == 0xff) {
      payload = MySQLPacketError.decode(Uint8List.sublistView(buffer, offset));
    } else if (type == 0xfb) {
      throw MySQLProtocolException('COM_QUERY_RESPONSE of type 0xfb is not implemented');
    } else {
      payload = MySQLPacketColumnCount.decode(Uint8List.sublistView(buffer, offset));
    }

    return MySQLPacket(sequenceID: sequenceNumber, payloadLength: payloadLength, payload: payload);
  }

  factory MySQLPacket.decodeColumnDefPacket(Uint8List buffer) {
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;
    final int payloadLength = header.item1;
    final int sequenceNumber = header.item2;

    final MySQLColumnDefinitionPacket payload = MySQLColumnDefinitionPacket.decode(
      Uint8List.sublistView(buffer, offset),
    );

    return MySQLPacket(sequenceID: sequenceNumber, payloadLength: payloadLength, payload: payload);
  }

  factory MySQLPacket.decodeResultSetRowPacket(Uint8List buffer, int numOfCols) {
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;
    final int payloadLength = header.item1;
    final int sequenceNumber = header.item2;

    final MySQLResultSetRowPacket payload = MySQLResultSetRowPacket.decode(
      Uint8List.sublistView(buffer, offset),
      numOfCols,
    );

    return MySQLPacket(sequenceID: sequenceNumber, payloadLength: payloadLength, payload: payload);
  }

  factory MySQLPacket.decodeBinaryResultSetRowPacket(Uint8List buffer, List<MySQLColumnDefinitionPacket> colDefs) {
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;
    final int payloadLength = header.item1;
    final int sequenceNumber = header.item2;

    final MySQLBinaryResultSetRowPacket payload = MySQLBinaryResultSetRowPacket.decode(
      Uint8List.sublistView(buffer, offset),
      colDefs,
    );

    return MySQLPacket(sequenceID: sequenceNumber, payloadLength: payloadLength, payload: payload);
  }

  factory MySQLPacket.decodeCommPrepareStmtResponsePacket(Uint8List buffer) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;
    final int payloadLength = header.item1;
    final int sequenceNumber = header.item2;

    final int type = byteData.getUint8(offset);

    MySQLPacketPayload payload;

    if (type == 0x00) {
      // OK packet
      payload = MySQLPacketStmtPrepareOK.decode(Uint8List.sublistView(buffer, offset));
    } else if (type == 0xff) {
      payload = MySQLPacketError.decode(Uint8List.sublistView(buffer, offset));
    } else {
      throw MySQLProtocolException('Unexpected header type while decoding COM_STMT_PREPARE response: $header');
    }

    return MySQLPacket(sequenceID: sequenceNumber, payloadLength: payloadLength, payload: payload);
  }

  int sequenceID;
  int payloadLength;
  MySQLPacketPayload payload;

  static int getPacketLength(Uint8List buffer) {
    // payloadLength
    final ByteData db = ByteData(4)
      ..setUint8(0, buffer[0])
      ..setUint8(1, buffer[1])
      ..setUint8(2, buffer[2])
      ..setUint8(3, 0);

    final int payloadLength = db.getUint32(0, Endian.little);

    return payloadLength + 4;
  }

  static Tuple2<int, int> decodePacketHeader(Uint8List buffer) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    // payloadLength
    final ByteData db = ByteData(4)
      ..setUint8(0, buffer[0])
      ..setUint8(1, buffer[1])
      ..setUint8(2, buffer[2])
      ..setUint8(3, 0);

    final int payloadLength = db.getUint32(0, Endian.little);
    offset += 3;

    // sequence number
    final int sequenceNumber = byteData.getUint8(offset);

    return Tuple2<int, int>(payloadLength, sequenceNumber);
  }

  static MySQLGenericPacketType detectPacketType(Uint8List buffer) {
    final ByteData byteData = ByteData.sublistView(buffer);
    int offset = 0;

    final Tuple2<int, int> header = MySQLPacket.decodePacketHeader(buffer);
    offset += 4;

    final int payloadLength = header.item1;
    final int type = byteData.getUint8(offset);

    if (type == 0x00 && payloadLength >= 7) {
      // OK packet
      return MySQLGenericPacketType.ok;
    } else if (type == 0xfe && payloadLength < 9) {
      // EOF packet
      return MySQLGenericPacketType.eof;
    } else if (type == 0xff) {
      return MySQLGenericPacketType.error;
    } else {
      return MySQLGenericPacketType.other;
    }
  }

  bool isOkPacket() => payload is MySQLPacketOK;

  bool isErrorPacket() => payload is MySQLPacketError;

  bool isEOFPacket() {
    final MySQLPacketPayload internalPayload = payload;

    if (internalPayload is MySQLPacketEOF) {
      return true;
    }

    return internalPayload is MySQLPacketOK && internalPayload.header == 0xfe && payloadLength < 9;
  }

  Uint8List encode() {
    final Uint8List payloadData = payload.encode();

    final ByteData byteData = ByteData(4)
      ..setInt32(0, payloadData.lengthInBytes, Endian.little)
      ..setInt8(3, sequenceID);

    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little)
      ..write(byteData.buffer.asUint8List())
      ..write(payloadData);

    return buffer.toBytes();
  }
}

List<int> sha1(List<int> data) {
  return crypto.sha1.convert(data).bytes;
}

List<int> sha256(List<int> data) {
  return crypto.sha256.convert(data).bytes;
}

Uint8List xor(List<int> aList, List<int> bList) {
  final Uint8List a = Uint8List.fromList(aList);
  final Uint8List b = Uint8List.fromList(bList);

  if (a.lengthInBytes == 0 || b.lengthInBytes == 0) {
    throw ArgumentError.value('lengthInBytes of Uint8List arguments must be > 0');
  }

  final bool aIsBigger = a.lengthInBytes > b.lengthInBytes;
  final int length = aIsBigger ? a.lengthInBytes : b.lengthInBytes;

  final Uint8List buffer = Uint8List(length);

  for (int i = 0; i < length; i++) {
    int aa, bb;
    try {
      aa = a.elementAt(i);
    } catch (e) {
      aa = 0;
    }
    try {
      bb = b.elementAt(i);
    } catch (e) {
      bb = 0;
    }

    buffer[i] = aa ^ bb;
  }

  return buffer;
}
