import 'dart:convert';
import 'dart:typed_data';
import 'package:buffer/buffer.dart';
import 'package:mysql_client_fork/mysql_protocol.dart';
import 'package:mysql_client_fork/mysql_protocol_extension.dart';

const int _supportedCapabilities = mysqlCapFlagClientProtocol41 |
    mysqlCapFlagClientSecureConnection |
    mysqlCapFlagClientPluginAuth |
    mysqlCapFlagClientPluginAuthLenEncClientData |
    mysqlCapFlagClientMultiStatements |
    mysqlCapFlagClientMultiResults;

class MySQLPacketHandshakeResponse41 extends MySQLPacketPayload {
  MySQLPacketHandshakeResponse41({
    required this.capabilityFlags,
    required this.maxPacketSize,
    required this.characterSet,
    required this.authResponse,
    required this.authPluginName,
    required this.username,
    this.database,
  });

  factory MySQLPacketHandshakeResponse41.createWithNativePassword({
    required String username,
    required String password,
    required MySQLPacketInitialHandshake initialHandshakePayload,
  }) {
    assert(initialHandshakePayload.authPluginDataPart2 != null);
    assert(initialHandshakePayload.authPluginName != null);

    final List<int> challenge =
        initialHandshakePayload.authPluginDataPart1 + initialHandshakePayload.authPluginDataPart2!.sublist(0, 12);

    assert(challenge.length == 20);

    final Uint8List passwordBytes = utf8.encode(password);

    final Uint8List authData = xor(sha1(passwordBytes), sha1(challenge + sha1(sha1(passwordBytes))));

    return MySQLPacketHandshakeResponse41(
      capabilityFlags: _supportedCapabilities,
      maxPacketSize: 50 * 1024 * 1024,
      authPluginName: initialHandshakePayload.authPluginName!,
      characterSet: initialHandshakePayload.charset,
      authResponse: authData,
      username: username,
    );
  }

  factory MySQLPacketHandshakeResponse41.createWithCachingSha2Password({
    required String username,
    required String password,
    required MySQLPacketInitialHandshake initialHandshakePayload,
  }) {
    final List<int> challenge =
        initialHandshakePayload.authPluginDataPart1 + initialHandshakePayload.authPluginDataPart2!.sublist(0, 12);

    assert(challenge.length == 20);

    final Uint8List passwordBytes = utf8.encode(password);

    final Uint8List authData = xor(sha256(passwordBytes), sha256(sha256(sha256(passwordBytes)) + challenge));

    return MySQLPacketHandshakeResponse41(
      capabilityFlags: _supportedCapabilities,
      maxPacketSize: 50 * 1024 * 1024,
      authPluginName: initialHandshakePayload.authPluginName!,
      characterSet: initialHandshakePayload.charset,
      authResponse: authData,
      username: username,
    );
  }

  int capabilityFlags;
  int maxPacketSize;
  int characterSet;
  Uint8List authResponse;
  String authPluginName;
  String username;
  String? database;

  @override
  Uint8List encode() {
    final ByteDataWriter buffer = ByteDataWriter(endian: Endian.little);

    if (database != null) {
      capabilityFlags = capabilityFlags | mysqlCapFlagClientConnectWithDB;
    }

    buffer
      ..writeUint32(capabilityFlags)
      ..writeUint32(maxPacketSize)
      ..writeUint8(characterSet)
      ..write(List<int>.filled(23, 0))
      ..write(utf8.encode(username))
      ..writeUint8(0);

    if (capabilityFlags & mysqlCapFlagClientSecureConnection != 0) {
      buffer
        ..writeVariableEncInt(authResponse.lengthInBytes)
        ..write(authResponse);
    }

    if (database != null && capabilityFlags & mysqlCapFlagClientConnectWithDB != 0) {
      buffer
        ..write(utf8.encode(database!))
        ..writeUint8(0);
    }

    if (capabilityFlags & mysqlCapFlagClientPluginAuth != 0) {
      buffer
        ..write(utf8.encode(authPluginName))
        ..writeUint8(0);
    }

    return buffer.toBytes();
  }
}
