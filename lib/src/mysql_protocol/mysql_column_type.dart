import 'dart:typed_data';

import 'package:mysql_client_fork/exception.dart';
import 'package:mysql_client_fork/mysql_protocol_extension.dart';
import 'package:tuple/tuple.dart';

const int mysqlColumnTypeDecimal = 0x00;
const int mysqlColumnTypeTiny = 0x01;
const int mysqlColumnTypeShort = 0x02;
const int mysqlColumnTypeLong = 0x03;
const int mysqlColumnTypeFloat = 0x04;
const int mysqlColumnTypeDouble = 0x05;
const int mysqlColumnTypeNull = 0x06;
const int mysqlColumnTypeTimestamp = 0x07;
const int mysqlColumnTypeLongLong = 0x08;
const int mysqlColumnTypeInt24 = 0x09;
const int mysqlColumnTypeDate = 0x0a;
const int mysqlColumnTypeTime = 0x0b;
const int mysqlColumnTypeDateTime = 0x0c;
const int mysqlColumnTypeYear = 0x0d;
const int mysqlColumnTypeNewDate = 0x0e;
const int mysqlColumnTypeVarChar = 0x0f;
const int mysqlColumnTypeBit = 0x10;
const int mysqlColumnTypeTimestamp2 = 0x11;
const int mysqlColumnTypeDateTime2 = 0x12;
const int mysqlColumnTypeTime2 = 0x13;
const int mysqlColumnTypeNewDecimal = 0xf6;
const int mysqlColumnTypeEnum = 0xf7;
const int mysqlColumnTypeSet = 0xf8;
const int mysqlColumnTypeTinyBlob = 0xf9;
const int mysqlColumnTypeMediumBlob = 0xfa;
const int mysqlColumnTypeLongBlob = 0xfb;
const int mysqlColumnTypeBlob = 0xfc;
const int mysqlColumnTypeVarString = 0xfd;
const int mysqlColumnTypeString = 0xfe;
const int mysqlColumnTypeGeometry = 0xff;

class MySQLColumnType {
  final int _value;

  const MySQLColumnType._(int value) : _value = value;

  factory MySQLColumnType.create(int value) => MySQLColumnType._(value);

  int get intVal => _value;

  static const MySQLColumnType decimalType = MySQLColumnType._(mysqlColumnTypeDecimal);
  static const MySQLColumnType tinyType = MySQLColumnType._(mysqlColumnTypeTiny);
  static const MySQLColumnType shortType = MySQLColumnType._(mysqlColumnTypeShort);
  static const MySQLColumnType longType = MySQLColumnType._(mysqlColumnTypeLong);
  static const MySQLColumnType floatType = MySQLColumnType._(mysqlColumnTypeFloat);
  static const MySQLColumnType doubleType = MySQLColumnType._(mysqlColumnTypeDouble);
  static const MySQLColumnType nullType = MySQLColumnType._(mysqlColumnTypeNull);
  static const MySQLColumnType timestampType = MySQLColumnType._(mysqlColumnTypeTimestamp);
  static const MySQLColumnType longLongType = MySQLColumnType._(mysqlColumnTypeLongLong);
  static const MySQLColumnType int24Type = MySQLColumnType._(mysqlColumnTypeInt24);
  static const MySQLColumnType dateType = MySQLColumnType._(mysqlColumnTypeDate);
  static const MySQLColumnType timeType = MySQLColumnType._(mysqlColumnTypeTime);
  static const MySQLColumnType dateTimeType = MySQLColumnType._(mysqlColumnTypeDateTime);
  static const MySQLColumnType yearType = MySQLColumnType._(mysqlColumnTypeYear);
  static const MySQLColumnType newDateType = MySQLColumnType._(mysqlColumnTypeNewDate);
  static const MySQLColumnType vatChartType = MySQLColumnType._(mysqlColumnTypeVarChar);
  static const MySQLColumnType bitType = MySQLColumnType._(mysqlColumnTypeBit);
  static const MySQLColumnType timestamp2Type = MySQLColumnType._(mysqlColumnTypeTimestamp2);
  static const MySQLColumnType dateTime2Type = MySQLColumnType._(mysqlColumnTypeDateTime2);
  static const MySQLColumnType time2Type = MySQLColumnType._(mysqlColumnTypeTime2);
  static const MySQLColumnType newDecimalType = MySQLColumnType._(mysqlColumnTypeNewDecimal);
  static const MySQLColumnType enumType = MySQLColumnType._(mysqlColumnTypeEnum);
  static const MySQLColumnType setType = MySQLColumnType._(mysqlColumnTypeSet);
  static const MySQLColumnType tinyBlobType = MySQLColumnType._(mysqlColumnTypeTinyBlob);
  static const MySQLColumnType mediumBlobType = MySQLColumnType._(mysqlColumnTypeMediumBlob);
  static const MySQLColumnType longBlobType = MySQLColumnType._(mysqlColumnTypeLongBlob);
  static const MySQLColumnType blocType = MySQLColumnType._(mysqlColumnTypeBlob);
  static const MySQLColumnType varStringType = MySQLColumnType._(mysqlColumnTypeVarString);
  static const MySQLColumnType stringType = MySQLColumnType._(mysqlColumnTypeString);
  static const MySQLColumnType geometryType = MySQLColumnType._(mysqlColumnTypeGeometry);

  T? convertStringValueToProvidedType<T>(String? value, [int? columnLength]) {
    if (value == null) {
      return null;
    }

    if (T == String || T == dynamic) {
      return value as T;
    }

    if (T == bool) {
      if (_value == mysqlColumnTypeTiny && columnLength == 1) {
        return int.parse(value) > 0 as T;
      } else {
        throw MySQLProtocolException('Can not convert MySQL type $_value to requested type bool');
      }
    }

    // convert to int
    if (T == int) {
      switch (_value) {
        // types convertible to dart int
        case mysqlColumnTypeTiny:
        case mysqlColumnTypeShort:
        case mysqlColumnTypeLong:
        case mysqlColumnTypeLongLong:
        case mysqlColumnTypeInt24:
        case mysqlColumnTypeYear:
          return int.parse(value) as T;
        default:
          throw MySQLProtocolException('Can not convert MySQL type $_value to requested type int');
      }
    }

    if (T == double) {
      switch (_value) {
        case mysqlColumnTypeTiny:
        case mysqlColumnTypeShort:
        case mysqlColumnTypeLong:
        case mysqlColumnTypeLongLong:
        case mysqlColumnTypeInt24:
        case mysqlColumnTypeFloat:
        case mysqlColumnTypeDouble:
          return double.parse(value) as T;
        default:
          throw MySQLProtocolException('Can not convert MySQL type $_value to requested type double');
      }
    }

    if (T == num) {
      switch (_value) {
        case mysqlColumnTypeTiny:
        case mysqlColumnTypeShort:
        case mysqlColumnTypeLong:
        case mysqlColumnTypeLongLong:
        case mysqlColumnTypeInt24:
        case mysqlColumnTypeFloat:
        case mysqlColumnTypeDouble:
          return num.parse(value) as T;
        default:
          throw MySQLProtocolException('Can not convert MySQL type $_value to requested type num');
      }
    }

    if (T == DateTime) {
      switch (_value) {
        case mysqlColumnTypeDate:
        case mysqlColumnTypeDateTime2:
        case mysqlColumnTypeDateTime:
        case mysqlColumnTypeTimestamp:
        case mysqlColumnTypeTimestamp2:
          return DateTime.parse(value) as T;
        default:
          throw MySQLProtocolException('Can not convert MySQL type $_value to requested type DateTime');
      }
    }

    throw MySQLProtocolException('Can not convert MySQL type $_value to requested type ${T.runtimeType}');
  }

  Type getBestMatchDartType(int columnLength) {
    switch (_value) {
      case mysqlColumnTypeString:
      case mysqlColumnTypeVarString:
      case mysqlColumnTypeVarChar:
      case mysqlColumnTypeEnum:
      case mysqlColumnTypeSet:
      case mysqlColumnTypeLongBlob:
      case mysqlColumnTypeMediumBlob:
      case mysqlColumnTypeBlob:
      case mysqlColumnTypeTinyBlob:
      case mysqlColumnTypeGeometry:
      case mysqlColumnTypeBit:
      case mysqlColumnTypeDecimal:
      case mysqlColumnTypeNewDecimal:
        return String;
      case mysqlColumnTypeTiny:
        if (columnLength == 1) {
          return bool;
        } else {
          return int;
        }
      case mysqlColumnTypeShort:
      case mysqlColumnTypeLong:
      case mysqlColumnTypeLongLong:
      case mysqlColumnTypeInt24:
      case mysqlColumnTypeYear:
        return int;
      case mysqlColumnTypeFloat:
      case mysqlColumnTypeDouble:
        return double;
      case mysqlColumnTypeDate:
      case mysqlColumnTypeDateTime2:
      case mysqlColumnTypeDateTime:
      case mysqlColumnTypeTimestamp:
      case mysqlColumnTypeTimestamp2:
        return DateTime;
      default:
        return String;
    }
  }
}

Tuple2<String, int> parseBinaryColumnData(int columnType, ByteData data, Uint8List buffer, int startOffset) {
  switch (columnType) {
    case mysqlColumnTypeTiny:
      final int value = data.getInt8(startOffset);
      return Tuple2(value.toString(), 1);
    case mysqlColumnTypeShort:
      final int value = data.getInt16(startOffset, Endian.little);
      return Tuple2(value.toString(), 2);
    case mysqlColumnTypeLong:
    case mysqlColumnTypeInt24:
      final int value = data.getInt32(startOffset, Endian.little);
      return Tuple2(value.toString(), 4);
    case mysqlColumnTypeLongLong:
      final int value = data.getInt64(startOffset, Endian.little);
      return Tuple2(value.toString(), 8);
    case mysqlColumnTypeFloat:
      final double value = data.getFloat32(startOffset, Endian.little);
      return Tuple2(value.toString(), 4);
    case mysqlColumnTypeDouble:
      final double value = data.getFloat64(startOffset, Endian.little);
      return Tuple2(value.toString(), 8);
    case mysqlColumnTypeDate:
    case mysqlColumnTypeDateTime:
    case mysqlColumnTypeTimestamp:
      final int initialOffset = startOffset;

      // read number of bytes (0, 4, 7, 11)
      final int numOfBytes = data.getUint8(startOffset);
      startOffset += 1;

      if (numOfBytes == 0) {
        return const Tuple2('0000-00-00 00:00:00', 1);
      }

      int year = 0;
      int month = 0;
      int day = 0;
      int hour = 0;
      int minute = 0;
      int second = 0;
      int microSecond = 0;

      if (numOfBytes >= 4) {
        year = data.getUint16(startOffset, Endian.little);
        startOffset += 2;

        month = data.getUint8(startOffset);
        startOffset += 1;

        day = data.getUint8(startOffset);
        startOffset += 1;
      }

      if (numOfBytes >= 7) {
        hour = data.getUint8(startOffset);
        startOffset += 1;

        minute = data.getUint8(startOffset);
        startOffset += 1;

        second = data.getUint8(startOffset);
        startOffset += 1;
      }

      if (numOfBytes >= 11) {
        microSecond = data.getUint32(startOffset, Endian.little);
        startOffset += 4;
      }

      final StringBuffer result = StringBuffer();
      result.write('$year-');
      result.write('${month.toString().padLeft(2, '0')}-');
      result.write('${day.toString().padLeft(2, '0')} ');
      result.write('${hour.toString().padLeft(2, '0')}:');
      result.write('${minute.toString().padLeft(2, '0')}:');
      result.write('${second.toString().padLeft(2, '0')}.');
      result.write(microSecond.toString());

      return Tuple2(result.toString(), startOffset - initialOffset);
    case mysqlColumnTypeTime:
      final int initialOffset = startOffset;

      // read number of bytes (0, 8, 12)
      final int numOfBytes = data.getUint8(startOffset);
      startOffset += 1;

      if (numOfBytes == 0) {
        return const Tuple2('00:00:00', 1);
      }

      bool isNegative = false;
      int days = 0;
      int hours = 0;
      int minutes = 0;
      int seconds = 0;
      int microSecond = 0;

      if (numOfBytes >= 8) {
        isNegative = data.getUint8(startOffset) > 0;
        startOffset += 1;

        days = data.getUint32(startOffset, Endian.little);
        startOffset += 4;

        hours = data.getUint8(startOffset);
        startOffset += 1;

        minutes = data.getUint8(startOffset);
        startOffset += 1;

        seconds = data.getUint8(startOffset);
        startOffset += 1;
      }

      if (numOfBytes >= 12) {
        microSecond = data.getUint32(startOffset, Endian.little);
        startOffset += 4;
      }

      hours += days * 24;

      final StringBuffer result = StringBuffer();
      if (isNegative) {
        result.write('-');
      }
      result.write('${hours.toString().padLeft(2, '0')}:');
      result.write('${minutes.toString().padLeft(2, '0')}:');
      result.write('${seconds.toString().padLeft(2, '0')}.');
      result.write(microSecond.toString());

      return Tuple2(result.toString(), startOffset - initialOffset);
    case mysqlColumnTypeString:
    case mysqlColumnTypeVarString:
    case mysqlColumnTypeVarChar:
    case mysqlColumnTypeEnum:
    case mysqlColumnTypeSet:
    case mysqlColumnTypeLongBlob:
    case mysqlColumnTypeMediumBlob:
    case mysqlColumnTypeBlob:
    case mysqlColumnTypeTinyBlob:
    case mysqlColumnTypeGeometry:
    case mysqlColumnTypeBit:
    case mysqlColumnTypeDecimal:
    case mysqlColumnTypeNewDecimal:
      return buffer.getUtf8LengthEncodedString(startOffset);
  }

  throw MySQLProtocolException('Can not parse binary column data: column type $columnType is not implemented');
}
