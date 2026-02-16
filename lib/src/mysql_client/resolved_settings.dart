import 'dart:convert';
import 'dart:io';

import 'package:mysql_client_fork/src/mysql_client/mysql.dart';
import 'package:mysql_client_fork/src/mysql_client/pool.dart';

class ResolvedSessionSettings implements SessionSettings {
  ResolvedSessionSettings(SessionSettings? settings, SessionSettings? fallback)
      : connectTimeout = settings?.connectTimeout ?? fallback?.connectTimeout ?? const Duration(seconds: 15),
        queryTimeout = settings?.queryTimeout ?? fallback?.queryTimeout ?? const Duration(minutes: 5);
  @override
  final Duration connectTimeout;
  @override
  final Duration queryTimeout;

  bool isMatchingSession(ResolvedSessionSettings other) {
    return connectTimeout == other.connectTimeout && queryTimeout == other.queryTimeout;
  }
}

class ResolvedConnectionSettings extends ResolvedSessionSettings implements ConnectionSettings {
  ResolvedConnectionSettings(ConnectionSettings? super.settings, ConnectionSettings? super.fallback)
      : applicationName = settings?.applicationName ?? fallback?.applicationName,
        timeZone = settings?.timeZone ?? fallback?.timeZone ?? 'UTC',
        encoding = settings?.encoding ?? fallback?.encoding ?? utf8,
        securityContext = settings?.securityContext;
  @override
  final String? applicationName;
  @override
  final String timeZone;
  @override
  final Encoding encoding;
  @override
  final SecurityContext? securityContext;

  bool isMatchingConnection(ResolvedConnectionSettings other) {
    return isMatchingSession(other) &&
        applicationName == other.applicationName &&
        timeZone == other.timeZone &&
        encoding == other.encoding;
  }
}

class ResolvedPoolSettings extends ResolvedConnectionSettings implements PoolSettings {
  ResolvedPoolSettings(PoolSettings? settings)
      : maxConnectionCount = settings?.maxConnectionCount ?? 1,
        maxConnectionAge = settings?.maxConnectionAge ?? const Duration(hours: 12),
        maxSessionUse = settings?.maxSessionUse ?? const Duration(hours: 4),
        maxQueryCount = settings?.maxQueryCount ?? 100000,
        super(settings, null);
  @override
  final int maxConnectionCount;
  @override
  final Duration maxConnectionAge;
  @override
  final Duration maxSessionUse;
  @override
  final int maxQueryCount;
}
