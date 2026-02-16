import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:mysql_client_fork/src/mysql_client/connection.dart';

abstract class Session {
  /// Whether this connection is currently open.
  ///
  /// A [Connection] is open until it's closed (either by an explicit
  /// [Connection.close] call or due to an unrecoverable error from the server).
  /// Other sessions, such as transactions or connections borrowed from a pool,
  /// may have a shorter lifetime.
  ///
  /// The [closed] future can be awaited to get notified when this session is
  /// closing.
  bool get isOpen;

  /// A future that completes when [isOpen] turns false.
  Future<void> get closed;

  /// Prepares a reusable statement from a [query].
  Future<PreparedStmt> prepare(String query, {bool iterable = false});

  /// Executes the [query] with the given [params].
  Future<IResultSet> execute(String query, {Map<String, dynamic>? params, bool iterable = false});
}

class ConnectionSettings extends SessionSettings {
  final String? applicationName;
  final String? timeZone;
  final Encoding? encoding;

  /// The [SecurityContext] to use when opening a connection.
  final SecurityContext? securityContext;

  const ConnectionSettings({
    this.applicationName,
    this.timeZone,
    this.encoding,
    this.securityContext,
    super.connectTimeout,
    super.queryTimeout,
  });
}

class SessionSettings {
  // Duration(seconds: 15)
  final Duration? connectTimeout;

  // Duration(minutes: 5)
  final Duration? queryTimeout;

  const SessionSettings({this.connectTimeout, this.queryTimeout});
}
