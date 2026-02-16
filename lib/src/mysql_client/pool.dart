import 'dart:async';
import 'dart:developer';

import 'package:collection/collection.dart';
import 'package:mysql_client_fork/exception.dart';
import 'package:mysql_client_fork/mysql_client.dart';
import 'package:mysql_client_fork/src/mysql_client/mysql.dart';
import 'package:mysql_client_fork/src/mysql_client/resolved_settings.dart';
import 'package:mysql_client_fork/src/mysql_client/util/retry.dart';
import 'package:pool/pool.dart' as pool;

// We reference members that are within the library even if they are outside of the class's scope.
// ignore_for_file: comment_references

class PoolSettings extends ConnectionSettings {
  const PoolSettings({
    super.connectTimeout,
    this.maxConnectionCount,
    this.maxConnectionAge,
    this.maxSessionUse,
    this.maxQueryCount,
  });

  /// The maximum number of concurrent sessions.
  final int? maxConnectionCount;

  /// The maximum duration a connection is kept open.
  /// New sessions won't be scheduled after this limit is reached.
  final Duration? maxConnectionAge;

  /// The maximum duration a connection is used by sessions.
  /// New sessions won't be scheduled after this limit is reached.
  final Duration? maxSessionUse;

  /// The maximum number of queries to be run on a connection.
  /// New sessions won't be scheduled after this limit is reached.
  ///
  /// NOTE: not yet implemented
  final int? maxQueryCount;
}

final Set<MySQLClientExceptionCode> _retriableExceptionCodes = <MySQLClientExceptionCode>{
  MySQLClientExceptionCode.closedConnection,
  MySQLClientExceptionCode.brokenConnection,
};

/// Class to create and manage pool of database connections
class MySQLConnectionPool implements Session {
  /// Creates new Pool
  ///
  /// Almost all parameters are identical to [MySQLConnection.createConnection]
  /// Pass [maxConnections] to tell pool maximum number of connections it can use
  /// You can specify [timeoutMs], it will be passed to [MySQLConnection.connect] method when creating new connections
  MySQLConnectionPool({required this.endpoint, this.collation = 'utf8_general_ci', PoolSettings? settings})
      : _settings = ResolvedPoolSettings(settings);

  final Endpoint endpoint;
  final String collation;
  final ResolvedPoolSettings _settings;

  final List<_PoolConnection> _connections = <_PoolConnection>[];
  late final int _maxConnectionCount = _settings.maxConnectionCount;
  late final pool.Pool _semaphore = pool.Pool(_maxConnectionCount, timeout: _settings.connectTimeout);
  late final pool.Pool _connectLock = pool.Pool(1, timeout: _settings.connectTimeout);

  @override
  bool get isOpen => !_semaphore.isClosed;

  @override
  Future<void> get closed => _semaphore.done;

  /// Closes all connections in this pool and frees resources
  Future<void> close() async {
    await _semaphore.close();

    // Connections are closed when they are returned to the pool if it's closed.
    // We still need to close statements that are currently unused.
    for (final _PoolConnection connection in <_PoolConnection>[..._connections]) {
      if (!connection._isInUse) {
        await connection._dispose();
      }
    }
  }

  /// See [MySQLConnection.execute]
  @override
  Future<IResultSet> execute(String query, {Map<String, dynamic>? params, bool iterable = false}) {
    return retry(
      () async {
        return withConnection((MySQLConnection connection) async {
          return connection.execute(query, params: params, iterable: iterable);
        });
      },
      retryIf: (Object error) {
        if (error is MySQLClientException && _retriableExceptionCodes.contains(error.code)) {
          return true;
        }
        return false;
      },
    );
  }

  @override
  Future<PreparedStmt> prepare(String query, {bool iterable = false}) async {
    final Completer<PreparedStmt> statementCompleter = Completer<PreparedStmt>();

    unawaited(
      withConnection((MySQLConnection connection) async {
        PreparedStmt? poolStatement;

        try {
          poolStatement = await connection.prepare(query);
        } on Object catch (e, s) {
          // Could not prepare the statement, inform the caller and stop occupying
          // the connection.
          statementCompleter.completeError(e, s);
          return;
        }

        // Otherwise, make the future returned by prepare complete with the
        // statement.
        statementCompleter.complete(poolStatement);

        // And keep this connection reserved until the statement has been disposed.
        return poolStatement.disposed.future;
      }),
    );

    return statementCompleter.future;
  }

  /// Get free connection from this pool (possibly new connection) and invoke callback function with this connection
  ///
  /// After callback completes, connection is returned into pool as idle connection
  /// This function returns callback result
  Future<R> withConnection<R>(Future<R> Function(MySQLConnection connection) fn, {ConnectionSettings? settings}) async {
    final pool.PoolResource resource = await _semaphore.request();
    _PoolConnection? connection;
    bool reuse = true;
    final Stopwatch sw = Stopwatch();
    try {
      try {
        // Find an existing connection that is currently unused, or open another
        // one.
        connection = await _selectOrCreate(endpoint, ResolvedConnectionSettings(settings, _settings));
      } on TimeoutException catch (exception) {
        throw Error.throwWithStackTrace(
          MySQLClientException.timeout(
            message: 'The connection was not established within the time limit',
            duration: exception.duration ?? Duration.zero,
          ),
          StackTrace.current,
        );
      }

      sw.start();
      try {
        // The `await` here is important to ensure that the connection's state
        // is updated before releasing it back to the pool.
        return await fn(connection);
      } catch (error, stackTrace) {
        log('withConnection - Error: $error, StackTrace: $stackTrace');
        if (error is MySQLClientException) {
          reuse = false;
        }
        rethrow;
      }
    } finally {
      resource.release();
      sw.stop();

      // If the pool has been closed, this connection needs to be closed as
      // well.
      if (connection != null) {
        connection._elapsedInUse += sw.elapsed;
        if (_semaphore.isClosed || !reuse || !connection.isOpen) {
          await connection._dispose();
        } else {
          // Allow the connection to be re-used later.
          connection
            .._isInUse = false
            .._lastReturned = DateTime.now();
        }
      }
    }
  }

  Future<_PoolConnection> _selectOrCreate(Endpoint endpoint, ResolvedConnectionSettings settings) async {
    final _PoolConnection? oldC = _connections.firstWhereOrNull((_PoolConnection c) => c._mayReuse(endpoint, settings));
    if (oldC != null) {
      // NOTE: It is important to update the _isInUse flag here, otherwise
      //       race conditions may create conflicts.
      oldC._isInUse = true;
      return oldC;
    }

    return _connectLock.withResource(() async {
      while (_connections.length > _maxConnectionCount) {
        final List<_PoolConnection> candidates = _connections.where((_PoolConnection c) => !c._isInUse).toList();
        if (candidates.isEmpty) {
          throw StateError('The pool should not be in this state.');
        }
        final _PoolConnection selected = candidates.reduce(
          (_PoolConnection a, _PoolConnection b) => a._lastReturned.isBefore(b._lastReturned) ? a : b,
        );
        await selected._dispose();
      }

      final MySQLConnection sqlConnection = await MySQLConnection.createConnection(endpoint: endpoint);
      await sqlConnection.connect();
      final _PoolConnection newConnection = _PoolConnection(this, endpoint, settings, sqlConnection) //
        .._isInUse = true;
      // NOTE: It is important to update _connections list after the isInUse
      //       flag is set, otherwise race conditions may create conflicts or
      //       pool close may miss the connection.
      _connections.add(newConnection);
      return newConnection;
    });
  }

  /// See [MySQLConnection.transactional]
  Future<T> transactional<T>(FutureOr<T> Function(MySQLConnection conn) callback) async {
    return retry(
      () async => withConnection((MySQLConnection conn) => conn.transactional(callback)),
      // Retry for cases when the connection gets closed/broken during the transaction execution.
      retryIf: (Object error) {
        if (error is MySQLClientException && _retriableExceptionCodes.contains(error.code)) {
          return true;
        }
        return false;
      },
    );
  }
}

class _PoolConnection implements MySQLConnection {
  _PoolConnection(this._pool, this._endpoint, this._connectionSettings, this._connection);

  final DateTime _opened = DateTime.now();
  final MySQLConnectionPool _pool;
  final Endpoint _endpoint;
  final ResolvedConnectionSettings _connectionSettings;
  final MySQLConnection _connection;
  Duration _elapsedInUse = Duration.zero;
  DateTime _lastReturned = DateTime.now();
  bool _isInUse = false;

  bool _mayReuse(Endpoint endpoint, ResolvedConnectionSettings settings) {
    if (_isInUse || endpoint != _endpoint || _isExpired()) {
      return false;
    }
    if (!_connectionSettings.isMatchingConnection(settings)) {
      return false;
    }
    return true;
  }

  bool _isExpired() {
    final Duration age = DateTime.now().difference(_opened);
    if (age >= _pool._settings.maxConnectionAge) {
      return true;
    }
    if (_elapsedInUse >= _pool._settings.maxSessionUse) {
      return true;
    }
    return false;
  }

  Future<void> _dispose() async {
    _pool._connections.remove(this);
    await _connection.close();
  }

  @override
  bool get isOpen => _connection.isOpen;

  @override
  Future<void> get closed => _connection.closed;

  @override
  Future<void> close() async {
    // Don't forward the close call, the underlying connection should be re-used
    // when another pool connection is requested.
  }

  @override
  Future<IResultSet> execute(String query, {Map<String, dynamic>? params, bool iterable = false}) {
    return _connection.execute(query, params: params, iterable: iterable);
  }

  @override
  Future<PreparedStmt> prepare(String query, {bool iterable = false}) {
    return _connection.prepare(query, iterable: iterable);
  }

  @override
  Future<T> transactional<T>(FutureOr<T> Function(MySQLConnection conn) callback) {
    return _connection.transactional(callback);
  }

  @override
  Future<void> connect() => _connection.connect();

  @override
  bool get inTransaction => _connection.inTransaction;

  @override
  void onClose(void Function() callback) => _connection.onClose(callback);
}
