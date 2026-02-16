import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:mysql_client_fork/exception.dart';
import 'package:mysql_client_fork/mysql_client.dart';
import 'package:mysql_client_fork/mysql_protocol.dart';
import 'package:mysql_client_fork/src/mysql_client/mysql.dart';
import 'package:pool/pool.dart' as pool;
import 'package:rxdart/rxdart.dart';

// We reference members that are within the library even if they are outside of the class's scope.
// ignore_for_file: comment_references

/// Main class to interact with MySQL database
///
/// Use [MySQLConnection.createConnection] to create connection
class MySQLConnection extends Session {
  MySQLConnection._({
    required Socket socket,
    required Endpoint endpoint,
    String collation = 'utf8_general_ci',
    Duration? timeoutDuration,
  })  : _socket = socket,
        _endpoint = endpoint,
        _collation = collation,
        _stateSubject = BehaviorSubject<_MySQLConnectionState>.seeded(_MySQLConnectionState.fresh),
        _timeoutMs = timeoutDuration ?? _defaultTimeout;

  // Constants
  static const Duration _defaultTimeout = Duration(seconds: 15);
  static const Duration _socketFlushDelay = Duration(milliseconds: 10);
  static const int _authChallengeLength = 20;
  static const int _maxIncompleteBufferSize = 16 * 1024 * 1024 /*16 MB*/;

  Socket _socket;
  final Endpoint _endpoint;
  final String _collation;
  final BehaviorSubject<_MySQLConnectionState> _stateSubject;
  final Duration _timeoutMs;

  final List<void Function()> _onCloseCallbacks = <void Function()>[];
  final List<int> _incompleteBufferData = <int>[];
  final Set<PreparedStmt> _activePreparedStatements = <PreparedStmt>{};

  StreamSubscription<Uint8List>? _socketSubscription;
  Completer<IResultSet> _executeCompleter = Completer<IResultSet>();
  Future<void>? _onDone;
  Future<void> Function(Uint8List data)? _responseCallback;
  String? _activeAuthPluginName;

  int _serverCapabilities = 0;
  bool _inTransaction = false;
  bool _isClosing = false;

  /// The lock to guard operations that must run sequentially, like sending
  /// RPC messages to the MySql server and waiting for them to complete.
  ///
  /// Each session base has its own operation lock, but child sessions hold the
  /// parent lock while they are active. For instance, when starting a
  /// transaction,the [_operationLock] of the connection is held until the
  /// transaction completes. This ensures that no other statement can use the
  /// connection in the meantime.
  final pool.Pool _operationLock = pool.Pool(1);
  final Completer<void> _sessionClosedCompleter = Completer<void>();

  @override
  Future<void> get closed => _sessionClosedCompleter.future;

  final List<_MySQLConnectionState> _availableConnectionStates = <_MySQLConnectionState>[
    _MySQLConnectionState.connectionEstablished,
    _MySQLConnectionState.waitingCommandResponse,
  ];

  @override
  bool get isOpen {
    final bool isConnected = _availableConnectionStates.contains(_state);
    return !_sessionClosed && !_isClosing && isConnected;
  }

  bool get _sessionClosed => _sessionClosedCompleter.isCompleted;

  bool get _secure => _endpoint.secure;

  /// Returns true if this connection is executing a transaction
  bool get inTransaction => _inTransaction;

  _MySQLConnectionState get _state => _stateSubject.value;

  /// Creates connection with provided options.
  ///
  /// Keep in mind, **this is async** function. So you need to await the result.
  /// Don't forget to call [MySQLConnection.connect] to actually connect to database, or you will get errors.
  /// See examples directory for code samples.
  ///
  /// By default after connection is established, this library executes query to switch connection charset and collation:
  ///
  /// ```mysql
  /// SET @@collation_connection=$_collation, @@character_set_client=utf8mb4, @@character_set_connection=utf8mb4, @@character_set_results=utf8mb4
  /// ```
  static Future<MySQLConnection> createConnection({required Endpoint endpoint, Duration? timeoutDuration}) async {
    final Socket socket = await Socket.connect(endpoint.host, endpoint.port);
    if (socket.address.type != InternetAddressType.unix) {
      // no support for extensions on sockets
      socket.setOption(SocketOption.tcpNoDelay, true);
    }

    return MySQLConnection._(socket: socket, endpoint: endpoint, timeoutDuration: timeoutDuration);
  }

  /// Initiate connection to database. To close connection, invoke [MySQLConnection.close] method.
  ///
  /// Default [timeoutMs] is 10000 milliseconds
  Future<void> connect() async {
    if (!_state.isFresh) {
      throw MySQLClientException(
        'Can not connect: status is not fresh',
        StackTrace.current,
        code: MySQLClientExceptionCode.unexpectedState,
      );
    }

    _stateSubject.add(_MySQLConnectionState.waitInitialHandshake);
    _socketSubscription = _socket.listen(
      (Uint8List data) async {
        for (final Uint8List chunk in _splitPackets(data)) {
          try {
            await _processSocketData(chunk);
          } catch (error, stackTrace) {
            _stateSubject.addError(error, stackTrace);
          }
        }
      },
      onError: (Object error, StackTrace stackTrace) async {
        _stateSubject.addError(error);
        await _close(interruptRunning: true, socketIsBroken: true);
      },
    );
    _onDone = _socket.done;
    unawaited(
      _onDone!.catchError((Object error, StackTrace stackTrace) async {
        if (error is SocketException && !_executeCompleter.isCompleted) {
          _executeCompleter.completeError(
            MySQLClientException(
              'Broken connection, try using a new connection. Error: $error',
              stackTrace,
              code: MySQLClientExceptionCode.brokenConnection,
            ),
          );
          await _close(interruptRunning: true, socketIsBroken: true);
        }
      }),
    );

    // Wait for connection established
    await _waitForState(_MySQLConnectionState.connectionEstablished).timeout(
      _timeoutMs,
      onTimeout: () => throw MySQLClientException.timeout(duration: _timeoutMs),
    );

    // Set connection charset
    await execute(
      'SET @@collation_connection=$_collation, @@character_set_client=utf8mb4, @@character_set_connection=utf8mb4, @@character_set_results=utf8mb4',
    );
  }

  /// Executes given [query]
  ///
  /// [execute] can be used to make any query type (SELECT, INSERT, UPDATE)
  /// You can pass named parameters using [params]
  /// Pass [iterable] true if you want to receive rows one by one in Stream fashion
  @override
  Future<IResultSet> execute(
    String query, {
    Map<String, dynamic>? params,
    bool iterable = false,
  }) async {
    // Use operation lock to ensure queries execute sequentially
    Future<IResultSet> localExecute() async {
      if (query.isEmpty) {
        throw MySQLClientException(
          'Query cannot be empty',
          StackTrace.current,
          code: MySQLClientExceptionCode.invalidArgument,
        );
      }

      if (!isOpen) {
        throw MySQLClientException.connectionClosed(
          message: 'Cannot execute operation, connection is in open state: $_state, closing: $_isClosing',
          stackTrace: StackTrace.current,
        );
      }

      // Wait for ready state
      if (!_state.isConnectionEstablished) {
        await _waitForState(_MySQLConnectionState.connectionEstablished).timeout(
          _timeoutMs,
          onTimeout: () {
            throw MySQLClientException.timeout(duration: _timeoutMs, stackTrace: StackTrace.current);
          },
        );
      }

      _stateSubject.add(_MySQLConnectionState.waitingCommandResponse);
      if (params != null && params.isNotEmpty) {
        try {
          query = substituteParams(query, params);
        } catch (e) {
          _stateSubject.add(_MySQLConnectionState.connectionEstablished);
          rethrow;
        }
      }

      final MySQLPacketCommQuery payload = MySQLPacketCommQuery(query: query);
      final MySQLPacket packet = MySQLPacket(sequenceID: 0, payload: payload, payloadLength: 0);

      _executeCompleter = Completer<IResultSet>();

      /**
       * 0 - initial
       * 1 - columnCount decoded
       * 2 - columnDefs parsed
       * 3 - eofParsed
       * 4 - rowsParsed
       */
      int state = 0;
      int colsCount = 0;
      List<MySQLColumnDefinitionPacket> colDefs = <MySQLColumnDefinitionPacket>[];
      List<MySQLResultSetRowPacket> resultSetRows = <MySQLResultSetRowPacket>[];

      // support for iterable result set
      IterableResultSet? iterableResultSet;
      StreamSink<ResultSetRow>? sink;

      // used as a pointer to handle multiple result sets
      IResultSet? currentResultSet;
      IResultSet? firstResultSet;

      // This StackTrace is initialized outside of the bellow callback to make sure it includes this method's trace
      final StackTrace mysqlPacketErrorStackTrace = StackTrace.current;
      _responseCallback = (Uint8List data) async {
        try {
          MySQLPacket? packet;

          switch (state) {
            case 0:
              // if packet is OK packet, there is no data
              if (MySQLPacket.detectPacketType(data) == MySQLGenericPacketType.ok) {
                final MySQLPacket okPacket = MySQLPacket.decodeGenericPacket(data);
                _stateSubject.add(_MySQLConnectionState.connectionEstablished);
                _executeCompleter.complete(EmptyResultSet(okPacket: okPacket.payload as MySQLPacketOK));
                return;
              }

              packet = MySQLPacket.decodeColumnCountPacket(data);
            case 1:
              packet = MySQLPacket.decodeColumnDefPacket(data);
            case 2:
              packet = MySQLPacket.decodeGenericPacket(data);
              if (packet.isEOFPacket()) {
                state = 3;
              }
            case 3:
              if (iterable) {
                if (iterableResultSet == null) {
                  iterableResultSet = IterableResultSet(columns: colDefs);

                  sink = iterableResultSet!.sink;
                  _executeCompleter.complete(iterableResultSet);
                }

                // check eof
                if (MySQLPacket.detectPacketType(data) == MySQLGenericPacketType.eof) {
                  state = 4;

                  _stateSubject.add(_MySQLConnectionState.connectionEstablished);
                  await sink!.close();
                  return;
                }

                packet = MySQLPacket.decodeResultSetRowPacket(data, colsCount);
                final List<String?> values = (packet.payload as MySQLResultSetRowPacket).values;
                sink!.add(ResultSetRow(colDefs: colDefs, values: values));
                packet = null;
                break;
              } else {
                // check eof
                if (MySQLPacket.detectPacketType(data) == MySQLGenericPacketType.eof) {
                  final MySQLPacketResultSet resultSetPacket = MySQLPacketResultSet(
                    columnCount: BigInt.from(colsCount),
                    columns: colDefs,
                    rows: resultSetRows,
                  );

                  final ResultSet resultSet = ResultSet(resultSetPacket: resultSetPacket);

                  if (currentResultSet != null) {
                    currentResultSet!.next = resultSet;
                  } else {
                    firstResultSet = resultSet;
                  }
                  currentResultSet = resultSet;

                  final MySQLPacket eofPacket = MySQLPacket.decodeGenericPacket(data);
                  final MySQLPacketEOF eofPayload = eofPacket.payload as MySQLPacketEOF;

                  if (eofPayload.statusFlags & mysqlServerFlagMoreResultsExists != 0) {
                    state = 0;
                    colsCount = 0;
                    colDefs = <MySQLColumnDefinitionPacket>[];
                    resultSetRows = <MySQLResultSetRowPacket>[];
                    return;
                  } else {
                    // there is no more results, just return
                    state = 4;
                    _stateSubject.add(_MySQLConnectionState.connectionEstablished);
                    _executeCompleter.complete(firstResultSet);
                    return;
                  }
                }

                packet = MySQLPacket.decodeResultSetRowPacket(data, colsCount);
                break;
              }
          }

          if (packet != null) {
            final MySQLPacketPayload payload = packet.payload;

            if (payload is MySQLPacketError) {
              _stateSubject.add(_MySQLConnectionState.connectionEstablished);
              if (!_executeCompleter.isCompleted) {
                _executeCompleter.completeError(
                  MySQLServerException(
                    payload.errorMessage,
                    mysqlPacketErrorStackTrace,
                    payload.errorCode,
                    query,
                    params,
                  ),
                );
              }
              return;
            } else if (payload is MySQLPacketOK || payload is MySQLPacketEOF) {
              // do nothing
            } else if (payload is MySQLPacketColumnCount) {
              state = 1;
              colsCount = payload.columnCount.toInt();
              return;
            } else if (payload is MySQLColumnDefinitionPacket) {
              colDefs.add(payload);
              if (colDefs.length == colsCount) {
                state = 2;
              }
            } else if (payload is MySQLResultSetRowPacket) {
              assert(iterable == false, 'In iterable mode, rows are processed one by one in the callback');
              resultSetRows.add(payload);
            } else {
              if (!_executeCompleter.isCompleted) {
                _executeCompleter.completeError(
                  MySQLClientException(
                    'Unexpected payload received in response to COMM_QUERY request',
                    mysqlPacketErrorStackTrace,
                    code: MySQLClientExceptionCode.unexpectedPayload,
                  ),
                );
              }
              await _closeSocketAndCleanUp();
              return;
            }
          }
        } catch (e, s) {
          if (!_executeCompleter.isCompleted) {
            _executeCompleter.completeError(e, s);
          }
          await _closeSocketAndCleanUp();
        }
      };

      _socket.add(packet.encode());

      return _executeCompleter.future;
    }

    if (_inTransaction) {
      // If we are in a transaction, we are already holding the operation lock.
      return localExecute();
    }

    return _operationLock.withResource(localExecute);
  }

  /// Execute [callback] inside database transaction
  ///
  /// If any exception is thrown inside [callback] function, the transaction is rolled back.
  /// The exception is then rethrown after the rollback.
  Future<T> transactional<T>(FutureOr<T> Function(MySQLConnection conn) callback) async {
    // prevent double transaction
    if (_inTransaction) {
      throw MySQLClientException.unexpectedState(message: 'Already in transaction', stackTrace: StackTrace.current);
    }

    // Keep this connection locked while the transaction is active. We do that
    // because on a protocol level, the entire connection is in a transaction.
    // From a Dart point of view, methods called outside of the transaction
    // should not be able to view data in the transaction though. So we avoid
    // those outer calls while the transaction is active and resume them by
    // returning the operation lock in the end.
    return _operationLock.withResource(() async {
      _inTransaction = true;

      await execute('START TRANSACTION');
      try {
        final T result = await callback(this);
        await execute('COMMIT');
        return result;
      } catch (e) {
        // Rollback on any exception
        try {
          await execute('ROLLBACK');
        } catch (rollbackError) {
          // If rollback fails, log it but don't mask the original error
          // The original exception is more important
        }
        rethrow;
      } finally {
        _inTransaction = false;
      }
    });
  }

  /// Prepares given [query]
  ///
  /// Returns [PreparedStmt] which can be used to execute prepared statement multiple times with different parameters
  /// See [PreparedStmt.execute]
  /// You should call [PreparedStmt.deallocate] when you don't need prepared statement anymore to prevent memory leaks
  ///
  /// Pass [iterable] true if you want to iterable result set. See [execute] for details
  @override
  Future<PreparedStmt> prepare(String query, {bool iterable = false}) async {
    // Use operation lock to ensure operations execute sequentially
    Future<PreparedStmt> localPrepare() async {
      if (query.isEmpty) {
        throw MySQLClientException(
          'Query cannot be empty',
          StackTrace.current,
          code: MySQLClientExceptionCode.invalidArgument,
        );
      }

      if (!isOpen) {
        throw MySQLClientException.connectionClosed(
          message: 'Cannot execute operation, connection is in open state: $_state, closing: $_isClosing',
          stackTrace: StackTrace.current,
        );
      }

      // wait for ready state
      if (!_state.isConnectionEstablished) {
        await _waitForState(_MySQLConnectionState.connectionEstablished).timeout(
          _timeoutMs,
          onTimeout: () => throw MySQLClientException.timeout(duration: _timeoutMs, stackTrace: StackTrace.current),
        );
      }

      _stateSubject.add(_MySQLConnectionState.waitingCommandResponse);

      final MySQLPacketCommStmtPrepare payload = MySQLPacketCommStmtPrepare(query: query);

      final MySQLPacket packet = MySQLPacket(sequenceID: 0, payload: payload, payloadLength: 0);

      final Completer<PreparedStmt> completer = Completer<PreparedStmt>();

      /**
       * 0 - initial
       * 1 - first packet decoded
       * 2 - eof decoded
       */
      int state = 0;
      int numOfEofPacketsParsed = 0;
      MySQLPacketStmtPrepareOK? preparedPacket;

      _responseCallback = (Uint8List data) async {
        try {
          MySQLPacket? packet;

          switch (state) {
            case 0:
              packet = MySQLPacket.decodeCommPrepareStmtResponsePacket(data);
              state = 1;
            default:
              packet = null;

              if (MySQLPacket.detectPacketType(data) == MySQLGenericPacketType.eof) {
                numOfEofPacketsParsed++;

                bool done = false;

                assert(preparedPacket != null, 'preparedPacket is null in prepare response eof handling');

                if (preparedPacket!.numOfCols > 0 && preparedPacket!.numOfParams > 0) {
                  // there should be two EOF packets in this case
                  if (numOfEofPacketsParsed == 2) {
                    done = true;
                  }
                } else {
                  // there should be only one EOF packet otherwise
                  done = true;
                }

                if (done) {
                  state = 2;

                  completer.complete(
                    PreparedStmt._(preparedPacket: preparedPacket!, connection: this, iterable: iterable),
                  );

                  _stateSubject.add(_MySQLConnectionState.connectionEstablished);
                  return;
                }
              }
          }

          if (packet != null) {
            final MySQLPacketPayload payload = packet.payload;

            if (payload is MySQLPacketStmtPrepareOK) {
              preparedPacket = payload;
            } else if (payload is MySQLPacketError) {
              completer.completeError(
                MySQLServerException(payload.errorMessage, StackTrace.current, payload.errorCode, query),
              );
              _stateSubject.add(_MySQLConnectionState.connectionEstablished);
              return;
            } else {
              completer.completeError(
                MySQLClientException.unexpectedPayload(request: 'COMM_STMT_PREPARE', stackTrace: StackTrace.current),
              );
              await _closeSocketAndCleanUp();
              return;
            }
          }
        } catch (e) {
          completer.completeError(e, StackTrace.current);
          await _closeSocketAndCleanUp();
        }
      };

      _socket.add(packet.encode());

      final PreparedStmt preparedStmt = await completer.future;
      _activePreparedStatements.add(preparedStmt);
      return preparedStmt;
    }

    if (_inTransaction) {
      // If we are in a transaction, we are already holding the operation lock.
      return localPrepare();
    }

    return _operationLock.withResource(localPrepare);
  }

  Future<void> close() => _close(interruptRunning: false);

  /// Registers callback to be executed when this connection is closed
  void onClose(void Function() callback) => _onCloseCallbacks.add(callback);

  Future<void> _close({required bool interruptRunning, bool socketIsBroken = false}) async {
    if (!_isClosing) {
      _isClosing = true;

      if (interruptRunning) {
        if (socketIsBroken) {
          await _closeSocketAndCleanUp();
        } else {
          await _closeGracefully();
        }
      } else {
        // Wait for the previous operation to complete by using the lock
        await _operationLock.withResource(() async {
          // Use lock to await earlier operations
          if (socketIsBroken) {
            await _closeSocketAndCleanUp();
          } else {
            await _closeGracefully();
          }
        });
      }

      _closeSession();
    }
  }

  /// Close this connection gracefully (by sending the close signal to the socket)
  Future<void> _closeGracefully() async {
    if (!_state.isConnectionEstablished) {
      throw MySQLClientException.unexpectedState(
        message:
            'Cannot close the connection. Connection state is not in connectionEstablished state (Current state: ${_state.name})',
        stackTrace: StackTrace.current,
      );
    }

    final MySQLPacket packet = MySQLPacket(sequenceID: 0, payload: MySQLPacketCommQuit(), payloadLength: 0);
    try {
      _stateSubject.add(_MySQLConnectionState.quitCommandSend);
      _socket.add(packet.encode());
    } on Exception catch (_) {
      // If socket is broken, we can not send quit command
      // So just close the socket and clean-up (called in finally block)
    } finally {
      await _closeSocketAndCleanUp();
    }
  }

  void _closeSession() {
    if (!_sessionClosed) {
      _sessionClosedCompleter.complete();
    }
  }

  Future<void> _closeSocketAndCleanUp() async {
    if (!_stateSubject.isClosed) {
      _stateSubject.add(_MySQLConnectionState.closed);
      unawaited(_stateSubject.close());
    }

    await _socket.flush();
    await Future<void>.delayed(_socketFlushDelay);
    await _socketSubscription?.cancel();
    _socket.destroy();
    _onCloseCallbacks
      ..forEach((void Function() element) => element())
      ..clear();
    _incompleteBufferData.clear();
    _inTransaction = false;
    _responseCallback = null;
    _socketSubscription = null;

    for (final PreparedStmt stmt in _activePreparedStatements.toList()) {
      try {
        await _deallocatePreparedStmt(stmt, useOperationLock: false);
      } catch (_) {
        // Ignore errors during cleanup - connection is closing anyway
      }
    }
    _activePreparedStatements.clear();
  }

  Future<void> _processSocketData(Uint8List data) async {
    if (_state.isClosed) {
      // Don't process any data if state is closed
      return;
    }

    if (_state.isWaitingForInitialHandshake) {
      await _processInitialHandshake(data);
      return;
    }

    if (_state.isInitialHandshakeResponseSend) {
      // Check for auth switch request
      try {
        final MySQLPacket authSwitchPacket = MySQLPacket.decodeAuthSwitchRequestPacket(data);

        final MySQLPacketAuthSwitchRequest payload = authSwitchPacket.payload as MySQLPacketAuthSwitchRequest;

        _activeAuthPluginName = payload.authPluginName;

        switch (payload.authPluginName) {
          case 'mysql_native_password':
            final MySQLPacketAuthSwitchResponse responsePayload =
                MySQLPacketAuthSwitchResponse.createWithNativePassword(
              password: _endpoint.password,
              challenge: payload.authPluginData.sublist(0, _authChallengeLength),
            );
            final MySQLPacket responsePacket = MySQLPacket(
              sequenceID: authSwitchPacket.sequenceID + 1,
              payload: responsePayload,
              payloadLength: 0,
            );

            _socket.add(responsePacket.encode());
            return;
          default:
            throw MySQLClientException(
              'Unsupported auth plugin name: ${payload.authPluginName}',
              StackTrace.current,
              code: MySQLClientExceptionCode.unsupported,
            );
        }
      } on MySQLClientException catch (_) {
        // No auth switch request packet, continue packet processing
      }

      final MySQLPacket packet = MySQLPacket.decodeGenericPacket(data);
      if (packet.payload is MySQLPacketExtraAuthData) {
        assert(_activeAuthPluginName != null, '_activeAuthPluginName is null in MySQLPacketExtraAuthData handling');

        if (_activeAuthPluginName != 'caching_sha2_password') {
          throw MySQLClientException(
            'Unexpected auth plugin name $_activeAuthPluginName, while receiving MySQLPacketExtraAuthData packet',
            StackTrace.current,
            code: MySQLClientExceptionCode.unexpectedState,
          );
        }

        if (_secure == false) {
          throw MySQLClientException(
            'Auth plugin caching_sha2_password is supported only with secure connections. Pass secure: true or use another auth method',
            StackTrace.current,
            code: MySQLClientExceptionCode.unexpectedState,
          );
        }

        final MySQLPacketExtraAuthData payload = packet.payload as MySQLPacketExtraAuthData;
        final int status = payload.pluginData.codeUnitAt(0);

        if (status == 3) {
          // server has password cache. just ignore
          return;
        } else if (status == 4) {
          // send password to the server
          final MySQLPacket authExtraDataResponse = MySQLPacket(
            sequenceID: packet.sequenceID + 1,
            payload: MySQLPacketExtraAuthDataResponse(data: Uint8List.fromList(utf8.encode(_endpoint.password))),
            payloadLength: 0,
          );

          _socket.add(authExtraDataResponse.encode());
          return;
        } else {
          throw MySQLClientException(
            'Unsupported extra auth data: $data',
            StackTrace.current,
            code: MySQLClientExceptionCode.unsupported,
          );
        }
      }

      if (packet.isErrorPacket()) {
        final MySQLPacketError errorPayload = packet.payload as MySQLPacketError;
        throw MySQLServerException(errorPayload.errorMessage, StackTrace.current, errorPayload.errorCode);
      }

      if (packet.isOkPacket()) {
        _stateSubject.add(_MySQLConnectionState.connectionEstablished);
      }

      return;
    }

    if (_state.isWaitingCommandResponse) {
      _processCommandResponse(data);
      return;
    }

    throw MySQLClientException(
      "Skipping socket data, because of connection's bad state\nState: ${_state.name}\nData: $data",
      StackTrace.current,
      code: MySQLClientExceptionCode.unexpectedState,
    );
  }

  Iterable<Uint8List> _splitPackets(Uint8List bytes) sync* {
    Uint8List packet = bytes;
    if (_incompleteBufferData.isNotEmpty) {
      // Use efficient setRange instead of list concatenation to avoid intermediate lists
      final Uint8List tmp = Uint8List(_incompleteBufferData.length + packet.length)
        ..setRange(0, _incompleteBufferData.length, _incompleteBufferData);
      tmp.setRange(_incompleteBufferData.length, tmp.length, packet);
      packet = tmp;
      _incompleteBufferData.clear();
    }

    while (true) {
      // if packet size is less then 4 bytes, we can not even detect payload length and total packet size
      // so just append data to incomplete buffer
      if (packet.length < 4) {
        if (_incompleteBufferData.length + packet.length > _maxIncompleteBufferSize) {
          throw MySQLClientException(
            'Incomplete buffer exceeded maximum size of $_maxIncompleteBufferSize bytes. Possible malformed packet or DoS attack.',
            StackTrace.current,
            code: MySQLClientExceptionCode.unexpectedPacket,
          );
        }
        _incompleteBufferData.addAll(packet);
        break;
      }

      final int packetLength = MySQLPacket.getPacketLength(packet);

      if (packet.lengthInBytes < packetLength) {
        // incomplete packet
        if (_incompleteBufferData.length + packet.length > _maxIncompleteBufferSize) {
          throw MySQLClientException(
            'Incomplete buffer exceeded maximum size of $_maxIncompleteBufferSize bytes. Possible malformed packet or DoS attack.',
            StackTrace.current,
            code: MySQLClientExceptionCode.unexpectedPacket,
          );
        }
        _incompleteBufferData.addAll(packet);
        break;
      }

      final Uint8List chunk = Uint8List.sublistView(packet, 0, packetLength);
      yield chunk;

      packet = Uint8List.sublistView(packet, packetLength);

      if (packet.isEmpty) {
        break;
      }
    }
  }

  Future<void> _processInitialHandshake(Uint8List data) async {
    // First packet can be error packet
    if (MySQLPacket.detectPacketType(data) == MySQLGenericPacketType.error) {
      final MySQLPacket packet = MySQLPacket.decodeGenericPacket(data);
      final MySQLPacketError payload = packet.payload as MySQLPacketError;
      throw MySQLServerException(payload.errorMessage, StackTrace.current, payload.errorCode);
    }

    final MySQLPacket packet = MySQLPacket.decodeInitialHandshake(data);
    final MySQLPacketPayload payload = packet.payload;

    if (payload is! MySQLPacketInitialHandshake) {
      throw MySQLClientException(
        'Expected MySQLPacketInitialHandshake packet',
        StackTrace.current,
        code: MySQLClientExceptionCode.unexpectedPacket,
      );
    }

    _serverCapabilities = payload.capabilityFlags;

    if (_secure && (_serverCapabilities & mysqlCapFlagClientSsl == 0)) {
      throw MySQLClientException(
        'Server does not support SSL connection. Pass secure: false to createConnection or enable SSL support',
        StackTrace.current,
        code: MySQLClientExceptionCode.unsupported,
      );
    }

    if (_secure) {
      // it secure = true, initiate ssl connection
      Future<void> initiateSSL() async {
        final MySQLPacketSSLRequest responsePayload = MySQLPacketSSLRequest.createDefault(
          initialHandshakePayload: payload,
          connectWithDB: _endpoint.database != null,
        );

        final MySQLPacket responsePacket = MySQLPacket(sequenceID: 1, payload: responsePayload, payloadLength: 0);

        _socket.add(responsePacket.encode());

        _socketSubscription?.pause();

        try {
          final SecureSocket secureSocket = await SecureSocket.secure(
            _socket,
            onBadCertificate: (X509Certificate certificate) => true,
          );

          // Switch socket
          _socket = secureSocket;
          _socketSubscription = _socket.listen(
            (Uint8List data) async {
              for (final Uint8List chunk in _splitPackets(data)) {
                try {
                  await _processSocketData(chunk);
                } catch (error, stackTrace) {
                  _stateSubject.addError(error, stackTrace);
                }
              }
            },
            onError: (Object error, StackTrace stackTrace) async {
              _stateSubject.addError(error);
              await _close(interruptRunning: true, socketIsBroken: true);
            },
          );
          _onDone = _socket.done;
          unawaited(
            _onDone!.catchError((Object error, StackTrace stackTrace) async {
              if (error is SocketException && !_executeCompleter.isCompleted) {
                _executeCompleter.completeError(
                  MySQLClientException(
                    'Broken connection, try using a new connection. Error: $error',
                    stackTrace,
                    code: MySQLClientExceptionCode.brokenConnection,
                  ),
                );
                await _close(interruptRunning: true, socketIsBroken: true);
              }
            }),
          );
        } catch (e) {
          // If SSL handshake fails, clean up and rethrow
          await _socketSubscription?.cancel();
          rethrow;
        }
      }

      await initiateSSL();
    }

    final String? authPluginName = payload.authPluginName;
    _activeAuthPluginName = authPluginName;

    switch (authPluginName) {
      case 'mysql_native_password':
        final MySQLPacketHandshakeResponse41 responsePayload = MySQLPacketHandshakeResponse41.createWithNativePassword(
          username: _endpoint.username,
          password: _endpoint.password,
          initialHandshakePayload: payload,
        );

        responsePayload.database = _endpoint.database;

        final MySQLPacket responsePacket = MySQLPacket(
          payload: responsePayload,
          sequenceID: _secure ? 2 : 1,
          payloadLength: 0,
        );

        _stateSubject.add(_MySQLConnectionState.initialHandshakeResponseSend);
        _socket.add(responsePacket.encode());

      case 'caching_sha2_password':
        final MySQLPacketHandshakeResponse41 responsePayload =
            MySQLPacketHandshakeResponse41.createWithCachingSha2Password(
          username: _endpoint.username,
          password: _endpoint.password,
          initialHandshakePayload: payload,
        );

        responsePayload.database = _endpoint.database;

        final MySQLPacket responsePacket = MySQLPacket(
          payload: responsePayload,
          sequenceID: _secure ? 2 : 1,
          payloadLength: 0,
        );

        _stateSubject.add(_MySQLConnectionState.initialHandshakeResponseSend);
        _socket.add(responsePacket.encode());

      default:
        throw MySQLClientException(
          'Unsupported auth plugin name: $authPluginName',
          StackTrace.current,
          code: MySQLClientExceptionCode.unsupported,
        );
    }
  }

  void _processCommandResponse(Uint8List data) {
    // Capture callback to avoid race condition with null assignment
    final Future<void> Function(Uint8List data)? callback = _responseCallback;
    if (callback == null) {
      // Connection is closing or callback was cleared, ignore the data
      return;
    }
    callback(data);
  }

  Future<IResultSet> _executePreparedStmt(PreparedStmt stmt, List<dynamic> params, bool iterable) async {
    // Use operation lock to ensure operations execute sequentially
    Future<IResultSet> localExecutePreparedStmt() async {
      if (!isOpen) {
        throw MySQLClientException.connectionClosed(
          message: 'Cannot execute operation, connection is in open state: $_state, closing: $_isClosing',
          stackTrace: StackTrace.current,
        );
      }

      // Wait for ready state
      if (!_state.isConnectionEstablished) {
        await _waitForState(_MySQLConnectionState.connectionEstablished).timeout(
          _timeoutMs,
          onTimeout: () => throw MySQLClientException.timeout(duration: _timeoutMs, stackTrace: StackTrace.current),
        );
      }

      _stateSubject.add(_MySQLConnectionState.waitingCommandResponse);

      final MySQLPacketCommStmtExecute payload = MySQLPacketCommStmtExecute(
        stmtID: stmt._preparedPacket.stmtID,
        params: params,
      );

      final MySQLPacket packet = MySQLPacket(sequenceID: 0, payload: payload, payloadLength: 0);

      final Completer<IResultSet> completer = Completer<IResultSet>();

      /**
       * 0 - initial
       * 1 - columnCount decoded
       * 2 - columnDefs parsed
       * 3 - eofParsed
       * 4 - rowsParsed
       */
      int state = 0;
      int colsCount = 0;
      final List<MySQLColumnDefinitionPacket> colDefs = <MySQLColumnDefinitionPacket>[];
      final List<MySQLBinaryResultSetRowPacket> resultSetRows = <MySQLBinaryResultSetRowPacket>[];

      // support for iterable result set
      IterablePreparedStmtResultSet? iterableResultSet;
      StreamSink<ResultSetRow>? sink;

      _responseCallback = (Uint8List data) async {
        try {
          MySQLPacket? packet;

          switch (state) {
            case 0:
              // if packet is OK packet, there is no data
              if (MySQLPacket.detectPacketType(data) == MySQLGenericPacketType.ok) {
                final MySQLPacket okPacket = MySQLPacket.decodeGenericPacket(data);
                _stateSubject.add(_MySQLConnectionState.connectionEstablished);

                completer.complete(EmptyResultSet(okPacket: okPacket.payload as MySQLPacketOK));

                return;
              }

              packet = MySQLPacket.decodeColumnCountPacket(data);
            case 1:
              packet = MySQLPacket.decodeColumnDefPacket(data);
            case 2:
              packet = MySQLPacket.decodeGenericPacket(data);
              if (packet.isEOFPacket()) {
                state = 3;
              } else if (packet.isErrorPacket()) {
                final MySQLPacketError errorPayload = packet.payload as MySQLPacketError;
                completer.completeError(
                  MySQLServerException(errorPayload.errorMessage, StackTrace.current, errorPayload.errorCode),
                );
                _stateSubject.add(_MySQLConnectionState.connectionEstablished);
                return;
              } else {
                completer.completeError(
                  MySQLClientException.unexpectedPacket(
                    message: 'Unexpected packet type',
                    stackTrace: StackTrace.current,
                  ),
                );
                await _closeSocketAndCleanUp();
                return;
              }
            case 3:
              if (iterable) {
                if (iterableResultSet == null) {
                  iterableResultSet = IterablePreparedStmtResultSet._(columns: colDefs);

                  sink = iterableResultSet!._sink;
                  completer.complete(iterableResultSet);
                }

                // check eof
                if (MySQLPacket.detectPacketType(data) == MySQLGenericPacketType.eof) {
                  state = 4;

                  _stateSubject.add(_MySQLConnectionState.connectionEstablished);
                  await sink!.close();
                  return;
                }

                packet = MySQLPacket.decodeBinaryResultSetRowPacket(data, colDefs);
                final List<String?> values = (packet.payload as MySQLBinaryResultSetRowPacket).values;
                sink!.add(ResultSetRow(colDefs: colDefs, values: values));
                packet = null;
                break;
              } else {
                // check eof
                if (MySQLPacket.detectPacketType(data) == MySQLGenericPacketType.eof) {
                  state = 4;

                  final MySQLPacketBinaryResultSet resultSetPacket = MySQLPacketBinaryResultSet(
                    columnCount: BigInt.from(colsCount),
                    columns: colDefs,
                    rows: resultSetRows,
                  );

                  _stateSubject.add(_MySQLConnectionState.connectionEstablished);

                  completer.complete(PreparedStmtResultSet._(resultSetPacket: resultSetPacket));

                  return;
                }

                packet = MySQLPacket.decodeBinaryResultSetRowPacket(data, colDefs);

                break;
              }
          }

          if (packet != null) {
            final MySQLPacketPayload payload = packet.payload;

            if (payload is MySQLPacketError) {
              completer
                  .completeError(MySQLServerException(payload.errorMessage, StackTrace.current, payload.errorCode));
              _stateSubject.add(_MySQLConnectionState.connectionEstablished);
              return;
            } else if (payload is MySQLPacketOK || payload is MySQLPacketEOF) {
              // do nothing
            } else if (payload is MySQLPacketColumnCount) {
              state = 1;
              colsCount = payload.columnCount.toInt();
              return;
            } else if (payload is MySQLColumnDefinitionPacket) {
              colDefs.add(payload);
              if (colDefs.length == colsCount) {
                state = 2;
              }
            } else if (payload is MySQLBinaryResultSetRowPacket) {
              resultSetRows.add(payload);
            } else {
              completer.completeError(
                MySQLClientException.unexpectedPayload(request: 'COMM_QUERY', stackTrace: StackTrace.current),
              );
              await _closeSocketAndCleanUp();
              return;
            }
          }
        } catch (e, s) {
          completer.completeError(e, s);
          await _closeSocketAndCleanUp();
        }
      };

      _socket.add(packet.encode());

      return completer.future;
    }

    if (_inTransaction) {
      // If we are in a transaction, we are already holding the operation lock.
      return localExecutePreparedStmt();
    }
    return _operationLock.withResource(localExecutePreparedStmt);
  }

  Future<void> _deallocatePreparedStmt(PreparedStmt stmt, {bool useOperationLock = true}) async {
    // Use operation lock to ensure operations execute sequentially
    Future<void> localDeallocate() async {
      if (!isOpen) {
        throw MySQLClientException.connectionClosed(
          message: 'Cannot execute operation, connection is in open state: $_state, closing: $_isClosing',
          stackTrace: StackTrace.current,
        );
      }

      // wait for ready state
      if (!_state.isConnectionEstablished) {
        await _waitForState(_MySQLConnectionState.connectionEstablished).timeout(
          _timeoutMs,
          onTimeout: () => throw MySQLClientException.timeout(duration: _timeoutMs, stackTrace: StackTrace.current),
        );
      }

      final MySQLPacketCommStmtClose payload = MySQLPacketCommStmtClose(stmtID: stmt._preparedPacket.stmtID);
      final MySQLPacket packet = MySQLPacket(sequenceID: 0, payload: payload, payloadLength: 0);

      _socket.add(packet.encode());
      _activePreparedStatements.remove(stmt);
    }

    if (_inTransaction) {
      // If we are in a transaction, we are already holding the operation lock.
      return localDeallocate();
    }

    if (!useOperationLock) {
      return localDeallocate();
    }
    return _operationLock.withResource(localDeallocate);
  }

  Future<void> _waitForState(_MySQLConnectionState state) async {
    if (_state == state) {
      return;
    }

    await _stateSubject.stream.firstWhere((_MySQLConnectionState current) => current == state);
  }
}

enum _MySQLConnectionState {
  fresh,
  waitInitialHandshake,
  initialHandshakeResponseSend,
  connectionEstablished,
  waitingCommandResponse,
  quitCommandSend,
  closed;

  bool get isFresh => this == _MySQLConnectionState.fresh;

  bool get isWaitingForInitialHandshake => this == _MySQLConnectionState.waitInitialHandshake;

  bool get isInitialHandshakeResponseSend => this == _MySQLConnectionState.initialHandshakeResponseSend;

  bool get isConnectionEstablished => this == _MySQLConnectionState.connectionEstablished;

  bool get isWaitingCommandResponse => this == _MySQLConnectionState.waitingCommandResponse;

  bool get isQuitCommandSent => this == _MySQLConnectionState.quitCommandSend;

  bool get isClosed => this == _MySQLConnectionState.closed;
}

/// Base class to represent result of calling [MySQLConnection.execute] and [PreparedStmt.execute]
abstract class IResultSet with IterableMixin<IResultSet> implements Iterator<IResultSet>, Iterable<IResultSet> {
  /// Number of colums in this result if any
  int get numOfColumns;

  /// Number of rows in this result if any (unavailable for iterable results)
  int get numOfRows;

  /// Number of affected rows
  BigInt get affectedRows;

  /// Last insert ID
  BigInt get lastInsertID;

  /// Next result set, if any.
  /// Prepared statements and iterable result sets does not supprot this
  IResultSet? next;

  IResultSet? _current;

  @override
  Iterator<IResultSet> get iterator => this;

  @override
  IResultSet get current {
    if (_current != null) {
      return _current!;
    } else {
      throw RangeError('Trying to access past the end value');
    }
  }

  @override
  bool moveNext() {
    if (_current == null) {
      _current = this;
      return true;
    } else {
      if (_current!.next != null) {
        _current = _current!.next;
        return true;
      } else {
        return false;
      }
    }
  }

  /// Provides access to data rows (unavailable for iterable results)
  Iterable<ResultSetRow> get rows;

  /// Use [cols] to get info about returned columns
  Iterable<ResultSetColumn> get cols;

  /// Provides Stream like access to data rows. Use [rowsStream] to get rows from iterable results
  Stream<ResultSetRow> get rowsStream => Stream<ResultSetRow>.fromIterable(rows);
}

/// Represents result of [MySQLConnection.execute] method
class ResultSet extends IResultSet {
  ResultSet({required MySQLPacketResultSet resultSetPacket}) : _resultSetPacket = resultSetPacket;
  final MySQLPacketResultSet _resultSetPacket;

  @override
  int get numOfColumns => _resultSetPacket.columns.length;

  @override
  int get numOfRows => _resultSetPacket.rows.length;

  @override
  BigInt get affectedRows => BigInt.zero;

  @override
  BigInt get lastInsertID => BigInt.zero;

  @override
  Iterable<ResultSetRow> get rows sync* {
    for (final MySQLResultSetRowPacket row in _resultSetPacket.rows) {
      yield ResultSetRow(colDefs: _resultSetPacket.columns, values: row.values);
    }
  }

  @override
  Iterable<ResultSetColumn> get cols {
    return _resultSetPacket.columns.map(
      (MySQLColumnDefinitionPacket e) => ResultSetColumn(name: e.name, type: e.type, length: e.columnLength),
    );
  }
}

/// Represents result of [MySQLConnection.execute] method when passing iterable = true
class IterableResultSet with IterableMixin<IResultSet> implements IResultSet {
  IterableResultSet({required List<MySQLColumnDefinitionPacket> columns}) : _columns = columns {
    _controller = StreamController<ResultSetRow>();
  }

  final List<MySQLColumnDefinitionPacket> _columns;
  late StreamController<ResultSetRow> _controller;

  @override
  IResultSet? get next => throw UnimplementedError();

  @override
  set next(IResultSet? val) => throw UnimplementedError();

  @override
  Iterator<IResultSet> get iterator => throw UnimplementedError();

  @override
  IResultSet? _current;

  @override
  IResultSet get current => throw UnimplementedError();

  @override
  bool moveNext() => throw UnimplementedError();

  StreamSink<ResultSetRow> get sink => _controller.sink;

  @override
  Stream<ResultSetRow> get rowsStream => _controller.stream;

  @override
  int get numOfColumns => _columns.length;

  @override
  int get numOfRows {
    throw Error.throwWithStackTrace(
      UnimplementedError('numOfRows is not implemented for IterableResultSet'),
      StackTrace.current,
    );
  }

  @override
  BigInt get affectedRows => BigInt.zero;

  @override
  BigInt get lastInsertID => BigInt.zero;

  @override
  Iterable<ResultSetColumn> get cols {
    return _columns.map(
      (MySQLColumnDefinitionPacket e) => ResultSetColumn(name: e.name, type: e.type, length: e.columnLength),
    );
  }

  @override
  Iterable<ResultSetRow> get rows {
    throw Error.throwWithStackTrace(
      UnsupportedError('Use rowsStream to get rows from IterableResultSet'),
      StackTrace.current,
    );
  }
}

/// Represents result of [PreparedStmt.execute] method
class PreparedStmtResultSet extends IResultSet {
  PreparedStmtResultSet._({required MySQLPacketBinaryResultSet resultSetPacket}) : _resultSetPacket = resultSetPacket;
  final MySQLPacketBinaryResultSet _resultSetPacket;

  @override
  int get numOfColumns => _resultSetPacket.columns.length;

  @override
  int get numOfRows => _resultSetPacket.rows.length;

  @override
  BigInt get affectedRows => BigInt.zero;

  @override
  BigInt get lastInsertID => BigInt.zero;

  @override
  Iterable<ResultSetRow> get rows sync* {
    for (final MySQLBinaryResultSetRowPacket row in _resultSetPacket.rows) {
      yield ResultSetRow(colDefs: _resultSetPacket.columns, values: row.values);
    }
  }

  @override
  Iterable<ResultSetColumn> get cols {
    return _resultSetPacket.columns.map(
      (MySQLColumnDefinitionPacket e) => ResultSetColumn(name: e.name, type: e.type, length: e.columnLength),
    );
  }
}

/// Represents result of [PreparedStmt.execute] method when using iterable = true
class IterablePreparedStmtResultSet extends IResultSet {
  IterablePreparedStmtResultSet._({required List<MySQLColumnDefinitionPacket> columns}) : _columns = columns {
    _controller = StreamController<ResultSetRow>();
  }

  final List<MySQLColumnDefinitionPacket> _columns;
  late StreamController<ResultSetRow> _controller;

  StreamSink<ResultSetRow> get _sink => _controller.sink;

  @override
  int get numOfColumns => _columns.length;

  @override
  int get numOfRows {
    throw Error.throwWithStackTrace(
      UnsupportedError('numOfRows is not implemented for IterableResultSet'),
      StackTrace.current,
    );
  }

  @override
  BigInt get affectedRows => BigInt.zero;

  @override
  BigInt get lastInsertID => BigInt.zero;

  @override
  Iterable<ResultSetRow> get rows {
    throw Error.throwWithStackTrace(
      UnsupportedError('Use rowsStream to get rows from IterablePreparedStmtResultSet'),
      StackTrace.current,
    );
  }

  @override
  Stream<ResultSetRow> get rowsStream => _controller.stream;

  @override
  Iterable<ResultSetColumn> get cols {
    return _columns.map(
      (MySQLColumnDefinitionPacket e) => ResultSetColumn(name: e.name, type: e.type, length: e.columnLength),
    );
  }
}

/// Represents empty result set
class EmptyResultSet extends IResultSet {
  EmptyResultSet({required MySQLPacketOK okPacket}) : _okPacket = okPacket;
  final MySQLPacketOK _okPacket;

  @override
  int get numOfColumns => 0;

  @override
  int get numOfRows => 0;

  @override
  BigInt get affectedRows => _okPacket.affectedRows;

  @override
  BigInt get lastInsertID => _okPacket.lastInsertID;

  @override
  Iterable<ResultSetRow> get rows => List<ResultSetRow>.empty();

  @override
  Iterable<ResultSetColumn> get cols => List<ResultSetColumn>.empty();
}

/// Represents result set row data
class ResultSetRow {
  ResultSetRow({required this.colDefs, required this.values});

  final List<MySQLColumnDefinitionPacket> colDefs;
  final List<String?> values;

  /// Get number of columns for this row
  int get numOfColumns => colDefs.length;

  /// Get column data by column index (starting form 0)
  String? colAt(int colIndex) {
    if (colIndex >= values.length) {
      throw Error.throwWithStackTrace(RangeError.index(colIndex, 'Column index is out of range'), StackTrace.current);
    }

    final String? value = values[colIndex];

    return value;
  }

  /// Same as [colAt] but performs conversion of string data, into provided type [T], if possible
  ///
  /// Conversion is "typesafe", meaning that actual MySQL column type will be checked,
  /// to decide is it possible to make such a conversion
  ///
  /// Throws [MySQLClientException] if conversion is not possible
  T? typedColAt<T>(int colIndex) {
    final String? value = colAt(colIndex);
    final MySQLColumnDefinitionPacket colDef = colDefs[colIndex];

    return colDef.type.convertStringValueToProvidedType<T>(value, colDef.columnLength);
  }

  /// Get column data by column name
  String? colByName(String columnName) {
    final int colIndex = colDefs.indexWhere(
      (MySQLColumnDefinitionPacket element) => element.name.toLowerCase() == columnName.toLowerCase(),
    );

    if (colIndex == -1) {
      throw Error.throwWithStackTrace(
        RangeError.index(colIndex, 'Column with $columnName name was not found'),
        StackTrace.current,
      );
    }

    if (colIndex >= values.length) {
      throw Error.throwWithStackTrace(RangeError.index(colIndex, 'Column index is out of range'), StackTrace.current);
    }

    final String? value = values[colIndex];

    return value;
  }

  /// Same as [colByName] but performs conversion of string data, into provided type [T], if possible
  ///
  /// Conversion is "typesafe", meaning that actual MySQL column type will be checked,
  /// to decide is it possible to make such a conversion
  ///
  /// Throws [MySQLClientException] if conversion is not possible
  T? typedColByName<T>(String columnName) {
    final String? value = colByName(columnName);

    final int colIndex = colDefs.indexWhere(
      (MySQLColumnDefinitionPacket element) => element.name.toLowerCase() == columnName.toLowerCase(),
    );

    final MySQLColumnDefinitionPacket colDef = colDefs[colIndex];

    return colDef.type.convertStringValueToProvidedType<T>(value, colDef.columnLength);
  }

  /// Get data for all columns
  Map<String, String?> assoc() {
    final Map<String, String?> result = <String, String?>{};

    int colIndex = 0;

    for (final MySQLColumnDefinitionPacket colDef in colDefs) {
      result[colDef.name] = values[colIndex];
      colIndex++;
    }

    return result;
  }

  /// Same as [assoc] but detects best dart type for columns, and converts string data into appropriate types
  Map<String, dynamic> typedAssoc() {
    final Map<String, dynamic> result = <String, dynamic>{};

    int colIndex = 0;

    for (final MySQLColumnDefinitionPacket colDef in colDefs) {
      final String? value = values[colIndex];

      if (value == null) {
        result[colDef.name] = null;
        colIndex++;
        continue;
      }

      final Type dartType = colDef.type.getBestMatchDartType(colDef.columnLength);

      // It is easier to read using switch expression compared to wildcard
      // ignore_for_file: type_literal_in_constant_pattern
      final Object decodedValue = switch (dartType) {
        int => int.parse(value),
        double => double.parse(value),
        num => num.parse(value),
        bool => int.parse(value) > 0,
        DateTime => DateTime.parse(value),
        String => value,
        _ => value,
      };

      result[colDef.name] = decodedValue;

      colIndex++;
    }

    return result;
  }
}

/// Represents column definition
class ResultSetColumn {
  ResultSetColumn({required this.name, required this.type, required this.length});

  String name;
  MySQLColumnType type;
  int length;
}

/// Prepared statement class
class PreparedStmt {
  PreparedStmt._({
    required MySQLPacketStmtPrepareOK preparedPacket,
    required MySQLConnection connection,
    required bool iterable,
  })  : _preparedPacket = preparedPacket,
        _connection = connection,
        _iterable = iterable;
  final MySQLPacketStmtPrepareOK _preparedPacket;
  final MySQLConnection _connection;
  final bool _iterable;
  final Completer<void> disposed = Completer<void>();

  int get numOfParams => _preparedPacket.numOfParams;

  /// Executes this prepared statement with given [params]
  Future<IResultSet> execute(List<dynamic> params) async {
    if (numOfParams != params.length) {
      throw MySQLClientException(
        'Can not execute prepared stmt: number of passed params != number of prepared params',
        StackTrace.current,
        code: MySQLClientExceptionCode.invalidArgument,
      );
    }

    return _connection._executePreparedStmt(this, params, _iterable);
  }

  /// Deallocates this prepared statement
  ///
  /// Use this method to prevent memory leaks for long running connections
  /// All prepared statements are automatically deallocated by database when connection is closed
  Future<void> deallocate() async {
    await _connection._deallocatePreparedStmt(this);
    disposed.complete();
  }
}
