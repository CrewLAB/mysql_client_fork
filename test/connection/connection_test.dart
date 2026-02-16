import 'dart:async';

import 'package:mysql_client_fork/exception.dart';
import 'package:mysql_client_fork/mysql_client.dart';
import 'package:test/test.dart';

/// Tests for fixes made to MySQLConnection
///
/// This test suite covers:
/// - Input validation (empty query checks)
/// - Prepared statement cleanup on connection close
/// - Transaction rollback behavior
/// - Double completer completion protection
/// - Response callback race condition handling
/// - State management and error recovery
/// - Concurrent operations
/// - Connection lifecycle
/// - Edge cases
///
/// This test assumes a local MySQL database is running.
/// Configure your test database endpoint below to match your setup.
///
/// Note: Memory limit testing for incomplete buffer would require
/// mocking the socket or creating malformed packets, which is complex
/// and may be better suited for integration tests.
void main() {
  // Configure your test database endpoint here
  const Endpoint testEndpoint = Endpoint(
    database: 'dev',
    host: 'localhost',
    username: 'root',
    password: 'AKMFtdCq0ceHCA4g',
    secure: false,
  );

  group('Input Validation Tests', () {
    late MySQLConnection connection;

    setUpAll(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();
    });

    tearDownAll(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('execute() throws error for empty query', () async {
      await expectLater(
        () => connection.execute(''),
        throwsA(
          isA<MySQLClientException>().having(
            (MySQLClientException e) => e.code,
            'code',
            MySQLClientExceptionCode.invalidArgument,
          ),
        ),
      );
    });

    test('prepare() throws error for empty query', () async {
      await expectLater(
        () => connection.prepare(''),
        throwsA(
          isA<MySQLClientException>().having(
            (MySQLClientException e) => e.code,
            'code',
            MySQLClientExceptionCode.invalidArgument,
          ),
        ),
      );
    });

    test('execute() accepts valid queries', () async {
      final IResultSet result = await connection.execute('SELECT 1 as test');
      expect(result.numOfRows, 1);
    });

    test('prepare() accepts valid queries', () async {
      final PreparedStmt stmt = await connection.prepare('SELECT ? as test');
      expect(stmt.numOfParams, 1);
      await stmt.deallocate();
    });
  });

  group('Prepared Statement Cleanup Tests', () {
    late MySQLConnection connection;

    setUp(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();
    });

    tearDown(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('prepared statements are tracked and cleaned up on connection close', () async {
      // Create multiple prepared statements
      final PreparedStmt stmt1 = await connection.prepare('SELECT ? as val1');
      await connection.prepare('SELECT ? as val2');
      await connection.prepare('SELECT ? as val3');

      // Verify they work
      final IResultSet result1 = await stmt1.execute(<dynamic>[1]);
      expect(result1.rows.first.colByName('val1'), '1');

      // Close connection without explicitly deallocating
      await connection.close();

      // Verify connection is closed
      expect(connection.isOpen, false);

      // Note: We can't directly verify the prepared statements were deallocated
      // on the server side without querying the server, but the cleanup code
      // should have been called. The fact that connection closes without
      // errors is a good sign.
    });

    test('prepared statements can be manually deallocated', () async {
      final PreparedStmt stmt = await connection.prepare('SELECT ? as test');

      // Execute it
      final IResultSet result = await stmt.execute(<dynamic>[42]);
      expect(result.rows.first.colByName('test'), '42');

      // Deallocate it
      await stmt.deallocate();

      // Try to execute again - should fail
      await expectLater(
        () => stmt.execute(<dynamic>[43]),
        throwsA(isA<MySQLServerException>()),
      );
    });

    test('multiple prepared statements cleanup on close', () async {
      // Create several prepared statements
      final List<PreparedStmt> statements = <PreparedStmt>[];
      for (int i = 0; i < 5; i++) {
        final PreparedStmt stmt = await connection.prepare('SELECT ? as val$i');
        statements.add(stmt);
        // Execute each one
        await stmt.execute(<dynamic>[i]);
      }

      // Close connection - all should be cleaned up
      await connection.close();
      expect(connection.isOpen, false);
    });
  });

  group('Concurrent Prepared Statement Tests', () {
    late MySQLConnection connection;

    setUp(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();
    });

    tearDown(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('multiple concurrent prepare() calls are queued and complete correctly', () async {
      // With _operationLock, concurrent prepare() calls are properly queued
      // All prepared statements should be created successfully

      // Start multiple prepare() calls concurrently
      final List<Future<PreparedStmt>> prepareFutures = <Future<PreparedStmt>>[];
      for (int i = 0; i < 5; i++) {
        prepareFutures.add(connection.prepare('SELECT ? as val$i'));
      }

      // All should complete successfully (they execute sequentially via _operationLock)
      final List<PreparedStmt> statements = await Future.wait(prepareFutures);
      expect(statements.length, 5);

      // Verify all prepared statements work correctly
      for (int i = 0; i < 5; i++) {
        expect(statements[i].numOfParams, 1);
        final IResultSet result = await statements[i].execute(<dynamic>[i]);
        expect(result.rows.first.colByName('val$i'), i.toString());
      }

      // Clean up
      await Future.wait(statements.map((PreparedStmt stmt) => stmt.deallocate()));
    });

    test('multiple concurrent execute() calls on same prepared statement are queued', () async {
      // Create a single prepared statement
      final PreparedStmt stmt = await connection.prepare('SELECT ? as val');

      // Execute it multiple times concurrently
      final List<Future<IResultSet>> executeFutures = <Future<IResultSet>>[];
      for (int i = 0; i < 10; i++) {
        executeFutures.add(stmt.execute(<dynamic>[i]));
      }

      // All should complete successfully (they execute sequentially via _operationLock)
      final List<IResultSet> results = await Future.wait(executeFutures);
      expect(results.length, 10);

      // Verify all results are correct
      for (int i = 0; i < 10; i++) {
        expect(results[i].rows.first.colByName('val'), i.toString());
      }

      // Clean up
      await stmt.deallocate();
    });

    test('multiple concurrent execute() calls on different prepared statements are queued', () async {
      // Create multiple prepared statements
      final List<PreparedStmt> statements = <PreparedStmt>[];
      for (int i = 0; i < 5; i++) {
        final PreparedStmt stmt = await connection.prepare('SELECT ? as val$i');
        statements.add(stmt);
      }

      // Execute all of them concurrently
      final List<Future<IResultSet>> executeFutures = <Future<IResultSet>>[];
      for (int i = 0; i < 5; i++) {
        executeFutures.add(statements[i].execute(<dynamic>[i * 10]));
      }

      // All should complete successfully
      final List<IResultSet> results = await Future.wait(executeFutures);
      expect(results.length, 5);

      // Verify all results are correct
      for (int i = 0; i < 5; i++) {
        expect(results[i].rows.first.colByName('val$i'), (i * 10).toString());
      }

      // Clean up
      for (final PreparedStmt stmt in statements) {
        await stmt.deallocate();
      }
    });

    test('concurrent prepare() and execute() calls are properly queued', () async {
      // Mix of prepare() and execute() calls should be queued correctly

      // Start some prepare() calls
      final List<PreparedStmt> preparedStatements = <PreparedStmt>[];
      for (int i = 0; i < 3; i++) {
        final PreparedStmt preparedStmtFuture = await connection.prepare('SELECT ? as val$i');
        preparedStatements.add(preparedStmtFuture);
      }

      // Also start some regular execute() calls
      final List<Future<IResultSet>> executeFutures = <Future<IResultSet>>[];
      for (int i = 0; i < 3; i++) {
        executeFutures.add(connection.execute('SELECT $i as regular'));
      }

      // All should complete successfully
      final List<IResultSet> results = await Future.wait(executeFutures);

      expect(preparedStatements.length, 3);
      expect(results.length, 3);

      // Verify prepared statements work
      for (int i = 0; i < 3; i++) {
        final IResultSet stmtResult = await preparedStatements[i].execute(<dynamic>[i]);
        expect(stmtResult.rows.first.colByName('val$i'), i.toString());
      }

      // Verify regular execute results
      for (int i = 0; i < 3; i++) {
        expect(results[i].rows.first.colByName('regular'), i.toString());
      }

      // Clean up
      for (final PreparedStmt stmt in preparedStatements) {
        await stmt.deallocate();
      }
    });

    test('concurrent deallocate() calls are properly queued', () async {
      // Create multiple prepared statements
      final List<PreparedStmt> statements = <PreparedStmt>[];
      for (int i = 0; i < 5; i++) {
        final PreparedStmt stmt = await connection.prepare('SELECT ? as val$i');
        statements.add(stmt);
      }

      // Deallocate all of them concurrently
      final List<Future<void>> deallocateFutures = <Future<void>>[];
      for (final PreparedStmt stmt in statements) {
        deallocateFutures.add(stmt.deallocate());
      }

      // All should complete successfully
      await Future.wait(deallocateFutures);

      // Verify they're deallocated by trying to execute (should fail)
      for (final PreparedStmt stmt in statements) {
        await expectLater(
          () => stmt.execute(<dynamic>[1]),
          throwsA(isA<MySQLServerException>()),
        );
      }
    });
  });

  group('Transaction Rollback Tests', () {
    late MySQLConnection connection;

    setUpAll(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();

      // Create test table
      await connection.execute('''
        CREATE TABLE IF NOT EXISTS transaction_test (
          id INT AUTO_INCREMENT PRIMARY KEY,
          value VARCHAR(255) NOT NULL
        )
      ''');
    });

    tearDownAll(() async {
      if (connection.isOpen) {
        await connection.execute('DROP TABLE IF EXISTS transaction_test');
        await connection.close();
      }
    });

    tearDown(() async {
      // Clean up after each test
      await connection.execute('DELETE FROM transaction_test');
    });

    test('transaction rolls back on MySQLClientException', () async {
      await expectLater(
        () => connection.transactional<void>((MySQLConnection conn) async {
          print('Connection: ${conn.inTransaction}, isOpen: ${conn.isOpen}');
          await conn.execute("INSERT INTO transaction_test (value) VALUES ('test1')");
          throw MySQLClientException(
            'Test error',
            StackTrace.current,
            code: MySQLClientExceptionCode.invalidArgument,
          );
        }),
        throwsA(isA<MySQLClientException>()),
      );

      // Verify rollback occurred - no rows should be inserted
      final IResultSet result = await connection.execute('SELECT COUNT(*) as count FROM transaction_test');
      expect(result.rows.first.colByName('count'), '0');
    });

    test('transaction rolls back on MySQLServerException', () async {
      await expectLater(
        () => connection.transactional<void>((MySQLConnection conn) async {
          await conn.execute("INSERT INTO transaction_test (value) VALUES ('test1')");
          // This will cause a server exception
          await conn.execute('SELECT * FROM non_existent_table');
        }),
        throwsA(isA<MySQLServerException>()),
      );

      // Verify rollback occurred
      final IResultSet result = await connection.execute('SELECT COUNT(*) as count FROM transaction_test');
      expect(result.rows.first.colByName('count'), '0');
    });

    test('transaction rolls back on any exception', () async {
      await expectLater(
        () => connection.transactional<void>((MySQLConnection conn) async {
          await conn.execute("INSERT INTO transaction_test (value) VALUES ('test1')");
          throw Exception('Generic exception');
        }),
        throwsA(isA<Exception>()),
      );

      // Verify rollback occurred
      final IResultSet result = await connection.execute('SELECT COUNT(*) as count FROM transaction_test');
      expect(result.rows.first.colByName('count'), '0');
    });

    test('transaction commits successfully', () async {
      await connection.transactional<void>((MySQLConnection conn) async {
        await conn.execute("INSERT INTO transaction_test (value) VALUES ('test1')");
        await conn.execute("INSERT INTO transaction_test (value) VALUES ('test2')");
      });

      // Verify commit occurred
      final IResultSet result = await connection.execute('SELECT COUNT(*) as count FROM transaction_test');
      expect(result.rows.first.colByName('count'), '2');
    });

    test('transaction state is reset after completion', () async {
      expect(connection.inTransaction, false);

      await connection.transactional<void>((MySQLConnection conn) async {
        expect(conn.inTransaction, true);
        await conn.execute("INSERT INTO transaction_test (value) VALUES ('test1')");
      });

      expect(connection.inTransaction, false);
    });

    test('transaction state is reset after rollback', () async {
      expect(connection.inTransaction, false);

      try {
        await connection.transactional<void>((MySQLConnection conn) async {
          expect(conn.inTransaction, true);
          throw Exception('Test error');
        });
      } catch (_) {
        // Expected
      }

      expect(connection.inTransaction, false);
    });
  });

  group('Double Completer Completion Protection', () {
    late MySQLConnection connection;

    setUpAll(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();
    });

    tearDownAll(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('sequential execute calls work correctly', () async {
      // MySQL connections should execute queries sequentially
      // Test that sequential execution works properly without double completion
      for (int i = 0; i < 10; i++) {
        final IResultSet result = await connection.execute('SELECT $i as val');
        expect(result.rows.first.colByName('val'), i.toString());
      }
    });

    test('multiple concurrent execute calls are queued and complete correctly', () async {
      // With _operationLock, concurrent queries are now properly queued and executed sequentially
      // All queries should complete successfully without race conditions

      // Start multiple queries concurrently
      final List<Future<IResultSet>> futures = <Future<IResultSet>>[];
      for (int i = 0; i < 10; i++) {
        futures.add(connection.execute('SELECT $i as val'));
      }

      // All should complete successfully (they execute sequentially via _operationLock)
      final List<IResultSet> results = await Future.wait(futures);
      expect(results.length, 10);

      // Verify all results are correct
      for (int i = 0; i < 10; i++) {
        expect(results[i].rows.first.colByName('val'), i.toString());
      }
    });

    test('connection close during execute does not cause double completion', () async {
      final Future<IResultSet> executeFuture = connection.execute('SELECT SLEEP(0.1)');

      // Close connection while query is executing
      unawaited(connection.close());

      // Should either complete or throw, but not crash
      try {
        await executeFuture;
      } catch (e) {
        expect(e, isA<MySQLClientException>());
      }
    });
  });

  group('Response Callback Race Condition Tests', () {
    late MySQLConnection connection;

    setUp(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();
    });

    tearDown(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('connection close during query execution handles null callback gracefully', () async {
      // Start a query
      final Future<IResultSet> queryFuture = connection.execute('SELECT SLEEP(0.1)');

      // Close connection immediately (this will null out _responseCallback)
      unawaited(connection.close());

      // Should handle gracefully without crashing
      try {
        await queryFuture;
      } catch (e) {
        // Expected to fail, but should be a proper exception, not a null pointer
        expect(e, isA<Exception>());
      }
    });

    test('multiple concurrent queries handle callback correctly', () async {
      final List<Future<IResultSet>> futures = <Future<IResultSet>>[];
      for (int i = 0; i < 5; i++) {
        futures.add(connection.execute('SELECT $i as val'));
      }

      // All should complete successfully
      final List<IResultSet> results = await Future.wait(futures);
      expect(results.length, 5);
    });
  });

  group('State Management Tests', () {
    late MySQLConnection connection;

    setUp(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
    });

    tearDown(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('connection state transitions correctly', () async {
      expect(connection.isOpen, false);

      await connection.connect();
      expect(connection.isOpen, true);

      // Execute a query
      final IResultSet result = await connection.execute('SELECT 1');
      expect(result.numOfRows, 1);
      expect(connection.isOpen, true);

      await connection.close();
      expect(connection.isOpen, false);
    });

    test('execute fails on closed connection', () async {
      await connection.connect();
      await connection.close();

      await expectLater(
        () => connection.execute('SELECT 1'),
        throwsA(
          isA<MySQLClientException>().having(
            (MySQLClientException e) => e.code,
            'code',
            MySQLClientExceptionCode.closedConnection,
          ),
        ),
      );
    });

    test('prepare fails on closed connection', () async {
      await connection.connect();
      await connection.close();

      await expectLater(
        () => connection.prepare('SELECT ?'),
        throwsA(
          isA<MySQLClientException>().having(
            (MySQLClientException e) => e.code,
            'code',
            MySQLClientExceptionCode.closedConnection,
          ),
        ),
      );
    });
  });

  group('Error Recovery Tests', () {
    late MySQLConnection connection;

    setUp(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();
    });

    tearDown(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('connection recovers after query error', () async {
      // Execute a query that will fail
      await expectLater(
        () => connection.execute('SELECT * FROM non_existent_table_12345'),
        throwsA(isA<MySQLServerException>()),
      );

      // Connection should still be open and usable
      expect(connection.isOpen, true);

      // Should be able to execute another query
      final IResultSet result = await connection.execute('SELECT 1 as test');
      expect(result.rows.first.colByName('test'), '1');
    });

    test('connection recovers after prepared statement error', () async {
      // Try to prepare a statement with syntax error
      await expectLater(
        () => connection.prepare('SELECT * FROM non_existent_table WHERE'),
        throwsA(isA<MySQLServerException>()),
      );

      // Connection should still be open
      expect(connection.isOpen, true);

      // Should be able to prepare a valid statement
      final PreparedStmt stmt = await connection.prepare('SELECT ? as test');
      final IResultSet result = await stmt.execute(<dynamic>[42]);
      expect(result.rows.first.colByName('test'), '42');
      await stmt.deallocate();
    });

    test('connection state is restored after parameter substitution error', () async {
      // Try to execute with invalid parameters
      await expectLater(
        () => connection.execute(
          'SELECT * FROM book WHERE id = :id',
          params: <String, dynamic>{'wrong_param': 1},
        ),
        throwsA(isA<MySQLClientException>()),
      );

      // Connection should still be open
      expect(connection.isOpen, true);

      // Should be able to execute another query
      final IResultSet result = await connection.execute('SELECT 1 as test');
      expect(result.rows.first.colByName('test'), '1');
    });
  });

  group('Concurrent Operations Tests', () {
    late MySQLConnection connection;

    setUp(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();
    });

    tearDown(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('sequential operations work correctly', () async {
      for (int i = 0; i < 10; i++) {
        final IResultSet result = await connection.execute('SELECT $i as val');
        expect(result.rows.first.colByName('val'), i.toString());
      }
    });

    test('transaction uses operation lock to prevent concurrent operations', () async {
      // Transactions use _operationLock, so operations inside a transaction
      // will block other operations on the same connection

      // Start a transaction that takes some time
      final Future<void> transactionFuture = connection.transactional<void>((MySQLConnection conn) async {
        await conn.execute('SELECT SLEEP(0.1)');
      });

      // Try to execute another operation - should wait for transaction to complete
      // because transactional() uses _operationLock
      final Future<IResultSet> otherFuture = connection.execute('SELECT 1 as val');

      // Both should complete (transaction first, then the other query)
      await Future.wait(<Future<void>>[transactionFuture, otherFuture]);

      // Verify the second query completed correctly
      final IResultSet result = await otherFuture;
      expect(result.rows.first.colByName('val'), '1');
    });

    test('concurrent execute() calls are properly queued', () async {
      // With _operationLock, concurrent execute() calls are now properly queued
      // and executed sequentially. All queries complete successfully.

      // Start multiple queries concurrently
      final List<Future<IResultSet>> futures = <Future<IResultSet>>[];
      for (int i = 0; i < 5; i++) {
        futures.add(connection.execute('SELECT $i as val'));
      }

      // All should complete successfully
      final List<IResultSet> results = await Future.wait(futures);
      expect(results.length, 5);

      // Verify results
      for (int i = 0; i < 5; i++) {
        expect(results[i].rows.first.colByName('val'), i.toString());
      }
    });
  });

  group('Connection Lifecycle Tests', () {
    test('onClose callbacks are called', () async {
      final MySQLConnection connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();

      int callbackCount = 0;
      connection
        ..onClose(() {
          callbackCount++;
        })
        ..onClose(() {
          callbackCount++;
        });

      await connection.close();
      expect(callbackCount, 2);
    });

    test('closed future completes on close', () async {
      final MySQLConnection connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();

      final Future<void> closedFuture = connection.closed;
      await connection.close();
      await expectLater(closedFuture, completes);
    });
  });

  group('Edge Cases', () {
    late MySQLConnection connection;

    setUp(() async {
      connection = await MySQLConnection.createConnection(
        endpoint: testEndpoint,
        timeoutDuration: const Duration(seconds: 5),
      );
      await connection.connect();
    });

    tearDown(() async {
      if (connection.isOpen) {
        await connection.close();
      }
    });

    test('empty result set handling', () async {
      final IResultSet result = await connection.execute('SELECT 1 WHERE 1 = 0');
      expect(result.numOfRows, 0);
      expect(result.numOfColumns, 1);
    });

    test('large result set handling', () async {
      // Create a table with many rows
      await connection.execute('''
        CREATE TEMPORARY TABLE IF NOT EXISTS large_test (
          id INT,
          value VARCHAR(255)
        )
      ''');

      // Insert many rows
      for (int i = 0; i < 100; i++) {
        await connection.execute(
          'INSERT INTO large_test (id, value) VALUES (:id, :val)',
          params: <String, dynamic>{'id': i, 'val': 'value_$i'},
        );
      }

      // Query all rows
      final IResultSet result = await connection.execute('SELECT * FROM large_test');
      expect(result.numOfRows, 100);
    });

    test('multiple result sets', () async {
      final IResultSet result = await connection.execute('SELECT 1 as a; SELECT 2 as b, 3 as c');

      expect(result.next, isNotNull);
      final List<IResultSet> results = result.toList();
      expect(results.length, 2);
      expect(results[0].rows.first.colByName('a'), '1');
      expect(results[1].rows.first.colByName('b'), '2');
      expect(results[1].rows.first.colByName('c'), '3');
    });
  });
}
