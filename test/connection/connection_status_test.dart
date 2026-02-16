import 'package:mysql_client_fork/exception.dart';
import 'package:mysql_client_fork/mysql_client.dart';
import 'package:test/test.dart';

/// This test assumes the local db is running
void main() {
  late MySQLConnection connection;

  setUpAll(() async {
    connection = await MySQLConnection.createConnection(
      endpoint: const Endpoint(
        database: 'dev',
        host: 'localhost',
        username: 'root',
        password: 'AKMFtdCq0ceHCA4g',
        secure: false,
      ),
      timeoutDuration: const Duration(seconds: 3),
    );

    await connection.connect();
  });

  tearDownAll(() async {
    if (connection.isOpen) {
      await connection.close();
    }
  });

  group('MySQLConnection status', () {
    test('The connection is established', () async {
      final IResultSet result = await connection.execute('SELECT version()');
      expect(result.rows.length, 1);
    });

    test('The connection is broken', () async {
      final IResultSet processResult = await connection.execute('show processlist;');
      final ResultSetRow currentConnection = processResult.rows.lastWhere((ResultSetRow row) {
        return row.colByName('User') == 'root' && row.colByName('db') == 'dev';
      });

      // Kill the current connection
      final String? connectionId = currentConnection.colByName('Id');

      await expectLater(
        () => connection.execute('KILL $connectionId;'),
        throwsA(
          isA<MySQLServerException>().having(
            (MySQLServerException e) => e.message,
            'message',
            'Query execution was interrupted',
          ),
        ),
      );

      await connection.close();

      await expectLater(
        () => connection.execute('select version();'),
        throwsA(
          isA<MySQLClientException>().having(
            (MySQLClientException e) => e.message,
            'message',
            contains('connection is in open state'),
          ),
        ),
      );
    });
  });
}
