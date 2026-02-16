import 'package:mysql_client_fork/mysql_client.dart';

Future<void> main(List<String> arguments) async {
  print('Connecting to mysql server...');

  // create connection
  final MySQLConnection conn = await MySQLConnection.createConnection(
    endpoint: const Endpoint(
      host: '127.0.0.1',
      username: 'your_user',
      password: 'your_password',
      database: 'your_database_name', // optional
    ),
  );

  await conn.connect();

  print('Connected');

  // insert some data
  PreparedStmt stmt = await conn.prepare('INSERT INTO book (author_id, title, price, created_at) VALUES (?, ?, ?, ?)');

  await stmt.execute(<dynamic>[null, 'Some book 1', 120, '2022-01-01']);
  await stmt.execute(<dynamic>[null, 'Some book 2', 10, '2022-01-01']);
  await stmt.deallocate();

  // select data
  stmt = await conn.prepare('SELECT * FROM book');
  final IResultSet result = await stmt.execute(<dynamic>[]);
  await stmt.deallocate();

  for (final ResultSetRow row in result.rows) {
    print(row.assoc());
  }

  // close all connections
  await conn.close();
}
