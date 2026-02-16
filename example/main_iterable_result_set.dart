import 'package:mysql_client_fork/mysql_client.dart';

Future<void> main(List<String> arguments) async {
  print('Connecting to mysql server...');

  // Create connection
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

  // make query (notice third parameter, iterable=true)
  final IResultSet result = await conn.execute('SELECT * FROM book', params: <String, dynamic>{}, iterable: true);

  // print some result data
  // (numOfRows is not available when using iterable result set)
  print(result.numOfColumns);
  print(result.lastInsertID);
  print(result.affectedRows);

  // get rows, one by one
  result.rowsStream.listen((ResultSetRow row) {
    print(row.assoc());
  });

  // close all connections
  await conn.close();
}
