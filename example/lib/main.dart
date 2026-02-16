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

  // update some rows
  final IResultSet res = await conn.execute('UPDATE book SET price = :price', params: <String, dynamic>{'price': 200});

  print(res.affectedRows);

  // make query
  final IResultSet result = await conn.execute('SELECT * FROM book');

  // print some result data
  print(result.numOfColumns);
  print(result.numOfRows);
  print(result.lastInsertID);
  print(result.affectedRows);

  // print query result
  for (final ResultSetRow row in result.rows) {
    // print(row.colAt(0));
    // print(row.colByName("title"));

    // print all rows as Map<String, String>
    print(row.assoc());
  }

  // or you can use stream interface (which is required for iterable results)

  result.rowsStream.listen((ResultSetRow row) {
    print(row.assoc());
  });

  // close all connections
  await conn.close();
}
