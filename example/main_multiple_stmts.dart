import 'package:mysql_client_fork/mysql_client.dart';

Future<void> main(List<String> arguments) async {
  print('Connecting to mysql server...');

  // create connection
  final MySQLConnection conn = await MySQLConnection.createConnection(
    endpoint: const Endpoint(
      host: '127.0.0.1',
      port: 3306,
      username: 'your_user',
      password: 'your_password',
      database: 'your_database_name', // optional
    ),
  );

  await conn.connect();

  print('Connected');

  final IResultSet resultSets = await conn.execute('SELECT 1 as val_1_1; SELECT 2 as val_2_1, 3 as val_2_2');

  assert(resultSets.next != null);

  for (final IResultSet result in resultSets) {
    // for every result set
    for (final ResultSetRow row in result.rows) {
      // for every row in result set
      print(row.assoc());
    }
  }

  // close all connections
  await conn.close();
}
