import 'package:mysql_client_fork/mysql_client.dart';

Future<void> main(List<String> arguments) async {
  // create connections pool
  final MySQLConnectionPool pool = MySQLConnectionPool(
    endpoint: const Endpoint(
      host: '127.0.0.1',
      username: 'your_user',
      password: 'your_password',
      database: 'your_database_name', // optional
    ),
  );

  // update table (inside transaction) and get total number of affected rows
  final int updateResult = await pool.transactional((MySQLConnection conn) async {
    int totalAffectedRows = 0;

    IResultSet res = await conn.execute('UPDATE book SET price = :price', params: <String, dynamic>{'price': 300});

    totalAffectedRows += res.affectedRows.toInt();

    res = await conn.execute('UPDATE book_author SET name = :name', params: <String, dynamic>{'name': 'John Doe'});

    totalAffectedRows += res.affectedRows.toInt();

    return totalAffectedRows;
  });

  // show total number of updated rows
  print(updateResult);

  // make query
  final IResultSet result = await pool.execute('SELECT * FROM book');

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

  // close all connections
  await pool.close();
}
