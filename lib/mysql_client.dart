export 'src/mysql_client/connection.dart';
export 'src/mysql_client/pool.dart';
export 'src/mysql_client/util/util.dart';

final class Endpoint {
  final dynamic host;
  final int port;
  final String? database;
  final String username;
  final String password;
  final bool isUnixSocket;
  final bool secure;

  const Endpoint({
    required this.host,
    this.database,
    required this.username,
    required this.password,
    this.port = 3306,
    this.isUnixSocket = false,
    this.secure = true,
  });

  @override
  int get hashCode => Object.hash(host, port, database, username, password, isUnixSocket, secure);

  @override
  bool operator ==(Object other) {
    return other is Endpoint &&
        host == other.host &&
        port == other.port &&
        database == other.database &&
        username == other.username &&
        password == other.password &&
        isUnixSocket == other.isUnixSocket &&
        secure == other.secure;
  }
}
