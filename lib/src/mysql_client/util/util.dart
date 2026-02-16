import 'package:mysql_client_fork/exception.dart';

String substituteParams(String query, Map<String, dynamic> params) {
  // convert params to string
  final Map<String, String> convertedParams = <String, String>{};

  for (final MapEntry<String, dynamic> param in params.entries) {
    String value;

    final dynamic paramValue = param.value;
    if (paramValue == null) {
      value = 'NULL';
    } else if (paramValue is String) {
      value = "'${_escapeString(paramValue)}'";
    } else if (paramValue is num) {
      value = paramValue.toString();
    } else if (paramValue is bool) {
      value = paramValue ? 'TRUE' : 'FALSE';
    } else {
      value = "'${_escapeString(paramValue.toString())}'";
    }

    convertedParams[param.key] = value;
  }

  // find all :placeholders, which can be substituted
  final RegExp pattern = RegExp(r':(\w+)');

  final List<RegExpMatch> matches = pattern.allMatches(query).where((RegExpMatch match) {
    final String subString = query.substring(0, match.start);

    int count = "'".allMatches(subString).length;
    if (count > 0 && count.isOdd) {
      return false;
    }

    count = '"'.allMatches(subString).length;
    if (count > 0 && count.isOdd) {
      return false;
    }

    return true;
  }).toList();

  int lengthShift = 0;

  for (final RegExpMatch match in matches) {
    final String? paramName = match.group(1);

    // check param exists
    if (false == convertedParams.containsKey(paramName)) {
      throw MySQLClientException(
        'There is no parameter with name: $paramName',
        StackTrace.current,
        code: MySQLClientExceptionCode.invalidArgument,
      );
    }

    final String newQuery = query.replaceFirst(match.group(0)!, convertedParams[paramName]!, match.start + lengthShift);

    lengthShift += newQuery.length - query.length;
    query = newQuery;
  }

  return query;
}

String _escapeString(String value) {
  value = value.replaceAll(r'\', r'\\');
  value = value.replaceAll(r"'", r"''");
  return value;
}
