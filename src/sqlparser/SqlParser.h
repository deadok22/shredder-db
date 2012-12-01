#pragma once

#include <string>
#include "SqlStatement.h"
#include "../common/Utils.h"

class SqlParser {
  public:
    SqlStatement parse(std::string const & statement_text);
};

//TEST_CODE
#ifdef TEST_SQLPARSER
int main() {
  SqlParser parser;
  parser.parse("CREATE TABLE table_name(INT a, DOUBLE b, VARCHAR(50) c)");
  parser.parse("SELECT * FROM table_name");
  parser.parse("SELECT a, b, c FROM table_name");
  parser.parse("INSERT INTO table_name(a, b, c) VALUES(1, 2, 3)");
}
#endif
