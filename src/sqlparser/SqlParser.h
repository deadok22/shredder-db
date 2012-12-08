#pragma once

#include <string>
#include "SqlStatement.h"
#include "../common/Utils.h"

class SqlParser {
  public:
    /**
     * Returns dynamically allocated object or null in case of error. (don't forget to 'delete' it)
     * UnknownStatement is returned in case of bad statement syntax.
     */
    SqlStatement const * parse(std::string const & statement_text) const;
  private:
    SqlStatementType getSqlStatementType(std::string const & statement_text) const;
    SqlStatement const * parseCreateTableStatement(std::string const & statement_text) const;
    SqlStatement const * parseInsertStatement(std::string const & statement_text) const;
    SqlStatement const * parseSelectStatement(std::string const & statement_text) const;
    std::vector<std::string> parseCommaSeparatedValues(std::string const & values_string) const;
};

//TEST_CODE
#ifdef TEST_SQLPARSER
int main() {
  SqlParser parser;
  delete parser.parse("CREATE TABLE table_name(INT a, DOUBLE b, VARCHAR(50) c)");
  delete parser.parse("SELECT * FROM table_name");
  delete parser.parse("SELECT l, l, a, b, c FROM table_name");
  delete parser.parse("INSERT INTO table_name(a, b, c) VALUES(1, 2, 3)");
}
#endif
