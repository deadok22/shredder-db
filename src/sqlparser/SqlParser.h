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
    SqlStatementType get_sql_statement_type(std::string const & statement_text) const;
    SqlStatement const * parse_create_table_statement(std::string const & statement_text) const;
    SqlStatement const * parse_create_index_statement(std::string const & statement_text) const;
    SqlStatement const * parse_insert_statement(std::string const & statement_text) const;
    SqlStatement const * parse_select_statement(std::string const & statement_text) const;
    std::vector<std::string> parse_comma_separated_values(std::string const & values_string) const;
    std::vector<TableColumn> parse_table_columns(std::string const & columns_string) const;
    std::vector<CreateIndexStatement::Column> parse_create_index_columns(std::string const & columns_string) const;
    WhereClause parse_where_clause(std::string const & predicates_string) const;
};

//TEST_CODE
#ifdef TEST_SQLPARSER
int main() {
  SqlParser parser;
  delete parser.parse("CREATE TABLE table_name(a INT, b DOUBLE, c VArCHAR(50))");
  
  delete parser.parse("SELECT * FROM table_name");
  delete parser.parse("SELECT l, l, a, b, c FROM table_name");
  delete parser.parse("select a, b, c from table_name where a < 10 and b < 146");
  delete parser.parse("select a, b, c from table_name where a = 10 and b != 146");
  
  delete parser.parse("INSERT INTO table_name(a, b, c) VALUES(1, 2, 3)");
  delete parser.parse("insert into table_name(a, b, c) values(10, 11.2, \"one, two, three =)))\")");
  delete parser.parse("insert into table_name(a, b, c) values(\"\"\"\", \"\", a cat named \"Fluffy\")");
  
  delete parser.parse("CREATE INDEX my_index ON my_table(a, b DESC, c ASC) USING BTREE");
  delete parser.parse("CREATE UNIQUE INDEX my_index ON my_table(a ASC) USING HASH");
}
#endif
