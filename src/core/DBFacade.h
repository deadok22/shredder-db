#pragma once

#include "../sqlparser/SqlStatement.h"

class DBFacade {
public:
  static DBFacade * get_instance();

  void execute_statement(SqlStatement const *);
  void execute_statement(CreateTableStatement const *);
  void execute_statement(SelectStatement const *);
  void execute_statement(InsertStatement const *);
  void execute_statement(CreateIndexStatement const *);
  void execute_statement(DeleteStatement const *);
  void execute_statement(UpdateStatement const *);

private:
  static DBFacade * instance_;
  void create_empty_file(std::string const & fname);
};


#ifdef TEST_DBF
#include <vector>

int main(void) {
  std::vector<TableColumn>  columns;
  
  DataType vc_type(VARCHAR, 4);
  TableColumn name_column("name", vc_type);
  TableColumn age_column("name", DataType::get_int());
  columns.push_back(name_column);
  columns.push_back(age_column);

  CreateTableStatement c_stms("test_table", columns);
  DBFacade::get_instance()->execute_statement(c_stms);

  return 0;
}
#endif
