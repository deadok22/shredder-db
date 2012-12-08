#pragma once

#include "../sqlparser/SqlStatement.h"

class DBFacade {
public:
  static DBFacade * get_instance();

  void execude_statement(CreateTableStatement const &);
  void execude_statement(SelectStatement const &);
  void execude_statement(InsertStatement const &);

private:
  static DBFacade * instance_;

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
  DBFacade::get_instance()->execude_statement(c_stms);

  return 0;
}
#endif