#include <string>
#include <fstream>

#include "MetaDataProvider.h"
#include "DBFacade.h"
#include "../common/Utils.h" 
#include "../common/InfoPool.h" 

DBFacade * DBFacade::instance_ = new DBFacade();

DBFacade * DBFacade::get_instance() {
  return instance_;
}

void DBFacade::execute_statement(SqlStatement const * stmt) {
  switch (stmt->get_type()) {
    case SELECT:
      execute_statement((SelectStatement const *) stmt);
      break;
    case INSERT:
      execute_statement((InsertStatement const *) stmt);
      break;
    case CREATE_TABLE:
      execute_statement((CreateTableStatement const *) stmt);
      break;
    case CREATE_INDEX:
      //TODO IMPL;
      break;
    case UPDATE:
      //TODO IMPL;
      break;
    case DELETE:
      //TODO IMPL;
      break;
    case UNKNOWN:
      Utils::warning("[DBF] Given statement is unknown");
      break;
  }
}

void DBFacade::execute_statement(CreateTableStatement const * stmt) {
  Utils::info("[DBFacade][EXEC_START] Create statement");

  std::vector<TableColumn> columns = stmt->get_columns();

  TableMetaData table_metadata;
  table_metadata.set_name(stmt->get_table_name());
  Utils::info("[DBFacade] Table name is " + stmt->get_table_name());
  

  for (std::vector<TableColumn>::iterator it = columns.begin();
      it != columns.end();
      ++it) {

    TableColumn column = *it;
    TableMetaData_AttributeDescription * attr = table_metadata.add_attribute();
    
    attr->set_name(column.get_name());
    attr->set_type_name(column.get_data_type().get_type_code());
    attr->set_size(column.get_data_type().get_size());
  }

  MetaDataProvider::get_instance()->save_meta_data(&table_metadata);

  //create file for data
  std::string table_folder = InfoPool::get_instance()->get_db_info()->root_path + table_metadata.name();
  std::string data_path = table_folder + "/data";
  std::fstream output(data_path.c_str(), ios::out | ios::trunc | ios::binary);
  output.close();

  Utils::info("[DBFacade][EXEC_END] Create statement");
}

void DBFacade::execute_statement(SelectStatement const * stmt) {
  Utils::info("[EXEC_START] Select statement");
}

void DBFacade::execute_statement(InsertStatement const * stmt) {
  Utils::info("[EXEC_START] Insert statement");
}
