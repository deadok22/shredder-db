#include <string>

#include "MetaDataProvider.h"
#include "DBFacade.h"
#include "../common/Utils.h" 

DBFacade * DBFacade::instance_ = new DBFacade();

DBFacade * DBFacade::get_instance() {
  return instance_;
}

void DBFacade::execude_statement(CreateTableStatement const & stmt) {
  Utils::info("[DBFacade][EXEC_START] Create statement");

  std::vector<TableColumn> columns = stmt.get_columns();

  TableMetaData table_metadata;
  table_metadata.set_name(stmt.get_table_name());

  for (std::vector<TableColumn>::iterator it = columns.begin();
      it != columns.end();
      ++it) {

    TableColumn column = *it;
    TableMetaData_AttributeDescription * attr = table_metadata.add_attribute();
    
    attr->set_name(column.get_name());
    attr->set_type_name(column.get_data_type().get_type_code());
    attr->set_size(column.get_data_type().get_size());
  }

  MetaDataProvider::get_instance()->save_meta_data(table_metadata);

  Utils::info("[DBFacade][EXEC_END] Create statement");
}

void DBFacade::execude_statement(SelectStatement const & stmt) {
  Utils::info("[EXEC_START] Select statement");
}

void DBFacade::execude_statement(InsertStatement const & stmt) {
  Utils::info("[EXEC_START] Insert statement");
}
