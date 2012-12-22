#include <string>
#include <fstream>

#include "MetaDataProvider.h"
#include "DBFacade.h"
#include "../common/Utils.h" 
#include "../common/InfoPool.h" 
#include "../backend/PagesDirectory.h"
#include "../backend/HeapFileManager.h"

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
      execute_statement((CreateIndexStatement const *) stmt);
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

void DBFacade::execute_statement(CreateIndexStatement const * stmt) {
#ifdef DBFACADE_DBG
  Utils::info("[DBFacade][EXEC_START] Create index statement");
  Utils::info("  [DBFacade] Init index info");
#endif
  TableMetaData_IndexMetadata index_metadata;
  index_metadata.set_name(stmt->get_index_name());
  index_metadata.set_type(stmt->is_btree() ? 1 : 0);
  index_metadata.set_unique(stmt->is_unique());

  for (unsigned i = 0; i < stmt->get_columns().size(); ++i) {
    CreateIndexStatement::Column column = stmt->get_columns()[i];
    TableMetaData_IndexMetadata_KeyInfo *attr = index_metadata.add_keys();
    attr->set_name(column.name);
    attr->set_asc(!column.is_descending);
  }

  Utils::warning("[DBFacade] Actual index creation is not impelemnted");

#ifdef DBFACADE_DBG
  Utils::info("[DBFacade] Saving index metadata");
#endif
  if (!MetaDataProvider::add_index_info(stmt->get_table_name(), index_metadata)) {
    Utils::warning("[DBFacade] Unable to save index description");
  }
}

void DBFacade::execute_statement(CreateTableStatement const * stmt) {
#ifdef DBFACADE_DBG
  Utils::info("[DBFacade][EXEC_START] Create statement");
#endif
  std::vector<TableColumn> columns = stmt->get_columns();

  TableMetaData table_metadata;
  table_metadata.set_name(stmt->get_table_name());
#ifdef DBFACADE_DBG
  Utils::info("[DBFacade] Table name is " + stmt->get_table_name());
#endif

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

  //create heap file for data
  std::string table_folder = InfoPool::get_instance().get_db_info().root_path + table_metadata.name();
  std::string data_path = table_folder + "/data";

  create_empty_file(data_path);
  create_empty_file(PagesDirectory::get_directory_file_name(data_path));
  PagesDirectory::init_directory(data_path, table_metadata.records_per_page());
#ifdef DBFACADE_DBG
  Utils::info("[DBFacade][EXEC_END] Create statement");
#endif
}

void DBFacade::execute_statement(SelectStatement const * stmt) {
  HeapFileManager &hfm = HeapFileManager::get_instance();
  TableMetaData * metadata = MetaDataProvider::get_instance()->get_meta_data(stmt->get_table_name());

  if (metadata == NULL) {
#ifdef DBFACADE_DBG
    Utils::error("[DBFacade][Select stmt] Table " + stmt->get_table_name() + " doesn't exist");
#endif
    return;
  }

  hfm.print_all_records(*metadata);
  delete metadata;
}

void DBFacade::execute_statement(InsertStatement const * stmt) {
  HeapFileManager &hfm = HeapFileManager::get_instance();
  TableMetaData * metadata = MetaDataProvider::get_instance()->get_meta_data(stmt->get_table_name());

  if (metadata == NULL) {
#ifdef DBFACADE_DBG
    Utils::error("[DBFacade][Insert stmt] Table " + stmt->get_table_name() + " doesn't exist");
#endif
    return;
  }

  hfm.process_insert_record(*metadata, stmt->get_column_names(), stmt->get_values());
  cout << "Insert OK" << std::endl;
  delete metadata;
}

void DBFacade::create_empty_file(std::string const & fname) {
  std::fstream output(fname.c_str(), ios::out | ios::trunc);
  output.close();
}
