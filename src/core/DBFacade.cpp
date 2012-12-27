#include <string>
#include <fstream>
#include <cstdlib>

#include "MetaDataProvider.h"
#include "DBFacade.h"
#include "../common/Utils.h" 
#include "../common/InfoPool.h" 
#include "../backend/PagesDirectory.h"
#include "../backend/HeapFileManager.h"
#include "../backend/BufferManager.h"
#include "QueryPlanner.h"

#include "indices/IndexManager.h"
#include "indices/BTreeIndexManager.h"
#include "indices/ExtIndexManager.h"
#include "indices/IndexOperationParams.h"
#include "RecordsIterator.h"
#include "CsvPrinter.h"

typedef google::protobuf::RepeatedPtrField<TableMetaData_IndexMetadata>::const_iterator index_const_iter;

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
      execute_statement((UpdateStatement const *) stmt);
      break;
    case DELETE:
      execute_statement((DeleteStatement const *) stmt);
      break;
    case DROP:
      execute_statement((DropStatement const *) stmt);  
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
    TableMetaData_IndexMetadata_KeyInfo *key = index_metadata.add_keys();
    key->set_name(column.name);
    key->set_asc(!column.is_descending);
  }

#ifdef DBFACADE_DBG
  Utils::info("[DBFacade] Saving index metadata");
#endif
  if (!MetaDataProvider::add_index_info(stmt->get_table_name(), index_metadata)) {
    std::cout << "Unable to save index description" << std::endl;
    return;
  }

  //TODO fix magic number
  if (index_metadata.type() == 0) {
#ifdef DBFACADE_DBG
    Utils::info("[DBFacade] Creating Hash index with Extendable Hash table implementation");  
#endif
    ExtIndexManager::create_index(stmt->get_table_name(), index_metadata);
  } else {
    BTreeIndexManager::create_index(stmt->get_table_name(), index_metadata);
  }

  std::cout << "Index creation finished." << std::endl;
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
  TableMetaData * metadata = MetaDataProvider::get_instance()->get_meta_data(stmt->get_table_name());

  if (metadata == NULL) {
#ifdef DBFACADE_DBG
    Utils::error("[DBFacade][Select stmt] Table " + stmt->get_table_name() + " doesn't exist");
#endif
    return;
  }

  std::vector<WhereClause::Predicate> conds = stmt->has_where_clause() ? stmt->get_where_clause().get_predicates() : vector<WhereClause::Predicate>();
  RecordsIterator *rec_itr = QueryPlanner::get_instance().execute_select(*metadata, conds);

  unsigned returned_records = 0;
  std::cout << CsvPrinter::get_instance().get_header_csv(*metadata, stmt->get_column_names());
  while (rec_itr->next()) {
    std::cout << CsvPrinter::get_instance().get_csv(**rec_itr, *metadata, stmt->get_column_names());
    ++returned_records;
  }
  std::cout << "Total " << returned_records << " record(s)." << std::endl;

  delete rec_itr;
}

void DBFacade::execute_statement(InsertStatement const * stmt) {
  HeapFileManager &hfm = HeapFileManager::get_instance();
  TableMetaData * metadata = MetaDataProvider::get_instance()->get_meta_data(stmt->get_table_name());

  if (metadata == NULL) {
    Utils::error("[DBFacade][Insert stmt] Table " + stmt->get_table_name() + " doesn't exist");
    return;
  }

  HeapFileManager::HeapFMOperationResult insertion_result;
  insertion_result.record_data = new char[metadata->record_size()]();

  hfm.process_insert_record(*metadata, stmt->get_column_names(), stmt->get_values(), &insertion_result);

  //add to all indices
  for (index_const_iter i = metadata->indices().begin(); i != metadata->indices().end(); ++i) {
    IndexManager *index_mgr = NULL;
    switch (i->type()) {
      case IndexManager::HASH: index_mgr = new ExtIndexManager(metadata->name(), i->name()); break;
      case IndexManager::BTREE: index_mgr = new BTreeIndexManager(metadata->name(), i->name()); break;
    }

    if (index_mgr == NULL) {
      Utils::warning("Can't update index named " + i->name() + ". It has unsupported type");
      continue;
    }

    IndexOperationParams params;
    params.value_size = IndexManager::compute_key_size(*metadata, *i);
    params.value = new char[params.value_size]();
    params.page_id = insertion_result.record_page_id;
    params.slot_id = insertion_result.record_slot_id;
    IndexManager::init_params_with_record(*metadata, *i, insertion_result.record_data, &params);

    index_mgr->insert_value(params);

    delete [] (char *)params.value;
    delete index_mgr;
  }

  delete [] insertion_result.record_data;
  std::cout << "OK. 1 row affected" << std::endl;
}

void DBFacade::execute_statement(UpdateStatement const * stmt) {
  HeapFileManager &hfm = HeapFileManager::get_instance();  
  TableMetaData * metadata = MetaDataProvider::get_instance()->get_meta_data(stmt->get_table_name());

  if (metadata == NULL) {
#ifdef DBFACADE_DBG
    Utils::error("[DBFacade][Update stmt] Table " + stmt->get_table_name() + " doesn't exist");
#endif
    return;
  }

  std::vector<WhereClause::Predicate> conds = stmt->get_where_clause().get_predicates();
  RecordsIterator *rec_itr = QueryPlanner::get_instance().execute_select(*metadata, conds);

  while (rec_itr->next()) {
    hfm.process_update_record(*metadata, rec_itr->record_page_id(), rec_itr->record_slot_id(), stmt->get_column_names(), stmt->get_values());
  }

  delete rec_itr;
}

void DBFacade::execute_statement(DeleteStatement const * stmt) {
  HeapFileManager &hfm = HeapFileManager::get_instance();  
  TableMetaData * metadata = MetaDataProvider::get_instance()->get_meta_data(stmt->get_table_name());

  if (metadata == NULL) {
#ifdef DBFACADE_DBG
    Utils::error("[DBFacade][Delete stmt] Table " + stmt->get_table_name() + " doesn't exist");
#endif
    return;
  }

  std::vector<WhereClause::Predicate> conds = stmt->get_where_clause().get_predicates();
  RecordsIterator *rec_itr = QueryPlanner::get_instance().execute_select(*metadata, conds);

  while (rec_itr->next()) {
    hfm.process_delete_record(*metadata, rec_itr->record_page_id(), rec_itr->record_slot_id());
  }

  delete rec_itr;
}


void DBFacade::execute_statement(DropStatement const * stmt) {
  BufferManager &bm = BufferManager::get_instance();
  std::string table_dir = Utils::get_table_dir(stmt->get_table_name());
  if( Utils::check_existence(table_dir, true) ) {
    bm.force(stmt->get_table_name());
    string cmd = "rm -r " + table_dir;    
    system(cmd.c_str());
  } else {
    Utils::error("[DBFacade][Drop stmt] Table " + stmt->get_table_name() + " doesn't exist");
  }  
}

void DBFacade::create_empty_file(std::string const & fname) {
  std::fstream output(fname.c_str(), ios::out | ios::trunc);
  output.close();
}
