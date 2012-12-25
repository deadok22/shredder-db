#include "QueryPlanner.h"
#include "../common/Utils.h"

#include "../backend/HeapFileManager.h"
#include "indices/BTreeIndexManager.h"

QueryPlanner * QueryPlanner::instance_ = new QueryPlanner();

QueryPlanner & QueryPlanner::get_instance() {
  return *QueryPlanner::instance_;
}

RecordsIterator * QueryPlanner::executeSelect(TableMetaData const &table, std::vector<WhereClause::Predicate> const &restrictions) {
  if (restrictions.size() > 1) {
    Utils::error("[QueryPlanner] Multiple lauses are not supported yet. :(");
    return new EmptyIterator();
  }

  if (restrictions.size() == 0) {
    //full select is required
    return new HeapFileManager::HeapRecordsIterator(table.name());
  }

  //TODO FIX
  return new BTreeIndexManager::SortedIterator(table.name(), "a");
}


