#include "QueryPlanner.h"
#include "../common/Utils.h"

#include "../backend/HeapFileManager.h"
#include "indices/BTreeIndexManager.h"
#include "FilteringIterator.h"
#include "RecordComparer.h"

QueryPlanner * QueryPlanner::instance_ = new QueryPlanner();

QueryPlanner & QueryPlanner::get_instance() {
  return *QueryPlanner::instance_;
}

typedef google::protobuf::RepeatedPtrField<TableMetaData_AttributeDescription>::const_iterator attr_const_iter;

RecordsIterator * QueryPlanner::execute_select(TableMetaData const &table, std::vector<WhereClause::Predicate> const &restrictions) {
  if (restrictions.size() > 1) {
    Utils::error("[QueryPlanner] Multiple clauses are not supported yet. :(");
    return new EmptyIterator();
  }

  if (restrictions.size() == 0) {
    //full select is required
    return new HeapFileManager::HeapRecordsIterator(table.name());
  } else if (restrictions.size() == 1) {
    return handle_single_predicate_filtering(table, restrictions[0]);
  }

  Utils::error("[QueryPlanner] Unhandled case. No idea what to do. :(");
  return new EmptyIterator();
}

RecordsIterator * QueryPlanner::handle_single_predicate_filtering(TableMetaData const &table, WhereClause::Predicate const &predicat) {
  //TODO CHECK indexes

  return handle_filtering_with_no_index(table, predicat);
}

RecordsIterator * QueryPlanner::handle_filtering_with_no_index(TableMetaData const &table, WhereClause::Predicate const &predicat) {
  std::vector<RecordComparer::ComparisonRule> rules;
  rules.push_back(predicat.column);
  RecordComparer * cmp = new RecordComparer(table, rules);

  char * sample_record = new char[table.record_size()];
  unsigned offset = 0;
  for (attr_const_iter i = table.attribute().begin(); i != table.attribute().end(); ++i) {
    if (i->name() == predicat.column) {
      if (i->type_name() == INT) {
        *((int *)(sample_record + offset)) = std::stoi(predicat.value);
      } else if (i->type_name() == DOUBLE) {
        *((double *)(sample_record + offset)) = std::stod(predicat.value);
      } else if (i->type_name() == VARCHAR) {
        memset(sample_record + offset, '\0', i->size()); //must mem set since value is used in hash counting
        memcpy(sample_record + offset, predicat.value.c_str(), i->size());
        *(sample_record + offset + i->size()) = '\0';
      }
      break;
    } else { 
      offset += i->size();
    }
  }

  return new FilteringIterator(new HeapFileManager::HeapRecordsIterator(table.name()), cmp, predicat.type, sample_record);
}

