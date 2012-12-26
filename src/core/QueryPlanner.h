#pragma once

#include <vector>
#include "TableMetadata.pb.h"
#include "../sqlparser/SqlStatement.h"
#include "RecordsIterator.h"

class QueryPlanner {
public:
  static QueryPlanner & get_instance();
  RecordsIterator * execute_select(TableMetaData const &table, std::vector<WhereClause::Predicate> const &restrictions);
private:
  RecordsIterator * handle_filtering_with_no_index(TableMetaData const &table, WhereClause::Predicate const &predicat);
  RecordsIterator * handle_single_predicate_filtering(TableMetaData const &table, WhereClause::Predicate const &predicat);
  static QueryPlanner * instance_;
};