#pragma once

#include <vector>
#include "TableMetadata.pb.h"
#include "../sqlparser/SqlStatement.h"
#include "RecordsIterator.h"

class QueryPlanner {
public:
  static QueryPlanner & get_instance();
  RecordsIterator * executeSelect(TableMetaData const &table, std::vector<WhereClause::Predicate> const &restrictions);
private:
  static QueryPlanner * instance_;
};