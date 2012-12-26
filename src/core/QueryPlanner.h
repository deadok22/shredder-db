#pragma once

#include <vector>
#include "TableMetadata.pb.h"
#include "../sqlparser/SqlStatement.h"
#include "RecordsIterator.h"
#include "RecordComparer.h"

class QueryPlanner {
public:
  enum TraversalType { RANGE_FW, RANGE_BW, LOOK_UP, FULL_SCAN};
public:
  static QueryPlanner & get_instance();
  RecordsIterator * execute_select(TableMetaData const &table, std::vector<WhereClause::Predicate> const &restrictions);
private:
  struct SampleRecordCtx {
    RecordComparer *cmp;
    char * sample_record;
  };
private:
  RecordsIterator * handle_query_using_index(TableMetaData const & table, std::string const & index_name, bool is_btree, WhereClause::Predicate const &predicat);
  RecordsIterator * handle_filtering_with_no_index(TableMetaData const &table, WhereClause::Predicate const &predicat);
  RecordsIterator * handle_single_predicate_filtering(TableMetaData const &table, WhereClause::Predicate const &predicat);

  void init_sample_record(TableMetaData const &table, WhereClause::Predicate const &predicat, SampleRecordCtx *ctx, bool index_spec = false);
  QueryPlanner::TraversalType predicat_to_traversal(WhereClause::Predicate const &predicat);

private:
  static long long const ALLOWED_FILTER_MISS = 400; // 4000 / 12 ~ 400
  static QueryPlanner * instance_;
};