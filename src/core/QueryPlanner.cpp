#include "QueryPlanner.h"
#include "../common/Utils.h"

#include "../backend/HeapFileManager.h"
#include "indices/BTreeIndexManager.h"
#include "indices/ExtIndexManager.h"
#include "FilteringIterator.h"
#include "RecordComparer.h"

QueryPlanner * QueryPlanner::instance_ = new QueryPlanner();

QueryPlanner & QueryPlanner::get_instance() {
  return *QueryPlanner::instance_;
}

typedef google::protobuf::RepeatedPtrField<TableMetaData_AttributeDescription>::const_iterator attr_const_iter;
typedef google::protobuf::RepeatedPtrField<TableMetaData_IndexMetadata>::const_iterator index_const_iter;
typedef google::protobuf::RepeatedPtrField<TableMetaData_IndexMetadata_KeyInfo>::const_iterator key_const_iter;

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

QueryPlanner::TraversalType QueryPlanner::predicat_to_traversal(WhereClause::Predicate const &predicat) {
  switch (predicat.type) {
    case WhereClause::EQ: return LOOK_UP;
    case WhereClause::NEQ: case WhereClause::UNKNOWN: return FULL_SCAN;
    case WhereClause::GTOE: case WhereClause::GT: return RANGE_BW;
    case WhereClause::LTOE: case WhereClause::LT: return RANGE_FW;
  }

  return FULL_SCAN;
}

RecordsIterator * QueryPlanner::handle_single_predicate_filtering(TableMetaData const &table, WhereClause::Predicate const &predicat) {
  TraversalType trav_type = predicat_to_traversal(predicat);
  
  if (trav_type == FULL_SCAN) {
    return handle_filtering_with_no_index(table, predicat);
  }

  bool best_match_found = false;
  std::string index_name = ""; 
  bool is_tree = false;
  for (index_const_iter i = table.indices().begin(); i != table.indices().end() && !best_match_found; ++i) {
    for (key_const_iter k = i->keys().begin(); k != i->keys().end(); ++k) {
      if (k->name() == predicat.column) { //BINGO!
        if (trav_type == LOOK_UP) {
          if (i->keys_size() == 1 && i->type() == 0) { //HASH
            best_match_found = true;
            index_name = i->name();
            is_tree = false;
          } else if (i->type() == 1) { //BTREE
            index_name = i->name();
            is_tree = true;
          }
        } else if (trav_type == RANGE_FW || trav_type == RANGE_BW){
          if (i->type() == 1) {
            index_name = i->name();
            best_match_found = i->keys_size() == 1;
            is_tree = true;
          }
        }
      }
    }
  }

#ifdef QPLAN
  if (!index_name.empty()) {
    Utils::info("[QueryPlan] Going to perform query using '" + index_name + "' " + (is_tree ? "BTREE" : "HASH")+ " index");
  } else {
    Utils::warning("[QueryPlan] No index was found. Perform FULL SCAN");
  }
#endif

  return index_name.empty() ?
    handle_filtering_with_no_index(table, predicat) :
    handle_query_using_index(table, index_name, is_tree, predicat);
}

RecordsIterator * QueryPlanner::handle_filtering_with_no_index(TableMetaData const &table, WhereClause::Predicate const &predicat) {
  SampleRecordCtx ctx;
  init_sample_record(table, predicat, &ctx);
  return new FilteringIterator(new HeapFileManager::HeapRecordsIterator(table.name()), ctx.cmp, predicat.type, ctx.sample_record);
}

RecordsIterator * QueryPlanner::handle_query_using_index(TableMetaData const & table, std::string const & index_name, bool is_btree, WhereClause::Predicate const &predicat) {
  RecordsIterator * iterator;

  TraversalType trav_type = predicat_to_traversal(predicat);

  SampleRecordCtx ind_ctx;
  init_sample_record(table, predicat, &ind_ctx, true);
  if (is_btree) {
    if (trav_type == RANGE_BW) {
      delete [] ind_ctx.sample_record;
      //traverse from start
      iterator = new BTreeIndexManager::SortedIterator(table.name(), index_name, NULL);
    } else {
      iterator = new BTreeIndexManager::SortedIterator(table.name(), index_name, ind_ctx.sample_record);
      //ind_ctx.sample_record will be deleted by iterator
    }
  } else {
    iterator = new ExtIndexManager::BucketIterator(table.name(), index_name, ind_ctx.sample_record, table.record_size());
     //ind_ctx.sample_record will be deleted by iterator
  }
  delete ind_ctx.cmp;

  if (iterator == NULL) { iterator = new EmptyIterator(); }

  SampleRecordCtx ctx;
  init_sample_record(table, predicat, &ctx);

  return new FilteringIterator(iterator, ctx.cmp, predicat.type, ctx.sample_record, is_btree ? ALLOWED_FILTER_MISS : -1);
}

void QueryPlanner::init_sample_record(TableMetaData const &table, WhereClause::Predicate const &predicat, SampleRecordCtx *ctx, bool index_spec) {
  std::vector<RecordComparer::ComparisonRule> rules;
  rules.push_back(predicat.column);
  RecordComparer * cmp = new RecordComparer(table, rules);

  char * sample_record = new char[table.record_size()]();
  unsigned offset = 0;
  for (attr_const_iter i = table.attribute().begin(); i != table.attribute().end(); ++i) {
    if (i->name() == predicat.column) {
      //TODO WORKS ONLY for Single clauses
      if (index_spec) { offset = 0;}
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

  ctx->cmp = cmp;
  ctx->sample_record = sample_record;
}
