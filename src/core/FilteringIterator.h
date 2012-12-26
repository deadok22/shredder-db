#pragma once

#include "TableMetadata.pb.h"
#include "../sqlparser/SqlStatement.h"
#include "RecordsIterator.h"
#include "RecordComparer.h"

class FilteringIterator : public RecordsIterator{
public:
  FilteringIterator(RecordsIterator * data_provider, RecordComparer *comparer, int p_type, void * record_for_cmp);
  virtual ~FilteringIterator();

  virtual bool next();
  virtual void * operator*();
  virtual unsigned record_page_id();
  virtual unsigned record_slot_id();
private:
  virtual bool is_record_ok();

  RecordsIterator * data_provider_;
  RecordComparer * comparer_;
  int p_type_;
  void * record_for_cmp_;
};
