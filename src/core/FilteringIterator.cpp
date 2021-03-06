#include "FilteringIterator.h"

FilteringIterator::FilteringIterator(
  RecordsIterator * data_provider, RecordComparer *comparer, int p_type, void * record_for_cmp, long long max_miss_cnt):
  data_provider_(data_provider), comparer_(comparer), p_type_(p_type),
  miss_cnt_(0), max_miss_cnt_(max_miss_cnt), record_for_cmp_(record_for_cmp) {}
  
FilteringIterator::~FilteringIterator() {
  delete data_provider_;
  delete comparer_;
  delete [] (char *)record_for_cmp_;
}

bool FilteringIterator::next() {
  if (max_miss_cnt_ != -1 && max_miss_cnt_ < miss_cnt_) {
    return false;
  }

  while (data_provider_->next()) {
    if (is_record_ok()) { return true; }
    else { ++miss_cnt_; }
  }

  return false;
}

void * FilteringIterator::operator*() { return **data_provider_; }
unsigned FilteringIterator::record_page_id() { return data_provider_->record_page_id(); }
unsigned FilteringIterator::record_slot_id() { return data_provider_->record_page_id(); }

bool FilteringIterator::is_record_ok() {
  switch (p_type_) {
    case WhereClause::EQ: return comparer_->compare(**data_provider_, record_for_cmp_) == 0;
    case WhereClause::NEQ: return comparer_->compare(**data_provider_, record_for_cmp_) != 0;
    case WhereClause::LT: return comparer_->compare(**data_provider_, record_for_cmp_) > 0;
    case WhereClause::GT: return comparer_->compare(**data_provider_, record_for_cmp_) < 0;
    case WhereClause::LTOE: return comparer_->compare(**data_provider_, record_for_cmp_) >= 0;
    case WhereClause::GTOE: return comparer_->compare(**data_provider_, record_for_cmp_) <= 0;
  }
  return false;
}
