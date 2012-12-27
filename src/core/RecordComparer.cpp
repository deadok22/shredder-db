#include "RecordComparer.h"
#include "../common/Utils.h"
#include <cstring>

typedef google::protobuf::RepeatedPtrField<TableMetaData_AttributeDescription>::const_iterator attr_const_iter;

RecordComparer::RecordComparisonRule::RecordComparisonRule(size_t offset, DataType type, bool is_desc)
      : offset_bytes_(offset),
        data_type_(type),
        is_descending_(is_desc) {}

RecordComparer::RecordComparisonRule::RecordComparisonRule(TableMetaData const & tmd,
          RecordComparer::ComparisonRule const & cr)
            : offset_bytes_(0),
              data_type_(DataType::get_int()),
              is_descending_(cr.is_descending) {
  bool column_found = false;
  for (attr_const_iter i = tmd.attribute().begin(); i != tmd.attribute().end(); ++i) {
    if (i->name() == cr.column_name) {
      column_found = true;
      data_type_ = DataType((TypeCode)i->type_name(), i->size());
      break;
    } else {
      offset_bytes_ += i->size();
    }
  }
  if (!column_found) {
    Utils::error("[RecordComparer] [RecordComparisonRule] no column named " + cr.column_name + " found in table schema.");
    Utils::critical_error();
  }
}

int RecordComparer::RecordComparisonRule::apply(int const * a, int const * b) const {
  if (*a == *b) {
    return 0;
  }
  return *a < *b && !is_descending_ ? 1 : -1;
}

int RecordComparer::RecordComparisonRule::apply(double const * a, double const * b) const {
  if (*a == *b) {
    return 0;
  }
  return *a < *b && !is_descending_ ? 1 : -1;
}

int RecordComparer::RecordComparisonRule::apply(char const * a, char const * b) const {
  int result = strncmp(a, b, data_type_.get_size() - 1);
  if (0 == result) {
    return 0;
  }
  return result < 0 && !is_descending_ ? 1 : -1;
}

int RecordComparer::RecordComparisonRule::apply(void const * a, void const * b) const {
  void const * a_plus_offset = (void const *) (offset_bytes_ + (char const *) a);
  void const * b_plus_offset = (void const *) (offset_bytes_ + (char const *) b);
  switch (data_type_.get_type_code()) {
    case INT : {
      return apply((int const *) a_plus_offset, (int const *) b_plus_offset);
    }
    case DOUBLE : {
      return apply((double const *) a_plus_offset, (double const *) b_plus_offset);
    }
    case VARCHAR : {
      return apply((char const *) a_plus_offset, (char const *) b_plus_offset);
    }
    default : {
      Utils::error("[RecordComparer] [RecordComparisonRule] unexpected enum value");
      Utils::critical_error();
    }
  }
  return 0;
}

typedef std::vector<RecordComparer::ComparisonRule>::const_iterator rc_const_iterator;

RecordComparer::RecordComparer(TableMetaData const & tmd, 
          std::vector<RecordComparer::ComparisonRule> const & comparison_rules) {
  if (0 == comparison_rules.size()) {
    Utils::error("[RecordComparer] no comparison rules specified.");
    Utils::critical_error();
  }
  for (rc_const_iterator i = comparison_rules.begin(); i != comparison_rules.end(); ++i) {
    cmp_rules_.push_back(RecordComparer::RecordComparisonRule(tmd, *i));
  }
}

typedef google::protobuf::RepeatedPtrField<TableMetaData_IndexMetadata_KeyInfo>::const_iterator idx_key_const_iter;

void RecordComparer::init_index_record_comparer(TableMetaData const & tmd, TableMetaData_IndexMetadata const & imd) {
  for (idx_key_const_iter idx_key = imd.keys().begin(); idx_key != imd.keys().end(); ++idx_key) {
    size_t offset_bytes = 0;
    for (attr_const_iter attr = tmd.attribute().begin(); attr != tmd.attribute().end(); ++attr) {
      if (attr->name() == idx_key->name()) {
        cmp_rules_.push_back(RecordComparer::RecordComparisonRule(offset_bytes, DataType((TypeCode)attr->type_name(), attr->size()), !idx_key->asc()));
        break;
      } else {
        offset_bytes += attr->size();
      }
    }
  }
}

typedef google::protobuf::RepeatedPtrField<TableMetaData_IndexMetadata>::const_iterator idx_const_iter;

RecordComparer::RecordComparer(TableMetaData const & tmd, std::string const & index_name) {
  bool index_found = false;
  for (idx_const_iter i = tmd.indices().begin(); i != tmd.indices().end(); ++i) {
    if (index_name == i->name()) {
      init_index_record_comparer(tmd, *i);
      index_found = true;
      break;
    }
  }
  if (!index_found) {
    Utils::error("[RecordComparer] no index named " + index_name + " found in schema");
    Utils::critical_error();
  }
}

int RecordComparer::compare(void const * data1, void const * data2) const {
  for (std::vector<RecordComparer::RecordComparisonRule>::const_iterator i = cmp_rules_.begin();
              i != cmp_rules_.end(); ++i) {
    int cmp_result = i->apply(data1, data2);
    if (0 != cmp_result) {
      return cmp_result;
    }
  }
  return 0;
}
