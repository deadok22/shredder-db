#pragma once

#include <string>
#include <tuple>
#include "../common/DataType.h"
#include "TableMetadata.pb.h"

class RecordComparer {
public:
  struct ComparisonRule {
      ComparisonRule(std::string const & col_name, bool is_desc = false)
          : column_name(col_name),
            is_descending(is_desc) {}
      std::string column_name;
      bool is_descending;
  };
public:
  RecordComparer(TableMetaData const & tmd, std::vector<RecordComparer::ComparisonRule> const & comparison_rules);
  // this constructor constructs an IndexRecordComparer
  RecordComparer(TableMetaData const & tmd, std::string const & index_name);
  int compare(void const * data1, void const * data2) const;
private:
  class RecordComparisonRule {
  public:
    RecordComparisonRule(size_t offset, DataType type, bool is_desc);
    RecordComparisonRule(TableMetaData const & tmd, RecordComparer::ComparisonRule const & cr);
    int apply(void const * a, void const * b) const;
  private:
    int apply(int const * a, int const * b) const;
    int apply(double const * a, double const * b) const;
    int apply(char const * a, char const * b) const;
  private:
    size_t offset_bytes_;
    DataType data_type_;
    bool is_descending_;
  };
private:
  void init_index_record_comparer(TableMetaData const & tmd, TableMetaData_IndexMetadata const & imd);
private:
  std::vector<RecordComparisonRule> cmp_rules_;
};
