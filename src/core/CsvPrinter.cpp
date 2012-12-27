#include "CsvPrinter.h"



CsvPrinter::CsvPrinter() : DELIM(',') {}

CsvPrinter::~CsvPrinter() {}

CsvPrinter & CsvPrinter::get_instance() {
  static CsvPrinter csv_p;
  return csv_p;
}

typedef google::protobuf::RepeatedPtrField<TableMetaData_AttributeDescription>::const_iterator attr_const_iter;

std::vector<std::string> CsvPrinter::get_column_names(TableMetaData const & table) {
  std::vector<std::string> column_names;
  for (attr_const_iter i = table.attribute().begin(); i != table.attribute().end(); ++i) {
    column_names.push_back(i->name());
  }
  return column_names;
}

string CsvPrinter::get_header_csv(TableMetaData const & table, std::vector<std::string> const & cols) { 
  std::vector<std::string> columns;
  if (0 == cols.size()) {
    columns = get_column_names(table);
  } else {
    columns = cols;
  }
#ifdef CSVPRINTER_DBG  
  Utils::info("[CsvPrinter] get csv_header_string");
#endif    
  stringstream record;
  for (std::vector<std::string>::const_iterator i = columns.begin(); i != columns.end(); ++i) {
    for (attr_const_iter j = table.attribute().begin(); j != table.attribute().end(); ++j) {
      if (j->name() == *i) {
        record << *i;
        switch ((TypeCode)j->type_name()) {
          case INT: {
            record << "(int)";
          }
          break;
          case DOUBLE: {
            record << "(double)";
          }
          break;
          case VARCHAR: {
            record << "(varchar)";
          }
          break;
        }
        if (i + 1 != columns.end()) {
          record << DELIM;
        }
        break;
      }
    }
  }
  record << std::endl;
  return record.str();
}

string CsvPrinter::get_csv(void const * record_data, TableMetaData const & table, std::vector<std::string> const & cols) { 
  std::vector<std::string> columns;
  if (0 == cols.size()) {
    columns = get_column_names(table);
  } else {
    columns = cols;
  }
#ifdef CSVPRINTER_DBG
  Utils::info("[CsvPrinter] get csv_string");  
#endif
  stringstream record;
  record.precision(1);

  for (std::vector<std::string>::const_iterator i = columns.begin(); i != columns.end(); ++i) {
    size_t offset = 0;
    for (attr_const_iter j = table.attribute().begin(); j != table.attribute().end(); ++j) {
      if (j->name() == *i) {
        char * attr_value = (char *)record_data + offset;
        switch ((TypeCode)j->type_name()) {
          case INT: {
            int value = *((int *)attr_value);
            record << value;
          }
          break;
          case DOUBLE: {
            double value = *((double *)attr_value);
            record << std::fixed << value;
          }
          break;
          case VARCHAR: {
            record << '"';
            record << attr_value;
            record << '"';
          }
          break;
        }
        if (i + 1 != columns.end()) {
          record << DELIM;
        }
        break;
      } else {
        offset += j->size();
      }
    }
  }
  record << std::endl;
  return record.str();
}
