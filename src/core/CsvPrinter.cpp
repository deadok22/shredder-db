#include "CsvPrinter.h"



CsvPrinter::CsvPrinter() : DELIM(',') {}

CsvPrinter::~CsvPrinter() {}

CsvPrinter & CsvPrinter::get_instance() {
  static CsvPrinter csv_p;
  return csv_p;
}

string CsvPrinter::get_header_csv( string const & table_name ) { 
  TableMetaData &table = *(MetaDataProvider::get_instance()->get_meta_data(table_name));
  
  stringstream record;
  for (int attr_ind = 0, end = table.attribute_size(); attr_ind != end; ++attr_ind) {
    record << table.attribute(attr_ind).name();
    switch ((TypeCode)table.attribute(attr_ind).type_name()) {
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
    if( attr_ind + 1 != end){
      record << DELIM;
    }

  }
  record << std::endl;
  return record.str();
}

string CsvPrinter::get_csv( RecordsIterator const& rec_iter, string const & table_name ) { 
  TableMetaData &table = *(MetaDataProvider::get_instance()->get_meta_data(table_name));

  stringstream record;
  int offset = 0;
  for (int attr_ind = 0, end = table.attribute_size(); attr_ind < end; ++attr_ind) {
    int attr_size = table.attribute(attr_ind).size();

    char char_attr_value[attr_size + 1];
    memcpy(char_attr_value, (char*)(*rec_iter) + offset, attr_size);
    switch ((TypeCode)table.attribute(attr_ind).type_name()) {
      case INT: {
          int value = *((int *)char_attr_value);
          record << value;
        }
        break;
      case DOUBLE: {
          double value = *((double *)char_attr_value);
          record.precision(1);
          record << std::fixed << value;
        }
        break;
      case VARCHAR: {
          char_attr_value[attr_size] = '\0';
          record << "\"";
          for( int i = 0; i != attr_size; ++i) {
            record << char_attr_value[i];           
            if( char_attr_value[i] == '"') {
              record << '"';
            }
          }
          record << "\"";
        }
        break;
    }
    if( attr_ind + 1 != end){
      record << DELIM;
    }
    offset += attr_size;
  }
  record << std::endl;
  return record.str();
}
