#include "CsvPrinter.h"



CsvPrinter::CsvPrinter() : DELIM(',') {}

CsvPrinter::~CsvPrinter() {}

CsvPrinter & CsvPrinter::get_instance() {
  static CsvPrinter csv_p;
  return csv_p;
}

string CsvPrinter::get_header_csv(TableMetaData const & table) { 
#ifdef CSVPRINTER_DBG  
  Utils::info("[CsvPrinter] get csv_header_string");
#endif    
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


string CsvPrinter::get_csv(void const * record_data, TableMetaData const & table) { 
#ifdef CSVPRINTER_DBG
  Utils::info("[CsvPrinter] get csv_string");  
#endif
  stringstream record;
  record.precision(1);

  int offset = 0;
  for (int attr_ind = 0, end = table.attribute_size(); attr_ind < end; ++attr_ind) {
    int attr_size = table.attribute(attr_ind).size();

    //FIXME do not copy record to your stack! why would you do that?

    char char_attr_value[attr_size + 1];
    memcpy(char_attr_value, (char *)record_data + offset, attr_size);
    switch ((TypeCode)table.attribute(attr_ind).type_name()) {
      case INT: {
          int value = *((int *)char_attr_value);
          record << value;
        }
        break;
      case DOUBLE: {
          double value = *((double *)char_attr_value);
          record << std::fixed << value;
        }
        break;
      case VARCHAR: {
          //FIXME zeroes are stored in db.just pass the pointer to stringstream's << operator and be happy.
          char_attr_value[attr_size] = '\0';
          record << "\"";
          for( int i = 0; i != attr_size; ++i) {
            record << char_attr_value[i];           
            if( char_attr_value[i] == '"') {
              record << '"';
            } else if( char_attr_value[i] == '\0') {
              break;
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
