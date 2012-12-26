#pragma once
#include "TableMetadata.pb.h"
#include "MetaDataProvider.h"
#include "../common/DataType.h"
#include <string>
#include <sstream>
#include <iostream>

using std::stringstream;
using std::string;

class CsvPrinter {
public:
  static CsvPrinter & get_instance();
  string get_csv(void const * record_data, string const & table_name );
  string get_header_csv( string const & table_name );

private:
  const char DELIM; 
  CsvPrinter();
  ~CsvPrinter();
};

#ifdef TEST_CSV_P

int main()
{


  return 0;
}

#endif
