#pragma once
#include "TableMetadata.pb.h"
#include "MetaDataProvider.h"
#include "../common/DataType.h"
#include "../common/Utils.h"
#include <string>
#include <sstream>
#include <iostream>

using std::stringstream;
using std::string;

class CsvPrinter {
public:
  static CsvPrinter & get_instance();
  string get_csv(void const * record_data, TableMetaData const & table);
  string get_header_csv(TableMetaData const & table);

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
