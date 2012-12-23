#include "Filter.h"

const Filter Filter::ANY;

bool Filter::isOk(TableMetaData const & table, void * data) const {
  return true;
}