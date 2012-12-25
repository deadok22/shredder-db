#pragma once

class RecordsIterator {
public:
  virtual bool next() = 0;
  virtual void * operator*() = 0;
  virtual ~RecordsIterator() {}

  virtual unsigned record_page_id() = 0;
  virtual unsigned record_slot_id() = 0;
};