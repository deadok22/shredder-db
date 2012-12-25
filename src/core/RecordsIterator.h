#pragma once

class RecordsIterator {
public:
  virtual bool next() = 0;
  virtual void * operator*() = 0;
  virtual ~RecordsIterator() {}

  virtual unsigned record_page_id() = 0;
  virtual unsigned record_slot_id() = 0;
};

class EmptyIterator : public RecordsIterator {
public:
  bool next() { return false; }
  void * operator*() { return NULL; }
  ~EmptyIterator() {}
  unsigned record_page_id() { return 0; }
  unsigned record_slot_id() { return 0; }
};