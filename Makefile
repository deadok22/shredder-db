# IO_BUFF_M -- buffer manager
# MAIN_DBG is for mostly for repl 
# DBFACADE_DBG for DbFacade logic
# SQLPARSE_DBG - for sql parsing subsystem
# EIM_DBG - is for Extendable index implementation
# PAGE_D_DBG -- is for page directory for heap file
# HFM_DBG -- heap file manager debug mode
# BTREE_DBG -- btree index manager debug mode
# BTREE_SI_DBG -- btree sorted iterator
# QPLAN -- query planner debug mode

#D_KEY = -DDEBUG -DMAIN_DBG -DDBFACADE_DBG -DSQLPARSE_DBG -DEIM_DBG -DPAGE_D_DBG -DHFM_DBG -DBTREE_SI_DBG
D_KEY = -DDEBUG #-DIO_DISK_M -DQPLAN
COPTS = -std=c++0x -D_GLIBCXX_FULLY_DYNAMIC_STRING -Wall

all: build

clean:
	rm -rf build

init:
	-mkdir build

gen_proto: init
	mkdir -p build/src/core
	cp src/core/TableMetadata.proto .
	protoc --cpp_out=build/src/core ./TableMetadata.proto
	rm ./TableMetadata.proto

build_proto: gen_proto
	g++ build/src/core/TableMetadata.pb.cc -c -o build/TableMetadata.o

build_common: init
	g++ src/common/Utils.cpp $(COPTS) $(D_KEY) -c -o build/Utils.o
	g++ src/common/InfoPool.cpp $(COPTS) -c -o build/InfoPool.o

build_sqlparser: init
	g++ src/sqlparser/SqlParser.cpp $(COPTS) $(D_KEY) -c -o build/SqlParser.o	

build_core: init build_common build_proto
	g++ src/core/MetaDataProvider.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/MetaDataProvider.o
	g++ src/core/FilteringIterator.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/FilteringIterator.o
	g++ src/core/DBFacade.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/DBFacade.o
	g++ src/core/QueryPlanner.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/QueryPlanner.o
	g++ src/core/RecordComparer.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/RecordComparer.o
	g++ src/core/indices/ExtIndexManager.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/ExtIndexManager.o
	g++ src/core/indices/BTreeIndexManager.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/BTreeIndexManager.o
	g++ src/core/CsvPrinter.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/CsvPrinter.o

build_backend: init build_common
	g++ src/backend/DiskManager.cpp $(COPTS) -c -o build/DiskManager.o
	g++ src/backend/BufferManager.cpp $(COPTS) $(D_KEY) -c -o build/BufferManager.o
	g++ src/backend/Page.cpp $(COPTS) -c -o build/Page.o
	g++ src/backend/PagesDirectory.cpp $(COPTS) $(D_KEY) -c -o build/PagesDirectory.o
	g++ src/backend/HeapFileManager.cpp $(COPTS) $(D_KEY) -Ibuild/src/core -c -o build/HeapFileManager.o

build: build_core build_common build_proto build_sqlparser build_backend
	g++ src/main.cpp build/*.o $(COPTS) $(D_KEY) -Ibuild/src/core -lprotobuf -lboost_regex -g3 -o shredder_db
