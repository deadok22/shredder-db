
D_KEY = -DDEBUG 
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
	g++ src/sqlparser/SqlParser.cpp $(COPTS) -c -o build/SqlParser.o	

build_core: init build_common build_proto
	g++ src/core/MetaDataProvider.cpp $(COPTS) -Ibuild/src/core -c -o build/MetaDataProvider.o
	g++ src/core/DBFacade.cpp $(COPTS) -Ibuild/src/core -c -o build/DBFacade.o

build_backend: init build_common
	g++ src/backend/DiskManager.cpp $(COPTS) -c -o build/DiskManager.o
	g++ src/backend/BufferManager.cpp $(COPTS) -c -o build/BufferManager.o
	g++ src/backend/Page.cpp $(COPTS) -c -o build/Page.o

build: build_core build_common build_proto build_sqlparser build_backend
	g++ src/main.cpp build/*.o $(COPTS) -Ibuild/src/core -lprotobuf -lboost_regex -g3 -o shredder_db
