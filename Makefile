
D_KEY = -DDEBUG

all: build
	g++ build/src/core/DBFacade.cpp build/MetaDataProvider.o build/common/Utils.o build/common/InfoPool.o build/gen/core/TableMetadata.o -lprotobuf -DTEST_DBF -DDEBUG -o test_dbf

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
	-mkdir -p build/gen/core
	g++ build/src/core/TableMetadata.pb.cc -c -o build/TableMetadata.o

build_common: init
	-mkdir build/common
	g++ src/common/Utils.cpp $(D_KEY) -c -o build/Utils.o
	g++ src/common/InfoPool.cpp -c -o build/InfoPool.o

build_sqlparser: init
	g++ src/sqlparser/SqlParser.cpp -c -o build/SqlParser.o	

build_core: init build_common build_proto
	-mkdir build/core
	g++ src/core/MetaDataProvider.cpp -Ibuild/src/core -c -o build/MetaDataProvider.o
	g++ src/core/DBFacade.cpp -Ibuild/src/core -c -o build/DBFacade.o

build: build_core build_common build_proto build_sqlparser
	g++ src/main.cpp build/*.o -std=c++0x -Ibuild/src/core -lprotobuf -lboost_regex -o shredder_db