
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
	g++ build/src/core/TableMetadata.pb.cc -c -o build/gen/core/TableMetadata.o

build_common: init
	-mkdir build/common
	g++ src/common/Utils.cpp -c -o build/common/Utils.o
	g++ src/common/InfoPool.cpp -c -o build/common/InfoPool.o

build_core: init build_common build_proto
	-mkdir build/core
	g++ src/core/MetaDataProvider.cpp -Ibuild/src/core -c -o build/core/MetaDataProvider.o
	g++ src/core/DBFacade.cpp -Ibuild/src/core -c -o build/core/DBFacade.o

build: build_core build_common build_proto

build_db_t: build
	g++ src/core/DBFacade.cpp build/core/MetaDataProvider.o build/common/Utils.o build/common/InfoPool.o build/gen/core/TableMetadata.o -Ibuild/src/core -lprotobuf -DTEST_DBF -DDEBUG -o test_dbf