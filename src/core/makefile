run:
	protoc --cpp_out=. ./TableMetadata.proto
	g++ DBFacade.cpp MetaDataProvider.cpp ../common/Utils.cpp ../common/InfoPool.cpp TableMetadata.pb.cc -lprotobuf -DTEST_DBF -DDEBUG -o test_dbf
	./test_dbf