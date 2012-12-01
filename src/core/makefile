run:
	protoc --cpp_out=. ./TableMetadata.proto
	g++ MetaDataProvider.cpp ../common/Utils.cpp ../common/InfoPool.cpp TableMetadata.pb.cc -lprotobuf -DTEST_MDP -o test_mp
	./test_mp