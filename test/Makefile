
test_redisclient_demo=test_redisclient.cpp

test_redisclient_header:=$(wildcard ../redis_client/*.h)
test_redisclient_src:=$(wildcard ../redis_client/*.cpp) $(test_redisclient_demo)
test_redisclient_obj:=$(patsubst %.cpp, %.o, $(test_redisclient_src))

base_src:=$(wildcard ../base_library/*.cpp)
base_obj:=$(patsubst %.cpp, %.o, $(base_src))

test_redisclient_target=test_redisclient

CC=g++
cxxflags=-g -Wall -std=c++0x

include=-I../redis_client/ -I../third_party/include/ -I../base_library/ -I../
lib=../third_party/lib/

ld_path=$(addprefix -L, $(lib))
ld_lib=-lglog -lpthread -lrt
ldflags=$(ld_path) $(ld_lib)


.PHONY: all
all: $(test_redisclient_target)

$(test_redisclient_target): $(test_redisclient_obj) $(base_obj)
	$(CC) -o $@ $^ $(ldflags)

%.o: %.cpp
	$(CC) -c $< -o $@ $(cxxflags) $(include)


%.o: %.cpp %.h
	$(CC) -c $< -o $@ $(cxxflags) $(include)

.PHONY: debug
debug:
	echo $(test_redisclient_src)
	echo $(test_redisclient_obj)

.PHONY: clean
clean:
	-rm $(base_obj)
	-rm $(test_redisclient_obj)
	-rm $(test_redisclient_target)

