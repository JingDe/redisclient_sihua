
TEST_REDIS_CLIENT_SRC=$(wildcard ../redis_client/*.cpp)
TEST_REDIS_CLIENT_OBJ=$(patsubst %.cpp, %.o, $(TEST_REDIS_CLIENT_SRC))

BASE_SRC=$(wildcard ../base_library/*.cpp)
BASE_OBJ=$(patsubst %.cpp, %.o, $(BASE_SRC))

TEST_REDIS_CLIENT_TARGET=test_redisclient

CC=g++
#CFLAGS=-g -Wall
CXXFLAGS=-g -Wall -std=c++0x

INCLUDE=-I../redis_client/ -I../ -I../third_party/include/
LIB=../third_party/lib/

LD_PATH=$(addprefix -L, $(LIB))
LD_LIB=-lglog -lpthread -lrt
LDFLAGS=$(LD_PATH) $(LD_LIB)


.PHONY: all
all: $(TEST_REDIS_CLIENT_TARGET)

$(TEST_REDIS_CLIENT_TARGET): $(TEST_REDIS_CLIENT_OBJ) $(BASE_OBJ)
	$(CC) $(CXXFLAGS) -o $(TEST_REDIS_CLIENT_TARGET) $(TEST_REDIS_CLIENT_OBJ) $(BASE_OBJ) $(INCLUDE) $(LDFLAGS)


# 隐含规则 ??

%.o: %.cpp
	$(CC) $(CXXFLAGS) -c $< -o $@ $(INCLUDE)

.PHONY: clean
clean:
	-rm $(TEST_REDIS_CLIENT_SRC)
	-rm $(TEST_REDIS_CLIENT_OBJ)


