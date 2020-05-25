
hiredis_demo_target:=demohiredis

hiredis_demo_header:=$(wildcard *.h)
hiredis_demo_src:=$(wildcard *.cpp *.cc *.c)
hiredis_demo_obj:=$(patsubst %.cpp, %.o, $(hiredis_demo_src))
hiredis_demo_obj:=$(patsubst %.c, %.o, $(hiredis_demo_obj))
hiredis_demo_obj:=$(patsubst %.cc, %.o, $(hiredis_demo_obj))


CC=g++
CXXFLAGS=-g -Wall -std=c++0x

INCLUDE=-I .


.PHONY: all
all: $(hiredis_demo_target)

$(hiredis_demo_target): $(hiredis_demo_obj) 
	$(CC) -o $@ $^

%.o:%.c
	$(CC) -c $< -o $@ $(CXXFLAGS)

.PHONY: clean
	-rm $(hiredis_demo_obj)
	-rm $(hiredis_demo_target)

