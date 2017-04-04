PREFIX=/reg/neh/home/davidsch/.conda/envs/lc2

CC=g++
SHARED=-shared

#CC=h5c++
#SHARED=-shlib

CFLAGS=--std=c++11 -c -Wall -Iinclude -I$(PREFIX)/include -fPIC
HDF5_LIBS=-lmpi -lmpi_cxx -lhdf5 -lhdf5_hl -lhdf5_cpp -lsz -lopen-rte -lopen-pal
#HDF5_LIBS=
XTRA_LIBS=-lyaml-cpp

LDFLAGS=-L$(PREFIX)/lib -Llib -Wl,--enable-new-dtags -Wl,-rpath='$$ORIGIN:$$ORIGIN/../lib:$(PREFIX)/lib' $(HDF5_LIBS) $(XTRA_LIBS)

.PHONY: all clean

APPS=bin/daq_writer bin/daq_master bin/ana_reader_master bin/ana_reader_stream bin/ana_daq_driver

TESTS=bin/test_Dset bin/test_vds_round_robin

LIBS=lib/liblc2daq.so

PYTHON_SCRIPTS=bin/ana_daq_driver

all: $(LIBS) $(APPS) $(TESTS) $(PYTHON_SCRIPTS)

#### PYTHON SCRIPTS
bin/ana_daq_driver:
	ln -s ../python/ana_daq_driver.py bin/ana_daq_driver
	chmod a+x bin/ana_daq_driver

#### LIBS
LIB_OBJS=build/DaqBase.o  build/Dset.o  build/DsetPropAccess.o  build/H5OpenObjects.o  build/VDSRoundRobin.o
LIB_USER_HEADERS=include/lc2daq.h

lib/liblc2daq.so: $(LIB_OBJS) $(LIB_USER_HEADERS)
	$(CC) $(SHARED) $(LDFLAGS) $(LIB_OBJS) -o $@

build/DaqBase.o: src/DaqBase.cpp include/DaqBase.h include/check_macros.h
	$(CC) $(CFLAGS) src/DaqBase.cpp -o build/DaqBase.o

build/Dset.o: src/Dset.cpp include/Dset.h include/check_macros.h include/DsetPropAccess.h
	$(CC) $(CFLAGS) src/Dset.cpp -o build/Dset.o

build/DsetPropAccess.o: src/DsetPropAccess.cpp include/DsetPropAccess.h include/check_macros.h
	$(CC) $(CFLAGS) src/DsetPropAccess.cpp -o build/DsetPropAccess.o

build/H5OpenObjects.o: src/H5OpenObjects.cpp include/H5OpenObjects.h
	$(CC) $(CFLAGS) src/H5OpenObjects.cpp -o build/H5OpenObjects.o

build/VDSRoundRobin.o: src/VDSRoundRobin.cpp include/VDSRoundRobin.h 
	$(CC) $(CFLAGS) src/VDSRoundRobin.cpp -o build/VDSRoundRobin.o


## header files
include/lc2daq.h: include/check_macros.h include/Dset.h include/DsetPropAccess.h include/H5OpenObjects.h include/VDSRoundRobin.h

include/DaqBase.h:

include/Dset.h:

include/DsetPropAccess.h:

include/H5OpenObjects.h:

include/VDSRoundRobin.h:

include/check_macros.h:


#### DAQ WRITER RAW/STREAM
bin/daq_writer: build/daq_writer.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq $< -o $@

build/daq_writer.o: src/daq_writer.cpp 
	$(CC) $(CFLAGS) $< -o $@

#### DAQ MASTER
bin/daq_master: build/daq_master.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq  -lyaml-cpp $< -o $@

build/daq_master.o: src/daq_master.cpp
	$(CC) $(CFLAGS)  $< -o $@

#### ANA READ MASTER
bin/ana_reader_master: build/ana_reader_master.o lib/liblc2daq.so include/DaqBase.h
	$(CC) $(LDFLAGS) -llc2daq  $< -o $@

build/ana_reader_master.o: app/ana_reader_master.cpp
	$(CC) $(CFLAGS) $< -o $@

#### ANA READ STREAM
bin/ana_reader_stream: build/ana_reader_stream.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq  -lyaml-cpp $< -o $@

build/ana_reader_stream.o: src/ana_reader_stream.cpp
	$(CC) $(CFLAGS) $< -o $@

#### EVENT BASED INSTEAD OF ARRAY BASED
bin/event_writer: build/event_writer.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq  -lyaml-cpp build/event_writer.o -o bin/event_writer

build/event_writer.o: src/event_writer.cpp
	$(CC) $(CFLAGS) $< -o $@

build/test_vds_round_robin.o: test/test_vds_round_robin.cpp
	$(CC) $(CFLAGS) $< -o $@

build/test_Dset.o: test/test_Dset.cpp
	$(CC) $(CFLAGS) $< -o $@

######### test/tests
bin/test_vds_round_robin: build/test_vds_round_robin.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq -lyaml-cpp $< -o $@

bin/test_Dset: build/test_Dset.o build/Dset.o
	$(CC) $(LDFLAGS) build/test_Dset.o build/Dset.o -o $@

# TODO: add back test_vds_round_robin
test: bin/test_Dset 
	bin/test_Dset


#### clean
clean:
	rm lib/*.so build/*.o bin/*

