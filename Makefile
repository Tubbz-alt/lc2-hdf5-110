PREFIX=/reg/neh/home/davidsch/.conda/envs/lc2
CC=g++
CFLAGS=--std=c++11 -c -Wall -Iinclude -I$(PREFIX)/include -fPIC

LDFLAGS=-L$(PREFIX)/lib -Llib -Wl,--enable-new-dtags -Wl,-rpath='$$ORIGIN:$$ORIGIN/../lib:$(PREFIX)/lib' -lmpi -lmpi_cxx -lhdf5 -lhdf5_hl -lhdf5_cpp -lsz -lopen-rte -lopen-pal

.PHONY: all clean

all: lib/liblc2daq.so bin/daq_writer bin/daq_master bin/ana_reader_master bin/ana_reader_stream bin/ana_daq_driver

#### DRIVER
bin/ana_daq_driver:
	ln -s ../python/ana_daq_driver.py bin/ana_daq_driver
	chmod a+x bin/ana_daq_driver

#### LIB
lib/liblc2daq.so: build/ana_daq_util.o build/daq_base.o build/H5OpenObjects.o build/VDSRoundRobin.o build/Dset.o build/DsetCreation.o include/lc2daq.h 
	$(CC) -shared $(LDFLAGS) build/ana_daq_util.o build/daq_base.o build/H5OpenObjects.o  build/VDSRoundRobin.o build/Dset.o -o $@

build/ana_daq_util.o: src/ana_daq_util.cpp include/ana_daq_util.h
	$(CC) $(CFLAGS) src/ana_daq_util.cpp -o build/ana_daq_util.o

build/VDSRoundRobin.o: src/VDSRoundRobin.cpp include/VDSRoundRobin.h 
	$(CC) $(CFLAGS) src/VDSRoundRobin.cpp -o build/VDSRoundRobin.o

build/H5OpenObjects.o: src/H5OpenObjects.cpp include/H5OpenObjects.h
	$(CC) $(CFLAGS) src/H5OpenObjects.cpp -o build/H5OpenObjects.o

build/daq_base.o: src/daq_base.cpp include/daq_base.h 
	$(CC) $(CFLAGS) src/daq_base.cpp -o build/daq_base.o

build/Dset.o: src/Dset.cpp include/Dset.h include/check_macros.h
	$(CC) $(CFLAGS) src/Dset.cpp -o build/Dset.o

build/DsetCreation.o: src/DsetCreation.cpp include/DsetCreation.h include/check_macros.h
	$(CC) $(CFLAGS) src/DsetCreation.cpp -o build/DsetCreation.o

## header files
include/lc2daq.h: include/check_macros.h include/Dset.h include/ana_daq_util.h include/H5OpenObjects.h include/VDSRoundRobin.h

include/ana_daq_util.h: include/Dset.h

include/Dset.h:

include/check_macros.h:

include/daq_base.h: include/lc2daq.h


#### DAQ WRITER RAW/STREAM
bin/daq_writer: build/daq_writer.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq -lyaml-cpp $< -o $@

build/daq_writer.o: src/daq_writer.cpp 
	$(CC) $(CFLAGS) $< -o $@

#### DAQ MASTER
bin/daq_master: build/daq_master.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq  -lyaml-cpp $< -o $@

build/daq_master.o: src/daq_master.cpp
	$(CC) $(CFLAGS)  $< -o $@

#### ANA READ MASTER
bin/ana_reader_master: build/ana_reader_master.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq -lyaml-cpp $< -o $@

build/ana_reader_master.o: src/ana_reader_master.cpp
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

run_all: 
	bin/daq_writer
	bin/daq_master
	bin/ana_reader_master
	bin/ana_reader_stream
	bin/ana_daq_driver

#### clean
clean:
	rm lib/liblc2daq.so build/*.o bin/daq_writer bin/daq_master bin/ana_reader_master bin/ana_reader_stream bin/ana_daq_driver

