PREFIX=/reg/neh/home/davidsch/.conda/envs/lc2
CC=g++
CFLAGS=--std=c++11 -c -Wall -Iinclude -I$(PREFIX)/include -fPIC

LDFLAGS=-L$(PREFIX)/lib -Llib -Wl,--enable-new-dtags -Wl,-rpath='$$ORIGIN:$$ORIGIN/../lib:$(PREFIX)/lib' -lmpi -lmpi_cxx -lhdf5 -lhdf5_hl -lhdf5_cpp -lsz -lopen-rte -lopen-pal

.PHONY: all clean

all: lib/libana_daq_util.so bin/daq_writer bin/daq_master bin/ana_reader_master bin/ana_reader_stream bin/ana_daq_driver

#### DRIVER
bin/ana_daq_driver:
	ln -s ../python/ana_daq_driver.py bin/ana_daq_driver
	chmod a+x bin/ana_daq_driver

#### LIB
lib/libana_daq_util.so: build/ana_daq_util.o
	$(CC) -shared $(LDFLAGS) $< -o $@

build/ana_daq_util.o: src/ana_daq_util.cpp include/ana_daq_util.h
	$(CC) $(CFLAGS) $< -o $@

include/ana_daq_util.h:

#### DAQ STREAM WRITER
bin/daq_writer: build/daq_writer.o lib/libana_daq_util.so
	$(CC) $(LDFLAGS) -lana_daq_util $< -o $@

build/daq_writer.o: src/daq_writer.cpp
	$(CC) $(CFLAGS) $< -o $@

#### DAQ MASTER
bin/daq_master: build/daq_master.o lib/libana_daq_util.so
	$(CC) $(LDFLAGS) -lana_daq_util $< -o $@

build/daq_master.o: src/daq_master.cpp
	$(CC) $(CFLAGS)  $< -o $@

#### ANA READ MASTER
bin/ana_reader_master: build/ana_reader_master.o lib/libana_daq_util.so
	$(CC) $(LDFLAGS) -lana_daq_util $< -o $@

build/ana_reader_master.o: src/ana_reader_master.cpp
	$(CC) $(CFLAGS) $< -o $@

#### ANA READ STREAM
bin/ana_reader_stream: build/ana_reader_stream.o lib/libana_daq_util.so
	$(CC) $(LDFLAGS) -lana_daq_util $< -o $@

build/ana_reader_stream.o: src/ana_reader_stream.cpp
	$(CC) $(CFLAGS) $< -o $@

######### test
test: 
	bin/daq_writer
	bin/daq_master
	bin/ana_reader_master
	bin/ana_reader_stream
	bin/ana_daq_driver

#### clean
clean:
	rm lib/libana_daq_util.so build/*.o bin/daq_writer bin/daq_master bin/ana_reader_master bin/ana_reader_stream bin/ana_daq_driver

