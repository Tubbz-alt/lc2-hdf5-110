PREFIX=/reg/neh/home/davidsch/.conda/envs/lc2
CC=g++
CFLAGS=--std=c++11 -c -Wall -Iinclude -I$(PREFIX)/include -fPIC

LDFLAGS=-L$(PREFIX)/lib -Llib -Wl,--enable-new-dtags -Wl,-rpath='$$ORIGIN:$$ORIGIN/../lib:$(PREFIX)/lib' -lmpi -lmpi_cxx -lhdf5 -lhdf5_hl -lhdf5_cpp -lsz -lopen-rte -lopen-pal

.PHONY: all clean

all: lib/liblc2daq.so bin/daq_writer bin/daq_master bin/ana_reader_master bin/ana_reader_stream bin/ana_daq_driver

h5vds: examples/h5_vds.c
	h5c++ examples/h5_vds.c -o h5_vds
	./h5_vds
	ls -l vds.h5
	h5dump -p vds.h5

h5vdsmod: modified_examples/h5_vds_srcs.c modified_examples/h5_vds_master.cpp
	h5c++ modified_examples/h5_vds_srcs.c -o h5_vds_srcs
	h5c++ modified_examples/h5_vds_master.cpp -o h5_vds_master
	./h5_vds_srcs
	./h5_vds_master  0 1000 600 600 1 1024 1024 100 0 daq_writer 3  0 34 67 0 4 7 0 0 0 34 33 33 4 3 3 1 1 1 0 0 0 0 0 0 0 100 200 1 1 1 1 1 1 300 300 300
	ls -l /reg/d/ana01/temp/davidsch/lc2/runA/vds.h5
	h5ls -r /reg/d/ana01/temp/davidsch/lc2/runA/a.h5
	h5ls -r /reg/d/ana01/temp/davidsch/lc2/runA/b.h5
	h5ls -r /reg/d/ana01/temp/davidsch/lc2/runA/c.h5
	h5dump -p /reg/d/ana01/temp/davidsch/lc2/runA/vds.h5

#### DRIVER
bin/ana_daq_driver:
	ln -s ../python/ana_daq_driver.py bin/ana_daq_driver
	chmod a+x bin/ana_daq_driver

#### LIB
lib/liblc2daq.so: build/ana_daq_util.o build/daq_base.o build/H5OpenObjects.o include/lc2daq.h
	$(CC) -shared $(LDFLAGS) build/ana_daq_util.o build/daq_base.o build/H5OpenObjects.o -o $@

build/ana_daq_util.o: src/ana_daq_util.cpp include/ana_daq_util.h
	$(CC) $(CFLAGS) src/ana_daq_util.cpp -o build/ana_daq_util.o

build/H5OpenObjects.o: src/H5OpenObjects.cpp include/H5OpenObjects.h
	$(CC) $(CFLAGS) src/H5OpenObjects.cpp -o build/H5OpenObjects.o

build/daq_base.o: src/daq_base.cpp include/daq_base.h
	$(CC) $(CFLAGS) src/daq_base.cpp -o build/daq_base.o

include/ana_daq_util.h:

include/daq_base.h:

include/lc2daq.h:

#### DAQ WRITER RAW/STREAM
bin/daq_writer: build/daq_writer.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq $< -o $@

build/daq_writer.o: src/daq_writer.cpp 
	$(CC) $(CFLAGS) $< -o $@

#### DAQ MASTER
bin/daq_master: build/daq_master.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq $< -o $@

build/daq_master.o: src/daq_master.cpp
	$(CC) $(CFLAGS)  $< -o $@

#### ANA READ MASTER
bin/ana_reader_master: build/ana_reader_master.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq $< -o $@

build/ana_reader_master.o: src/ana_reader_master.cpp
	$(CC) $(CFLAGS) $< -o $@

#### ANA READ STREAM
bin/ana_reader_stream: build/ana_reader_stream.o lib/liblc2daq.so
	$(CC) $(LDFLAGS) -llc2daq $< -o $@

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
	rm lib/liblc2daq.so build/*.o bin/daq_writer bin/daq_master bin/ana_reader_master bin/ana_reader_stream bin/ana_daq_driver

