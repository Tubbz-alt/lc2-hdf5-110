# lc2-hdf5-110
Investigate hdf5 1.10 features like SWMR and virtual dataset for LCLS II

## Setup
I made a conda environment with hdf5 1.10, h5py 2.7, and python 3.5

The makefile sets PREFIX to the root of this conda environment.

make will compile the C++ code, lib, and make a soft link to the pyhon driver 

## Running
To run, first do
```
bin/ana_daq_driver -h
```
Some options we can set from the command line, but most are controlled through the yaml config file: https://github.com/slaclab/lc2-hdf5-110/blob/master/config.yaml

You run ana_daq_driver and it
* creates root_dir/run_dir
* use --force to overwrite, it always neesds a new directory
* creates root_dir/run_dir/hdf5 - sets lfs striping on it per config file
* creates root_dir/run_dir/logs - and a /results /pids subdirs. 
* bin/daq_writer - many of these will run from different hosts. They will write into the /hdf5 in SWMR mode (effectively MWMR since we will have many running in parallel, creating separate h5 files, one for each daq_writer)
* bin/daq_master - one of these should run, it will read all the daq_writer files and make a master file with the virtual dataset
* bin/ana_reader_master - many of these will run, they will all read the master file to work with the virtual view
* bin/ana_reader_stream - we can run these too, they will read the daq_writer streams directly
* all the C++ programs will write their pid's in the pids dir. To clean up, the driver has a --kill command.

## daq_writer
The schema will be
```
/small/00000/data
/small/00000/fiducials
/small/00000/nano

/small/00001/data
...
/vlen/00000/blob
/vlen/00000/blobstart
/vlen/00000/blobcount
/vlen/00000/fiducials
/vlen/00000/nano
...
/detector/00000/data
...
```
We will write many small groups, and each will have three datases.
fiducials will just be a counter, and nano will track nanoseconds since program start.
nano is just for profiling, not merging, fiducials is for merging.  

