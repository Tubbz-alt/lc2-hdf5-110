cmake_minimum_required(VERSION 3.6)
project(lc2-hdf5-110)

set(CMAKE_CXX_STANDARD 11)
#set(HDF5_ROOT /Users/david/Code/hdf5-1.10.0-patch1/hdf5)
set(HDF5_ROOT /reg/neh/home/davidsch/.conda/envs/lc2)
find_package(HDF5 REQUIRED)
include_directories(${HDF5_INCLUDE_DIR} include)

set(TEST_DSET_SOURCE_FILES test/test_Dset.cpp src/Dset.cpp src/DsetPropAccess.cpp)
add_executable(test_Dset ${TEST_DSET_SOURCE_FILES})
target_link_libraries(test_Dset ${HDF5_LIBRARIES})

set(LIB_SOURCE_FILES src/DaqBase.cpp  src/Dset.cpp  src/DsetPropAccess.cpp  src/H5OpenObjects.cpp  src/VDSRoundRobin.cpp)
add_library(lib/liblc2daq.so ${LIB_SOURCE_FILES})

add_executable(bin/ana_reader_master app/ana_reader_master.cpp)
target_link_libraries(bin/ana_reader_master lib/liblc2daq.so ${HDF5_LIBRARIES})


