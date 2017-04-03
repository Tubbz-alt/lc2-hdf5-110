cmake_minimum_required(VERSION 3.6)
project(lc2-hdf5-110)

set(CMAKE_CXX_STANDARD 11)
set(HDF5_ROOT /Users/david/Code/hdf5-1.10.0-patch1/hdf5)
find_package(HDF5 REQUIRED)
include_directories(${HDF5_INCLUDE_DIR} include)

set(SOURCE_FILES test/test_Dset.cpp src/Dset.cpp src/DsetPropAccess.cpp)
add_executable(test_Dset ${SOURCE_FILES})
target_link_libraries(test_Dset ${HDF5_LIBRARIES})
