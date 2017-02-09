#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdexcept>
#include <vector>

#define CHECK(arg) check_nonneg(arg, #arg, __LINE__, __FILE__)
long long int check_nonneg(long long int val, const char *expression, int lineno, const char *fname) {
  if (val < 0) {
    static char msg[1024]; 
    sprintf(msg, "ERROR: %lld = %s line=%d  file=%s\n", val, expression, lineno, fname);
    throw std::runtime_error(msg);
  }
  printf("%s\n", expression);
  return val;
}

#define FILE         "vds.h5"
#define RANK1           1
#define NUM_SRCS        3

const char *SRC_FILE[NUM_SRCS] = {
    "a.h5",
    "b.h5",
    "c.h5",
};

hsize_t SRC_LEN[NUM_SRCS] = {4,3,3};

const char *SRC_DATASET = "/group1/group2/A";

int
main (int argc, char *argv[])
{
  hid_t        fapl, file, dcpl, vds_space, src_space, dset; 
  hsize_t      vds_dim = 10,
               start, stride, count, block;
                 
  int          ii;

  H5open();
  fapl = CHECK( H5Pcreate(H5P_FILE_ACCESS) );
  CHECK( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  file = CHECK( H5Fcreate (FILE, H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  CHECK( H5Pclose( fapl ) );
  hid_t small_group = CHECK( H5Gcreate2(file, "small", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );  
  hid_t vlen_group = CHECK( H5Gcreate2(file, "vlen", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );  
  hid_t detector_group = CHECK( H5Gcreate2(file, "detector", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );  
  hid_t group2 = CHECK( H5Gcreate2( detector_group, "00000", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT ) );

  dcpl = CHECK( H5Pcreate(H5P_DATASET_CREATE) );
  long fill_value = -1;
  CHECK( H5Pset_fill_value (dcpl, H5T_NATIVE_LONG, &fill_value) );

  vds_space = CHECK( H5Screate_simple (RANK1, &vds_dim, NULL) );

  stride = NUM_SRCS;
  block = 1;
  std::vector<hid_t> spaces;
  for (ii = 0; ii < NUM_SRCS; ii++) {
    src_space = CHECK( H5Screate_simple (RANK1, &SRC_LEN[ii], NULL) );
    spaces.push_back(src_space);
    count = SRC_LEN[ii];
    start = ii;
    CHECK( H5Sselect_hyperslab (vds_space, H5S_SELECT_SET, &start, &stride, &count, &block) );
    CHECK( H5Pset_virtual (dcpl, vds_space, SRC_FILE[ii], SRC_DATASET, src_space) );
  }

  start=0; stride=1; count=vds_dim; block=1;
  CHECK( H5Sselect_hyperslab( vds_space, H5S_SELECT_SET, &start, &stride, &count, &block) );
  dset = CHECK( H5Dcreate2 (group2, "fiducials", H5T_NATIVE_INT, vds_space, H5P_DEFAULT, dcpl, H5P_DEFAULT) );

  CHECK( H5Sclose( vds_space ) );
  CHECK( H5Pclose( dcpl ) );
  CHECK( H5Sclose( spaces.at(0) ) );
  CHECK( H5Sclose( spaces.at(1) ) );
  CHECK( H5Sclose( spaces.at(2) ) );
  CHECK( H5Dclose( dset ) );
  CHECK( H5Gclose( small_group ) );

  // CHECK( H5Gclose( vlen_group ) );
  CHECK( H5Gclose( detector_group ) );
  CHECK( H5Gclose( group2 ) );
  CHECK( H5Fclose( file ) );
  CHECK( H5close() );

  return 0;
}

