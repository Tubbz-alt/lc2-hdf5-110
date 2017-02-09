#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdexcept>

#define CHECK(arg) check_nonneg(arg, #arg, __LINE__, __FILE__)
long long int check_nonneg(long long int val, const char *expression, int lineno, const char *fname) {
  if (val < 0) {
    static char msg[1024]; 
    sprintf(msg, "ERROR: %lld = %s line=%d  file=%s\n", val, expression, lineno, fname);
    throw std::runtime_error(msg);
  } 
  return val;
}

#define RANK1           1

const char *SRC_FILE[] = {
    "/reg/d/ana01/temp/davidsch/lc2/runA/a.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/b.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/c.h5"
};

const char *SRC_DATASET = "A";

hsize_t SRC_LEN[] = {4,3,3};

int
main (void)
{
  hid_t        fapl, file, group1, group2, space_create, dcpl, dset, space_write_file, space_write_mem;
  hsize_t      start_dims[1] = {0},
               max_dims[1] = {H5S_UNLIMITED},
               chunk[1] = {4},
               start0 = 0,
               count1 = 1,
               new_extent, file_dest;
  int          ii, jj;
  long         wdata;

  space_write_mem = CHECK( H5Screate_simple(RANK1, &count1, &count1) );

  for (ii=0; ii < 3; ii++) {
    fapl = CHECK( H5Pcreate(H5P_FILE_ACCESS) );
    CHECK( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
    file = CHECK( H5Fcreate (SRC_FILE[ii], H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
    CHECK( H5Pclose( fapl ) );
    group1 = CHECK( H5Gcreate2(file, "group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );
    group2 = CHECK( H5Gcreate2(group1, "group2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );
    space_create = CHECK( H5Screate_simple (RANK1, start_dims, max_dims) );
    dcpl = CHECK( H5Pcreate(H5P_DATASET_CREATE) );
    CHECK( H5Pset_chunk( dcpl, RANK1, chunk ) );
    dset = CHECK( H5Dcreate2 (group2, SRC_DATASET, H5T_NATIVE_LONG, space_create, 
                              H5P_DEFAULT, dcpl, H5P_DEFAULT) );
    CHECK( H5Pclose( dcpl ) );
    CHECK( H5Sclose ( space_create ) );
    
    CHECK( H5Fstart_swmr_write( file ) );
    
    for (jj=0; jj < SRC_LEN[ii]; ++jj) {
      file_dest = jj;
      new_extent = jj+1;
      wdata = (10 * ii) + jj;
      CHECK( H5Dset_extent( dset, &new_extent ) );
      space_write_file = CHECK( H5Dget_space( dset ) );
      CHECK( H5Sselect_hyperslab( space_write_file, H5S_SELECT_SET, &file_dest, NULL, &count1, NULL) );
      CHECK( H5Dwrite ( dset, H5T_NATIVE_LONG, space_write_mem, space_write_file, H5P_DEFAULT, &wdata ) );
      CHECK( H5Sclose( space_write_file ) );
      CHECK( H5Dflush ( dset ) );
    }

    CHECK( H5Dclose ( dset ) );
    CHECK( H5Gclose( group2 ) );
    CHECK( H5Gclose( group1 ) );
    CHECK( H5Fclose ( file ) );
  }
  CHECK( H5Sclose( space_write_mem ) );
  CHECK( H5close() );
  return 0;
}

