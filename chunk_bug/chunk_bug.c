#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdexcept>

#define CHECK(arg) check_nonneg(arg, #arg, __LINE__, __FILE__)
long long int check_nonneg(long long int val, const char *expression, int lineno, const char *fname) {
  if (val < 0) {
    static char msg[1024]; 
    sprintf(msg, "ERROR: %lld = %s line=%d  file=%s\n", val, expression, lineno, fname);
    fprintf(stderr,msg);
    abort();
  } 
  return val;
}

const char *SRC_FILE = "a.h5";
const char *SRC_DATASET = "A";

const int RANK1 = 1;
const int DIM0 = 6;

int
main (void)
{
  int          ii;
  hid_t        fapl, file, group1, group2, space, plist, dset;
  herr_t       status;
  hsize_t      dims[1] = {DIM0},
               max_dims[1] = {H5S_UNLIMITED}, 
               chunk[1] = {4};
  int          wdata[DIM0];
 
    for (ii = 0; ii < DIM0; ii++) wdata[ii] = ii;
        
    fapl = CHECK( H5Pcreate(H5P_FILE_ACCESS) );
    CHECK( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
    file = CHECK( H5Fcreate (SRC_FILE, H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
    CHECK( H5Pclose( fapl ) );
    group1 = CHECK( H5Gcreate2(file, "group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );
    group2 = CHECK( H5Gcreate2(group1, "group2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );
    space = CHECK( H5Screate_simple (RANK1, dims, max_dims) );
    plist = CHECK( H5Pcreate(H5P_DATASET_CREATE) );
    CHECK( H5Pset_chunk( plist, RANK1, chunk) );
    dset = CHECK( H5Dcreate2 (group2, SRC_DATASET, H5T_NATIVE_INT, space, 
                              H5P_DEFAULT, plist, H5P_DEFAULT) );
    CHECK( H5Pclose( plist ) );
    CHECK( H5Fstart_swmr_write(file) );
    CHECK( H5Dwrite (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, wdata) );
    CHECK( H5Dflush(dset) );
    CHECK( H5Sclose (space) );
    CHECK( H5Dclose (dset) );
    CHECK( H5Fclose (file) );
    printf("about to close\n");
    CHECK( H5close() );
    return 0;
}

