/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Board of Trustees of the University of Illinois.         *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the files COPYING and Copyright.html.  COPYING can be found at the root   *
 * of the source code distribution tree; Copyright.html can be found at the  *
 * root level of an installed copy of the electronic HDF5 document set and   *
 * is linked from the top-level documents page.  It can also be found at     *
 * http://hdfgroup.org/HDF5/doc/Copyright.html.  If you do not have          *
 * access to either file, you may request a copy from help@hdfgroup.org.     *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/************************************************************

  This example illustrates the concept of virtual dataset.
  The program  creates three 1-dim source datasets and writes
  data to them. Then it creates a 2-dim virtual dataset and
  maps the first three rows of the virtual dataset to the data 
  in the source datasets. Elements of a row are mapped to all 
  elements of the corresponding source dataset.
  The fourth row is not mapped and will be filled with the fill 
  values when virtual dataset is read back. 
   
  The program closes all datasets, and then reopens the virtual
  dataset, and finds and prints its creation properties.
  Then it reads the values. 

  This file is intended for use with HDF5 Library version 1.10

 ************************************************************/
/* EIP Add link to the picture */

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

#define WRITE_COUNT     6 
#define RANK1           1

const char *SRC_FILE[] = {
    "/reg/d/ana01/temp/davidsch/lc2/runA/a.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/b.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/c.h5"
};

const char *SRC_DATASET[] = {
    "A",
    "B",
    "C"
};

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
  int          wdata, ii, jj;

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
    dset = CHECK( H5Dcreate2 (group2, SRC_DATASET[ii], H5T_NATIVE_INT, space_create, 
                              H5P_DEFAULT, dcpl, H5P_DEFAULT) );
    CHECK( H5Pclose( dcpl ) );
    CHECK( H5Sclose ( space_create ) );

    CHECK( H5Fstart_swmr_write( file ) );
    
    wdata = 10 + WRITE_COUNT*ii;
    for (jj=0; jj < WRITE_COUNT; ++jj) {
      file_dest = jj;
      new_extent = jj+1;
      wdata += 1;
      CHECK( H5Dset_extent( dset, &new_extent ) );
      space_write_file = CHECK( H5Dget_space( dset ) );
      CHECK( H5Sselect_hyperslab( space_write_file, H5S_SELECT_SET, &file_dest, NULL, &count1, NULL) );
      CHECK( H5Dwrite ( dset, H5T_NATIVE_INT, space_write_mem, space_write_file, H5P_DEFAULT, &wdata ) );
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

