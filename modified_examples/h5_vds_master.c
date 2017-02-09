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

#define FILE         "/reg/d/ana01/temp/davidsch/lc2/runA/vds.h5"
#define DATASET      "VDS"
#define RANK1           1
#define NUM_SRCS        4
#define SRC_WRITE_COUNT 6

const char *SRC_FILE[NUM_SRCS] = {
    "/reg/d/ana01/temp/davidsch/lc2/runA/a.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/b.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/c.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/doesnt_exist.h5"
};

const char *SRC_DATASET[NUM_SRCS] = {
    "/group1/group2/A",
    "/group1/group2/B",
    "/group1/group2/C",
    "/group1/group2/doesnt_exist",
};

int
main (void)
{
  hid_t        fapl, file, group1, group2, dcpl, vds_space, src_space, dset; 
  hsize_t      vds_dim = NUM_SRCS * SRC_WRITE_COUNT,
               src_dim = SRC_WRITE_COUNT,  
               unlimited_dim = H5S_UNLIMITED,
               start, stride, count, block;
                
                 
  int          ii, fill_value = -1;    

  fapl = CHECK( H5Pcreate(H5P_FILE_ACCESS) );
  CHECK( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  file = CHECK( H5Fcreate (FILE, H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  CHECK( H5Pclose( fapl ) );
  group1 = CHECK( H5Gcreate2( file, "group1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT ) );
  group2 = CHECK( H5Gcreate2( group1, "group2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT ) );

  dcpl = H5Pcreate(H5P_DATASET_CREATE);
  CHECK( H5Pset_fill_value (dcpl, H5T_NATIVE_INT, &fill_value) );

  vds_space = CHECK( H5Screate_simple (RANK1, &vds_dim, NULL) );
  src_space = CHECK( H5Screate_simple (RANK1, &src_dim, NULL) );

  stride = NUM_SRCS;
  count = SRC_WRITE_COUNT;
  block = 1;
  for (ii = 0; ii < NUM_SRCS; ii++) {
    start = ii;
    CHECK( H5Sselect_hyperslab (vds_space, H5S_SELECT_SET, &start, &stride, &count, &block) );
    CHECK( H5Pset_virtual (dcpl, vds_space, SRC_FILE[ii], SRC_DATASET[ii], src_space) );
    printf("ii=%d success\n", ii);
  }

  start=0; stride=1; count=vds_dim; block=1;
  CHECK( H5Sselect_hyperslab( vds_space, H5S_SELECT_SET, &start, &stride, &count, &block) );
  dset = CHECK( H5Dcreate2 (file, DATASET, H5T_NATIVE_INT, vds_space, H5P_DEFAULT, dcpl, H5P_DEFAULT) );

  CHECK( H5Dclose( dset ) );
  CHECK( H5Pclose( dcpl ) );
  CHECK( H5Sclose( vds_space ) );
  CHECK( H5Sclose( src_space ) );
  CHECK( H5Gclose( group2 ) );
  CHECK( H5Gclose( group1 ) );
  CHECK( H5Fclose( file ) );
  CHECK( H5close() );

  return 0;
}

