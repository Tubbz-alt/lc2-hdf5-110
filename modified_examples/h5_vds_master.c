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

#define FILE         "/reg/d/ana01/temp/davidsch/lc2/runA/vds.h5"
#define DATASET      "VDS"
#define VDSDIM1         6 
#define VDSDIM0         4 
#define DIM0            6 
#define RANK1           1
#define RANK2           2

const char *SRC_FILE[] = {
    "/reg/d/ana01/temp/davidsch/lc2/runA/a.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/b.h5",
    "/reg/d/ana01/temp/davidsch/lc2/runA/c.h5"
};

const char *SRC_DATASET[] = {
    "/group1/group2/A",
    "/group1/group2/B",
    "/group1/group2/C"
};

int
main (void)
{
    hid_t        file, space, src_space, vspace, dset, fapl; /* Handles */
    hid_t        src_files[3];
    hid_t        dcpl;
    herr_t       status;
    hsize_t      vdsdims[2] = {VDSDIM0, VDSDIM1},      /* Virtual datasets dimension */
                 dims[1] = {DIM0},                     /* Source datasets dimensions */
                 start[2],                             /* Hyperslab parameters */
                 stride[2],
                 count[2],
                 block[2];
    hsize_t      start_out[2],
                 stride_out[2],
                 count_out[2],
                 block_out[2];
    int          wdata[DIM0],                /* Write buffer for source dataset */
                 rdata[VDSDIM0][VDSDIM1],    /* Read buffer for virtual dataset */
                 i, j, k, l;  
    int          fill_value = -1;            /* Fill value for VDS */
    H5D_layout_t layout;                     /* Storage layout */
    size_t       num_map;                    /* Number of mappings */
    ssize_t      len;                        /* Length of the string; also a return value */
    char         *filename;                  
    char         *dsetname;
    hsize_t      nblocks;
    hsize_t      *buf;                       /* Buffer to hold hyperslab coordinates */


    /* Create file in which virtual dataset will be stored. */
    file = H5Fcreate (FILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    /* Create VDS dataspace.  */
    space = H5Screate_simple (RANK2, vdsdims, NULL);

    /* Set VDS creation property. */
    dcpl = H5Pcreate (H5P_DATASET_CREATE);
    status = H5Pset_fill_value (dcpl, H5T_NATIVE_INT, &fill_value);
     
    /* Initialize hyperslab values. */
    start[0] = 0;
    start[1] = 0;
    count[0] = 1;
    count[1] = 1;
    block[0] = 1;
    block[1] = VDSDIM1;

    for (i=0; i <3; i++) src_files[i] = H5Fopen(SRC_FILE[i], H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT);
    /* 
     * Build the mappings.
     * Selections in the source datasets are H5S_ALL.
     * In the virtual dataset we select the first, the second and the third rows 
     * and map each row to the data in the corresponding source dataset. 
     */
    src_space = H5Screate_simple (RANK1, dims, NULL);
    for (i = 0; i < 3; i++) {
        start[0] = (hsize_t)i;
        /* Select i-th row in the virtual dataset; selection in the source datasets is the same. */
        status = H5Sselect_hyperslab (space, H5S_SELECT_SET, start, NULL, count, block);
        status = H5Pset_virtual (dcpl, space, SRC_FILE[i], SRC_DATASET[i], src_space);
    }

    /* Create a virtual dataset. */
    dset = H5Dcreate2 (file, DATASET, H5T_NATIVE_INT, space, H5P_DEFAULT,
                dcpl, H5P_DEFAULT);
    status = H5Sclose (space);
    status = H5Sclose (src_space);
    status = H5Dclose (dset);
    status = H5Fclose (file);    
     
    return 0;
}

