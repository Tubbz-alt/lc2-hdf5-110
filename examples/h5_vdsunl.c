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


 ************************************************************/

#include "hdf5.h"
#include <stdio.h>
#include <stdlib.h>

#define VFILE        "vdsunl.h5"
#define DATASET      "VDSUNL"
#define VDSDIM1         6 
#define VDSDIM0         4 
#define DIM0            6 
#define RANK1           1
#define RANK2           2

#define SRCFILE   "srcfile.h5"

const char *SRC_DATASET[] = {
    "A-0",
    "A-1",
    "A-2",
    "A-3"
};

int
main (void)
{
    hid_t        vfile, space, vspace, dset; /* Handles */ 
    hid_t        src_file, src_dset, src_space;
    hid_t        dcpl;
    herr_t       status;
    hsize_t      vdsdims[2] = {VDSDIM0, VDSDIM1},      /* Virtual datasets dimension */
                 vdsdims_max[2] = {H5S_UNLIMITED, VDSDIM1},
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


    src_file = H5Fcreate (SRCFILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    src_space = H5Screate_simple (RANK1, dims, NULL);

    for (i=0; i < VDSDIM0; i++) {
        for (j = 0; j < DIM0; j++) wdata[j] = i;
        src_dset = H5Dcreate2 (src_file, SRC_DATASET[i], H5T_NATIVE_INT, src_space, H5P_DEFAULT,
                    H5P_DEFAULT, H5P_DEFAULT);
        status = H5Dwrite (src_dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                    wdata);
        status = H5Dclose (src_dset);
     }


    vfile = H5Fcreate (VFILE, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    vspace = H5Screate_simple (RANK2, vdsdims, vdsdims_max);

    dcpl = H5Pcreate (H5P_DATASET_CREATE);
    status = H5Pset_fill_value (dcpl, H5T_NATIVE_INT, &fill_value);
     
    start[0] = 0;
    start[1] = 0;
    count[0] = H5S_UNLIMITED;
    count[1] = 1;
    block[0] = 1;
    block[1] = DIM0;

    status = H5Sselect_hyperslab (vspace, H5S_SELECT_SET, start, NULL, count, block);
    status = H5Pset_virtual (dcpl, vspace, SRCFILE, "/A-%b", src_space);

    dset = H5Dcreate2 (vfile, DATASET, H5T_NATIVE_INT, vspace, H5P_DEFAULT, dcpl, H5P_DEFAULT);
    status = H5Sclose (vspace);
    status = H5Dclose (dset);
    status = H5Pclose (dcpl);
    status = H5Fclose (vfile);    

    
    status = H5Sclose (src_space);
    status = H5Fclose (src_file);    
   
    /* Re-open the virtual file and dataset. */ 

    vfile = H5Fopen (VFILE, H5F_ACC_RDONLY, H5P_DEFAULT);
    dset = H5Dopen2 (vfile, DATASET, H5P_DEFAULT);

    dcpl = H5Dget_create_plist (dset);

    layout = H5Pget_layout (dcpl);
    if (H5D_VIRTUAL == layout) 
        printf(" Dataset has a virtual layout \n");
    else
        printf(" Wrong layout found \n");

    status = H5Pget_virtual_count (dcpl, &num_map);
    printf(" Number of mappings is %lu\n", (unsigned long)num_map);
    
    for (i = 0; i < (int)num_map; i++) {   
        printf(" Mapping %d \n", i);
        printf("         Selection in the virtual dataset ");
        vspace = H5Pget_virtual_vspace (dcpl, (size_t)i);

        if (H5Sget_select_type(vspace) == H5S_SEL_HYPERSLABS) { 
            if (H5Sis_regular_hyperslab(vspace)) {
                status = H5Sget_regular_hyperslab (vspace, start_out, stride_out, count_out, block_out);
                printf("\n         start  = [%llu, %llu] \n", (unsigned long long)start_out[0], (unsigned long long)start_out[1]);
                printf("         stride = [%llu, %llu] \n", (unsigned long long)stride_out[0], (unsigned long long)stride_out[1]);
                printf("         count  = [%lli, %llu] \n", (signed long long)count_out[0], (unsigned long long)count_out[1]);
                printf("         block  = [%llu, %llu] \n", (unsigned long long)block_out[0], (unsigned long long)block_out[1]);
            }
     
        }
        len = H5Pget_virtual_filename (dcpl, (size_t)i, NULL, 0);
        filename = (char *)malloc((size_t)len*sizeof(char)+1);

        H5Pget_virtual_filename (dcpl, (size_t)i, filename, len+1);
        printf("         Source filename %s\n", filename);
        len = H5Pget_virtual_dsetname (dcpl, (size_t)i, NULL, 0);
        dsetname = (char *)malloc((size_t)len*sizeof(char)+1);
        H5Pget_virtual_dsetname (dcpl, (size_t)i, dsetname, len+1);
        printf("         Source dataset name %s\n", dsetname);

        printf("         Selection in the source dataset ");
        src_space = H5Pget_virtual_srcspace (dcpl, (size_t)i);
        if(H5Sget_select_type(src_space) == H5S_SEL_ALL) {
                printf("(0) - (%d) \n", DIM0-1);
        }

        status = H5Sclose(vspace);
        status = H5Sclose(src_space);
        free(filename);
        free(dsetname);
    }

    status = H5Dread (dset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                rdata[0]);
    printf (" VDS Data:\n");
    for (i=0; i<VDSDIM0; i++) {
        printf (" [");
        for (j=0; j<VDSDIM1; j++)
            printf (" %3d", rdata[i][j]);
        printf ("]\n");
    }
    status = H5Pclose (dcpl);
    status = H5Dclose (dset);
    status = H5Fclose (vfile);

    return 0;
}
