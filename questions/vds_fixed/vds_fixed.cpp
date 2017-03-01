#include <cstdio>
#include <cstdlib>
#include "hdf5.h"

/*
  Before running this, create two files

  srcA.h5
  srcB.h5

  which have the datasets:
  srcA.h5/dsetA  N native short
  srcB.h5/dsetB  N native short

  where N is the datasets lengths, and it is passed into this routine
 */
int main(int argc, char *argv[]) {
  if (argc != 2) {
    fprintf(stderr, "usage: vds_fixed N where N is how many elements in src dsets\n");
    return -1;
  }

  hsize_t sizeN = atol(argv[1]);
  hsize_t size0=0;
  hsize_t size2N=2*sizeN;
  const int rank1 = 1;

  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate("master.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);

  short fill = -1;
  hid_t dcpl = H5Pcreate (H5P_DATASET_CREATE);
  H5Pset_fill_value (dcpl, H5T_NATIVE_SHORT, &fill);
 
  hid_t vds_space = H5Screate_simple(rank1, &size2N, NULL);
  hid_t src_space = H5Screate_simple(rank1, &sizeN, NULL);
  
  hsize_t start0=0, start1=1, stride2=2, countN=sizeN, block1=1;
  
  H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, &start0, &stride2, &countN, &block1);
  H5Pset_virtual(dcpl, vds_space, "srcA.h5", "/dsetA", src_space);
  
  H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, &start1, &stride2, &countN, &block1);
  H5Pset_virtual(dcpl, vds_space, "srcB.h5", "/dsetB", src_space);
  
  hid_t dset = H5Dcreate2(fid, "vds", H5T_NATIVE_SHORT, vds_space, 
                          H5P_DEFAULT, dcpl, H5P_DEFAULT);
  H5Dclose(dset);
  H5Sclose(src_space);
  H5Sclose(vds_space);
  H5Pclose(dcpl);
  H5Fclose(fid);
  H5Pclose(fapl);
  return 0;
}

