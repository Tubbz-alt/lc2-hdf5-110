#include <cstdio>
#include <cstdlib>
#include "hdf5.h"

/*
  Before running this, create two files

  srcA.h5
  srcB.h5

  which have the datasets:
  srcA.h5/dsetA native short
  srcB.h5/dsetB native short

 */
int main() {
  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate("master.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);

  short fill = -1;
  hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_fill_value(dcpl, H5T_NATIVE_SHORT, &fill);
 
  const int rank1 = 1;
  hsize_t size0 = 0;
  hsize_t unlimited = H5S_UNLIMITED;
  hid_t vds_space = H5Screate_simple(rank1, &size0, &unlimited);
  hid_t src_space = H5Screate_simple(rank1, &size0, &unlimited);
  
  hsize_t start0=0, start1=1, stride2=2, block1=1, count1=1, stride1=1,
    countUnlimited=H5S_UNLIMITED, blockUnlimited=H5S_UNLIMITED;

  H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, &start0, &stride2, &countUnlimited, &block1);
  H5Sselect_hyperslab(src_space, H5S_SELECT_SET, &start0, &stride1, &count1, &blockUnlimited);
  H5Pset_virtual(dcpl, vds_space, "srcA.h5", "/dsetA", src_space);
  
  H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, &start1, &stride2, &countUnlimited, &block1);
  H5Sselect_hyperslab(src_space, H5S_SELECT_SET, &start0, &stride1, &count1, &blockUnlimited);
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

