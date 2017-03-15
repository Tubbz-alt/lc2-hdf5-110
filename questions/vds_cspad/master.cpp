#include <cstdio>
#include <cstdlib>
#include <vector>
#include "hdf5.h"

int main(int argc, char *argv[]) {
  if (argc != 4) {
    printf("ERROR, no args\n");
    return -1;
  }
  int dim = atoi(argv[1]);
  std::string src1 = std::string(argv[2]);
  std::string src2 = std::string(argv[3]);

  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate("master.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  hid_t h5type = H5T_NATIVE_SHORT;
  hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
  short fill = -1;
  H5Pset_fill_value(dcpl, H5T_NATIVE_SHORT, &fill);
  hsize_t size0[3] = {0,dim,dim};
  hsize_t start0[3] = {0,0,0};
  hsize_t start1[3] = {1,0,0};
  hsize_t stride2[3] = {2,1,1};
  hsize_t stride1[3] = {1,1,1};
  hsize_t blockOne[3] = {1,dim,dim};
  hsize_t blockUnlimited[3] = {H5S_UNLIMITED,dim,dim};
  hsize_t countUnlimited[3] = {H5S_UNLIMITED,1,1};
  hsize_t countOne[3] = {1,1,1};
  
  hsize_t sizeUnlimited[3] = {H5S_UNLIMITED, dim, dim};
  int rank3 = 3;

  hid_t vds_space = H5Screate_simple(rank3, size0, sizeUnlimited);
  hid_t src_space = H5Screate_simple(rank3, size0, sizeUnlimited);

  H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, start0, stride2, countUnlimited, blockOne);
  H5Sselect_hyperslab(src_space, H5S_SELECT_SET, start0, stride1, countOne, blockUnlimited);
  H5Pset_virtual(dcpl, vds_space, src1.c_str(), "cspad", src_space);
  
  H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, start1, stride2, countUnlimited, blockOne);
  H5Sselect_hyperslab(src_space, H5S_SELECT_SET, start0, stride1, countOne, blockUnlimited);
  H5Pset_virtual(dcpl, vds_space, src2.c_str(), "cspad", src_space);
  
  hid_t dset = H5Dcreate(fid, "vds", H5T_NATIVE_SHORT, vds_space, H5P_DEFAULT, dcpl, H5P_DEFAULT);

  H5Dclose(dset);
  H5Sclose(src_space);
  H5Sclose(vds_space);
  H5Pclose(dcpl);
  H5Fclose(fid);
  H5Pclose(fapl);

  return 0;
}
