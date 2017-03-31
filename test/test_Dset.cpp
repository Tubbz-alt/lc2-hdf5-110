#include <iostream>
#include "check_macros.h"
#include "Dset.h"


hid_t create_file(const char *fname) {
  hid_t fapl =  NONNEG(H5Pcreate(H5P_FILE_ACCESS));
  NONNEG(H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST));
  hid_t fid =  NONNEG(H5Fcreate(fname, H5F_ACC_TRUNC, H5P_DEFAULT, fapl));
  NONNEG(H5Pclose(fapl));
  return fid;
}


void write_file() {
  hid_t fid = create_file("test_Dset.h5");
  std::vector<hsize_t> chunk = {100};
  Dset dset = Dset::create(fid, "dsetA", H5T_NATIVE_INT64, chunk);
  std::vector<int64_t> data = {3,4,5};
  dset.append(3, data);
  dset.append(3, data);
  dset.append(3, data);
  dset.close();
  NONNEG(H5Fclose(fid));
}


void read_file() {
  hid_t fid = H5Fopen("test_Dset.h5",H5P_DEFAULT, H5P_DEFAULT);
  Dset dset = Dset::open(fid, "dsetA");
  std::vector<int64_t> buf;
  dset.read(0,9,buf);
  std::cout << "read  back: "  << buf << std::endl;
  dset.close();
}


int main(int argc, char *argv[]) {
  write_file();
  read_file();
  return 0;
}
