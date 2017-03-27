#include "Dset.h"

hid_t create_file() {
  hid_t fapl =  H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid =  H5Fcreate("test_Dset.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  H5Pclose(fapl);
  return fid;
}

int main(int argc, char *argv[]) {
  hid_t fid = create_file();
  std::vector<hsize_t> chunk = {100};
  Dset dset = Dset::create(fid, "dsetA", H5T_NATIVE_INT64, chunk);
  std::vector<int64_t> data = {3,4,5};
  dset.append(data);
  dset.close();
  H5Fclose(fid);

  return 0;
}
