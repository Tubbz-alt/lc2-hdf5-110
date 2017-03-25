#include <stdint.h>
#include "hdf5.h"
#include "hdf5_hl.h"

int main(int argc, char *argv[]) {
  hid_t fapl =  H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid =  H5Fcreate("test_invariant.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  H5Pclose(fapl);
  hid_t small_group =  H5Gcreate2(fid, "small", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);  
  
  int rank = 1;
  hsize_t current_dim = 0;
  hsize_t maximum_dim = H5S_UNLIMITED;
  hsize_t chunk = 100;
  
  hid_t space_id =  H5Screate_simple(rank, &current_dim, &maximum_dim);
  hid_t plist_id =  H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_chunk(plist_id, rank, &chunk);
  
  hid_t h5_dset =  H5Dcreate2(small_group, "data", H5T_NATIVE_INT64, space_id, H5P_DEFAULT, plist_id, H5P_DEFAULT);
  
  H5Sclose(space_id);
  H5Pclose(plist_id);
    
  unsigned index = 0;
  int64_t value = 0;
  
  H5DOappend( h5_dset, H5P_DEFAULT, index, 1, H5T_NATIVE_INT64, &value );
  H5Dclose(h5_dset);
  H5Gclose(small_group);
  //  H5Fclose( fid );
}
