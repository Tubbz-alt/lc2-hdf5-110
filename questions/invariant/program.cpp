#include <stdint.h>
#include "hdf5.h"
#include "hdf5_hl.h"

#define NONNEG( x ) x

int main(int argc, char *argv[]) {
  H5open();
  hid_t fapl = NONNEG( H5Pcreate(H5P_FILE_ACCESS) );
  NONNEG( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  hid_t fid = NONNEG( H5Fcreate("test_invariant.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  NONNEG( H5Pclose(fapl) );
  hid_t small_group = NONNEG( H5Gcreate2(fid, "small", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT) );  
  
  int rank = 1;
  hsize_t current_dim = 0;
  hsize_t maximum_dim = H5S_UNLIMITED;
  hsize_t chunk = 100;

  hid_t space_id = NONNEG( H5Screate_simple(rank, &current_dim, &maximum_dim) );
  hid_t plist_id = NONNEG( H5Pcreate(H5P_DATASET_CREATE) );
  NONNEG(H5Pset_chunk(plist_id, rank, &chunk) );
    
  hid_t access_id = NONNEG( H5Pcreate(H5P_DATASET_ACCESS) );
    
  hsize_t num_bytes_in_a_chunk = 8 * chunk;

  size_t rdcc_nslots = 101;
  size_t rdcc_nbytes = num_bytes_in_a_chunk * 3;
  double rdcc_w0 = 0.75;
  
  NONNEG(H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0) );
    
  hid_t h5_dset = NONNEG( H5Dcreate2(small_group, "data", H5T_NATIVE_INT64, space_id, H5P_DEFAULT, plist_id, access_id) );
    
  NONNEG( H5Sclose(space_id) );
  NONNEG( H5Pclose(plist_id) );
  NONNEG( H5Pclose(access_id) );
    
  unsigned index = 0;
  int64_t value = 0;

  H5DOappend( h5_dset, H5P_DEFAULT, index, 1, H5T_NATIVE_INT64, &value );
  H5Dclose(h5_dset);
  H5close();
}
