#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include "hdf5.h"
#include "hdf5_hl.h"

int main(int argc, char *argv[]) {
  if (argc != 6) {
    fprintf(stderr, "usage: writer i D F N outputfile\n");
    fprintf(stderr, "  where i = writer number\n");
    fprintf(stderr, "        D = delay in microseconds between writes\n");
    fprintf(stderr, "        F = number of writes between flushes and chunksize\n");
    fprintf(stderr, "        N = total number of elements to write\n");
    fprintf(stderr, "        outputfile = name of output file\n");
    return -1;
  }

  const int writer = atoi(argv[1]);
  const int microseconds_between_writes = atoi(argv[2]) + 100*writer;
  const long writes_between_flushes = atol(argv[3]);
  const long num_writes = atol(argv[4]);
  const char *output_file_name = argv[5];

  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate(output_file_name, H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  
  int rank1=1;
  hsize_t chunksize=writes_between_flushes;
  hsize_t size0=0, unlimited=H5S_UNLIMITED;
  hid_t space_id = H5Screate_simple(rank1, &size0, &unlimited);
  hid_t plist_id = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_chunk(plist_id, rank1, &chunksize);
  
  hid_t access_id = H5Pcreate(H5P_DATASET_ACCESS);
  size_t rdcc_nslots = 101;
  size_t rdcc_nbytes = chunksize * 24;  // 24 from 8*3 where 8 is sizeof(long), 3, just more room
  double rdcc_w0 = 0.75;
  H5Pset_chunk_cache(access_id, rdcc_nslots, rdcc_nbytes, rdcc_w0);

  hid_t dset = H5Dcreate2(fid, "data", H5T_NATIVE_LONG, space_id, 
                          H5P_DEFAULT, plist_id, access_id);

  H5Fstart_swmr_write(fid);

  for (long elem = 0; elem < num_writes; ++elem) {
    H5DOappend(dset, H5P_DEFAULT, 0, 1, H5T_NATIVE_LONG, &elem);
    usleep(microseconds_between_writes);
    if (elem % writes_between_flushes == (writes_between_flushes-1)) {
      H5Dflush(dset);
    }
  }

  H5Dclose(dset);
  H5Pclose(access_id);
  H5Pclose(plist_id);
  H5Sclose(space_id);
  H5Fclose(fid);
  H5Pclose(fapl);
}

