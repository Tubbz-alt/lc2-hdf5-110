#include <cstdio>
#include <cstdlib>
#include "hdf5.h"

int main(int argc, const char *argv[]) {
  if (argc != 4) {
    fprintf(stderr, "usage: num_writers writer_outputfile outputfile\n");
    fprintf(stderr, "  where num_writers is the number of writers\n");
    fprintf(stderr, "        writer_outputfile is the writer output file with a %%d in it\n");
    fprintf(stderr, "        outputfile = name of master output file\n");
    return -1;
  }

  const int num_writers = atoi(argv[1]);
  const char *writer_outputfile = argv[2];
  const char *master_outputfile = argv[3];
  
  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate(master_outputfile, H5F_ACC_TRUNC, H5P_DEFAULT, fapl);

  short fill = -1;
  hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_fill_value(dcpl, H5T_NATIVE_SHORT, &fill);
 
  const int rank1 = 1;
  hsize_t size0 = 0;
  hsize_t unlimited = H5S_UNLIMITED;
  hid_t vds_space = H5Screate_simple(rank1, &size0, &unlimited);
  hid_t src_space = H5Screate_simple(rank1, &size0, &unlimited);
  
  hsize_t start0=0, block1=1, count1=1, stride1=1,
    countUnlimited=H5S_UNLIMITED, blockUnlimited=H5S_UNLIMITED;

  for (int writer = 0; writer < num_writers; ++writer) {
    char this_writer_outputfile[1024];
    sprintf(this_writer_outputfile, writer_outputfile, writer);

    hsize_t vds_stride = num_writers;
    hsize_t vds_start = writer;
    
    H5Sselect_hyperslab(vds_space, H5S_SELECT_SET, &vds_start, &vds_stride, &countUnlimited, &block1);
    H5Sselect_hyperslab(src_space, H5S_SELECT_SET, &start0, &stride1, &count1, &blockUnlimited);
    H5Pset_virtual(dcpl, vds_space, this_writer_outputfile, "/data", src_space);
  
  }
  
  hid_t dset = H5Dcreate2(fid, "vds", H5T_NATIVE_LONG, vds_space, 
                          H5P_DEFAULT, dcpl, H5P_DEFAULT);
  H5Dclose(dset);
  H5Sclose(src_space);
  H5Sclose(vds_space);
  H5Pclose(dcpl);
  H5Fclose(fid);
  H5Pclose(fapl);
  return 0;
}

