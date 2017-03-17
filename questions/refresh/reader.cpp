#include <cstdio>
#include <cstdlib>
#include <vector>
#include "hdf5.h"

int main(int argc, char *argv[]) {
  if (argc != 5) {
    fprintf(stderr, "usage: writer N M refresh outputfile\n");
    fprintf(stderr, "  where N = number of elements\n");
    fprintf(stderr, "        M = number of datasets\n");
    fprintf(stderr, "        refresh = 1 to do the refresh, 0 otherwise (for comparision)\n");
    fprintf(stderr, "        inputfile = name of input file\n");
    fprintf(stderr, "The input file is opened in SWMR mode, and N*M H5Drefreshes are done\n");
    return -1;
  }

  const int num_elem = atoi(argv[1]);
  const int num_datasets = atoi(argv[2]);
  bool do_refresh = (1 == atoi(argv[3]));
  const char *input_file_name = argv[4];

  hid_t fid = H5Fopen(input_file_name, H5F_ACC_RDONLY | H5F_ACC_SWMR_READ, H5P_DEFAULT);
  if (fid <= 0) {
    fprintf(stderr, "error opening input file %s\n", input_file_name);
    return -1;
  }
  std::vector<hid_t> datasets;
  for (int idx=0; idx < num_datasets; ++idx) {
    char dset_name[128];
    sprintf(dset_name, "%5.5d", idx);
    hid_t dset = H5Dopen2(fid, dset_name, H5P_DEFAULT);
    if (dset < 0) {
      fprintf(stderr, "error opening dset for %s", dset_name);
      return -1;
    }
    datasets.push_back(dset);
  }

  for (int elem=0; elem < num_elem; ++elem) {
    for (int idx=0; idx < num_datasets; ++idx) {
      hid_t dset = datasets.at(idx);
      if (do_refresh) {
        H5Drefresh( dset );
      }
    }
  }
  
  for (int idx=0; idx < num_datasets; ++idx) {
    hid_t dset = datasets.at(idx);
    H5Dclose( dset );
  }
  
  H5Fclose(fid);
}

