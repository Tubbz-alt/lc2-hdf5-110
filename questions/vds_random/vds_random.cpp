#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include <chrono>

#include "hdf5.h"
#include "hdf5_hl.h"

typedef std::chrono::high_resolution_clock Clock;

const char *DEFAULT_PREFIX = "/reg/d/ana01/temp/davidsch/lc2/vds_random";

std::pair<long, long> get_seconds_milli(std::chrono::time_point<Clock> t0) {
  std::chrono::time_point<Clock> t1 =  Clock::now();
  auto total_diff = t1 - t0;
  auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(total_diff).count();
  auto seconds = milliseconds / 1000;
  auto milli_frac = milliseconds % 1000;
  
  std::pair<long, long> sec_milli(seconds, milli_frac);
  return sec_milli;
}

/* create a chunked file of long */
void create_src_file(const std::string &fname, 
                     std::vector<hsize_t> &coord_include, 
                     hsize_t num_elem) {
  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  
  hid_t fid = H5Fcreate(fname.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  printf("%s\n", fname.c_str());

  int rank1=1;
  hsize_t size0=0;
  hsize_t unlimited=H5S_UNLIMITED;

  // we want 4MB chunks - the data type will have 8 bytes, so we do 4 << 17 
  // instead of 4 << 20
  hsize_t chunk_size = 4l << 17;
  hid_t plist = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_chunk(plist, rank1, &chunk_size);  
  
  hid_t space = H5Screate_simple(rank1, &size0, &unlimited);
  hid_t dset = H5Dcreate2( fid, "data", H5T_NATIVE_LONG, space, H5P_DEFAULT, plist, H5P_DEFAULT);
  
  std::vector<long> data(chunk_size);
  long next_val = 0;
  hsize_t chunk = 0;
  coord_include.clear();

  do {
    for (size_t idx = 0; idx < chunk_size; ++idx) {
      data[idx] = next_val++;
      if ((next_val % 2 == 0)) {
        coord_include.push_back(next_val);
      }
    }
    H5DOappend( dset, H5P_DEFAULT, 0, chunk_size, H5T_NATIVE_LONG, &data[0]);
    chunk += 1;
  } while (chunk * chunk_size < num_elem);
  
  H5Dclose( dset );
  H5Sclose( space );
  H5Pclose( plist );
  H5Fclose( fid );
  H5Pclose( fapl );
}

void create_view_file(const std::string &src, 
                      const std::string &fname, 
                      const std::vector<hsize_t> &coord_include, 
                      hsize_t num_elem) {
  
  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  
  hid_t fid = H5Fcreate(fname.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, fapl);
  
  int rank1=1;  
  hsize_t view_size=coord_include.size();

  hid_t view_space = H5Screate_simple(rank1, &view_size, &view_size);
  hid_t src_space =  H5Screate_simple(rank1, &num_elem, &num_elem);
  H5Sselect_all(view_space);
  H5Sselect_elements(src_space, H5S_SELECT_SET, coord_include.size(), &coord_include.at(0));

  long fill = -1;
  hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
  H5Pset_fill_value(dcpl, H5T_NATIVE_LONG, &fill);

  H5Pset_virtual(dcpl, view_space, src.c_str(), "/data", src_space);

  hid_t dset = H5Dcreate2( fid, "data", H5T_NATIVE_LONG, view_space, 
                           H5P_DEFAULT, dcpl, H5P_DEFAULT);

  
  H5Dclose( dset );
  H5Pclose( dcpl );
  H5Sclose( src_space );
  H5Fclose( view_space );
  H5Pclose( fid );
  H5Pclose( fapl );
}


/*
  hsize_t size0=0;
  hsize_t size2N=2*sizeN;
  hsize_t unlimited = H5S_UNLIMITED;
  const int rank1 = 1;

  hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
  hid_t fid = H5Fcreate("master.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl);

  short fill = -1;
  hid_t dcpl = H5Pcreate (H5P_DATASET_CREATE);
  H5Pset_fill_value (dcpl, H5T_NATIVE_SHORT, &fill);
 
  hid_t vds_space = H5Screate_simple(rank1, &size2N, NULL);
  hid_t src_space = H5Screate_simple(rank1, &sizeN, NULL);
  
  hsize_t start0=0, start1=1, stride1=1, stride2=2, countN=sizeN, count1=1, block1=1,
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

*/


  int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "usage: vds_random N x [prefix]\n");
    fprintf(stderr, "       N is how many elements in src dset.\n");
    fprintf(stderr, "       prefix is output prefix for two files, defaults to %s.\n", DEFAULT_PREFIX);
    fprintf(stderr, "\nwith default prefix, creates %s_src.h5 and %s_view.h5\n", DEFAULT_PREFIX, DEFAULT_PREFIX);
    return -1;
  }

  hsize_t sizeN = atol(argv[1]);
  const char *prefix = DEFAULT_PREFIX;
  if (argc > 2) prefix = argv[2];

  std::string src_fname(prefix);
  src_fname += std::string("_src.h5");

  std::string view_fname(prefix);
  view_fname += std::string("_view.h5");

  std::chrono::time_point<Clock> t0 = Clock::now();
  std::vector<hsize_t> coord_include;
  create_src_file(src_fname, coord_include, sizeN);
  std::pair<long, long> sec_milli = get_seconds_milli(t0);
  std::cout << "created src file: " << src_fname  << " in " << sec_milli.first
            << " sec and " << sec_milli.second << " milli" << std::endl;
 

  t0 = Clock::now();
  create_view_file(src_fname, view_fname, coord_include, sizeN);
  sec_milli = get_seconds_milli(t0);
  std::cout << "created view file: " << view_fname  << " in " << sec_milli.first
            << " sec and " << sec_milli.second << " milli" << std::endl;

  return 0;
}
