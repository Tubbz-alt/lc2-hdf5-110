#include "lc2daq.h"

int main() {
  hid_t fapl = NONNEG( H5Pcreate(H5P_FILE_ACCESS) );
  NONNEG( H5Pset_libver_bounds(fapl, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST) );
  hid_t m_master_fid = NONNEG( H5Fcreate("master_vds_rrobin.h5", 
                                         H5F_ACC_TRUNC, H5P_DEFAULT, fapl) );
  std::vector<std::string> src_filenames{ std::string("data/hdf5/daq_writer-s0000.h5"),
      std::string("data/hdf5/daq_writer-s0001.h5"),
      std::string("data/hdf5/daq_writer-s0002.h5") };

  std::vector<std::string> src_datasets{ std::string("/cspad/00000/data"),
      std::string("/cspad/00000/data"),
      std::string("/cspad/00000/data"),
      };

  VDSRoundRobin vdsRR(m_master_fid, "cspad_vds", src_filenames, src_datasets);
  hid_t vds_dset = vdsRR.get_and_transfer_ownership_of_VDS();
  NONNEG( H5Dclose(vds_dset) );
  NONNEG( H5Fclose( m_master_fid ) );
  NONNEG( H5Pclose( fapl ) );
  
  return 0;
}
