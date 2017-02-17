#include "lc2daq.h"


struct EventWriterConfig {
  std::string src_filename, cspad_dset;
  int num_events_to_cycle, num_streams;
  void set_srcfilename(std::string _src_filename) { src_filename=_src_filename; }
  void set_cspad_dset(std::string _cspad_dset) { cspad_dset=_cspad_dset; }
  void set_number_of_src_events_to_cycle_through(int arg) {num_events_to_cycle=arg; } 
  void set_number_of_streams(int arg) {num_streams=arg; };
};


typedef short CSPADDATA[32][185][388];

class EventWriter {
  EventWriterConfig m_config;
  hid_t m_src_file;
  std::vector<CSPADDATA> cspads;
public:
  EventWriter(EventWriterConfig _config);
  void loadData();
  void openStreamFiles();
  void writeData();
  void closeStreamFiles();
  int run();
};



EventWriter::EventWriter(EventWriterConfig config) : m_config(config) {
}


void EventWriter::loadData() {
  m_src_file = NONNEG( H5Fopen(m_config.src_filename.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT) );
  hid_t cspad_dset = NONNEG( H5Dopen2( m_src_file, 
                                       m_config.cspad_dset.c_str(), H5P_DEFAULT) );
  
  NONNEG( H5Dclose( cspad_dset ) );
  NONNEG( H5Fclose( m_src_file ) );
}


void EventWriter::openStreamFiles() {
}


void EventWriter::writeData() {
}


void EventWriter::closeStreamFiles() {
}

int EventWriter::run() {
  loadData();
  openStreamFiles();
  writeData();
  closeStreamFiles();
  return 0;
}


int main() {
  EventWriterConfig config;
  config.set_srcfilename(std::string("/reg/d/psdm/cxi/cxitut13/hdf5/cxitut13-r0020.h5"));
  config.set_cspad_dset(std::string("/Configure:0000/Run:0000/CalibCycle:0000/CsPad::ElementV2/CxiDs1.0:Cspad.0/data"));
  config.set_number_of_src_events_to_cycle_through(10);
  config.set_number_of_streams(6);
  EventWriter eventWriter(config);
  return eventWriter.run();
}
