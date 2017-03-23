#include "DsetInfo.h"
#include "check_macros.h"

// DsetInfo
void DsetInfo::close() {
  NONNEG( H5Dclose( dset_id() ) );
}

// DsetWriterInfo
void DsetWriterInfo::close() {
  DsetInfo::close();
}

DsetWriterInfo &DsetWriterInfo::operator=( DsetWriterInfo &o) {
  DsetInfo::operator=(o);
  return *this;
}

DsetWriterInfo::DsetWriterInfo(hid_t _dset_id, std::vector<hsize_t> _dim) :
  DsetInfo(_dset_id, _dim)
{
}

DsetWriterInfo::DsetWriterInfo(hid_t _dset_id, hsize_t _extent) :
  DsetInfo(_dset_id, _extent) {
}

// DsetReaderInfo
void DsetReaderInfo::close() {
  if (m_mem_space_one_event > -1) {
    NONNEG( H5Sclose( m_mem_space_one_event) );
  }
  if (m_file_space_id > -1) {
    NONNEG( H5Sclose( m_file_space_id) );
  }
  DsetInfo::close();
}

DsetReaderInfo &DsetReaderInfo::operator=( DsetReaderInfo &o) {
  DsetInfo::operator=(o);
  m_mem_space_one_event = o.m_mem_space_one_event;
  m_file_space_id = o.m_file_space_id;
  return *this;
}

void DsetReaderInfo::dim(const std::vector<hsize_t> &new_dim) {
  if (dset_id() < 0) throw std::runtime_error("DsetReaderInfo::dim - invalid dset_id");
  if (file_space_id() < 0) throw std::runtime_error("DsetReaderInfo::dim - invalid file_space_id");
  DsetInfo::dim(dim);
  NONNEG( H5set_extent_simple( m_file_space_id, dim().size(), dim().at(0), NULL) );
}

void DsetReaderInfo::select_file_space_id(int64_t event_index) {
  std::vector<hsize_t> start(dim()), count(dim().size(), 1), block(dim());
  start.at(0)=event_index;
  block.at(0)=1;
  NONNEG( H5Sselect_hyperslab( H5S_SELECT_SET, &start.at(0), &count.at(0), &block.at(0) ) );
}
