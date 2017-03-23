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
  if (file_space_id() > -1) {
    NONNEG( H5Sclose( file_space_id() ) );
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
  if (file_space_id() < 0) {
    file_space_id(  NONNEG( H5Screate( H5S_SIMPLE ) ) );
  }
  DsetInfo::dim(new_dim);
  NONNEG( H5Sset_extent_simple( file_space_id(), dim().size(), &dim().at(0), NULL) );
}

void DsetReaderInfo::select_file_space_id(int64_t event_index) {
  std::vector<hsize_t> start(dim().size(), 0);
  std::vector<hsize_t> count(dim().size(), 1);
  std::vector<hsize_t> stride(dim().size(), 1);
  std::vector<hsize_t> block(dim());

  start.at(0)=event_index;
  block.at(0)=1;

  NONNEG( H5Sselect_hyperslab( file_space_id(),
                               H5S_SELECT_SET,
                               &start.at(0), &count.at(0),
                               &stride.at(0), &block.at(0) ) );
}
