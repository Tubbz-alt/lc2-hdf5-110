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
  if (m_mem_space_id > -1) {
    NONNEG( H5Sclose( m_mem_space_id) );
  }
  if (file_space_id() > -1) {
    NONNEG( H5Sclose( file_space_id() ) );
  }
  DsetInfo::close();
}

DsetReaderInfo::DsetReaderInfo( const DsetReaderInfo &o) :
  DsetInfo(o),
  m_mem_space_id(o.m_mem_space_id),
  m_file_space_id(o.m_file_space_id),
  m_file_select_start(o.m_file_select_start),
  m_file_select_count(o.m_file_select_count),
  m_file_select_stride(o.m_file_select_stride),
  m_file_select_block(o.m_file_select_block)
{
}

DsetReaderInfo &DsetReaderInfo::operator=( DsetReaderInfo &o) {
  DsetInfo::operator=(o);
  m_mem_space_id = o.m_mem_space_id;
  m_file_space_id = o.m_file_space_id;

  m_file_select_start = o.m_file_select_start;
  m_file_select_count = o.m_file_select_count;
  m_file_select_stride = o.m_file_select_stride;
  m_file_select_block = o.m_file_select_block;
  
  return *this;
}

void DsetReaderInfo::dim(const std::vector<hsize_t> &new_dim) {
  if (file_space_id() < 0) {
    file_space_id(  NONNEG( H5Screate( H5S_SIMPLE ) ) );
  }

  if (mem_space_id() < 0) {
    mem_space_id(  NONNEG( H5Screate( H5S_SIMPLE ) ) );
  }

  DsetInfo::dim(new_dim);

  m_file_select_start = std::vector<hsize_t>(new_dim.size(), 0);

  m_file_select_count = std::vector<hsize_t>(new_dim.size(), 1);
  
  m_file_select_stride = std::vector<hsize_t>(new_dim.size(), 1);

  m_file_select_block = new_dim;
  m_file_select_block.at(0)=1;
  
  NONNEG( H5Sset_extent_simple( file_space_id(), dim().size(), &dim().at(0), NULL) );

  NONNEG( H5Sset_extent_simple( mem_space_id(), dim().size(), &m_file_select_block.at(0), NULL) );
}

void DsetReaderInfo::file_space_select(int64_t event_index) {
  m_file_select_start.at(0)=event_index;
  NONNEG( H5Sselect_hyperslab( file_space_id(),
                               H5S_SELECT_SET,
                               &m_file_select_start.at(0),
                               &m_file_select_count.at(0),
                               &m_file_select_stride.at(0),
                               &m_file_select_block.at(0) ) );
}
