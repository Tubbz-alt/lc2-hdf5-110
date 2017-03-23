#ifndef ANA_DAQ_UTIL_HH
#define ANA_DAQ_UTIL_HH

#include <cstdio>
#include <vector>
#include <chrono>
#include <cinttypes>

#include "hdf5.h"
#include "DsetInfo.h"

typedef std::chrono::high_resolution_clock Clock;
typedef std::map<int, hid_t>::const_iterator CMapIter;

const int CSPadDim1 = 32;
const int CSPadDim2 = 185;
const int CSPadDim3 = 388;
const int CSPadNumElem = CSPadDim1*CSPadDim2*CSPadDim3;

DsetWriterInfo create_1d_int64_dataset(hid_t parent, const char * dset, hsize_t chunk_size);
DsetWriterInfo create_4d_int16_dataset(hid_t parent, const char * dset,
                                       hsize_t dim1, hsize_t dim2, hsize_t dim3, hsize_t chunk_size);

void append_to_1d_int64_dset(DsetWriterInfo & dset_info, int64_t value);
void append_to_4d_int16_dset(DsetWriterInfo & dset_info, int16_t *data);
void append_many_to_1d_int64_dset(DsetWriterInfo & dset_info, hsize_t count, int64_t *data);

bool wait_for_dataset_to_grow(hid_t dset_id, hsize_t * dims, hsize_t len_to_grow_to, 
                              int microseconds_to_pause=0, int timeout_seconds=-1.0);


DsetReaderInfo createReaderDsetInfo(hid_t fid, const char *dset_path, int num_events_in_dataset_chunk_cache);
int64_t read_int64_from_1d(DsetReaderInfo &info, int64_t event_index);

#endif // ANA_DAQ_UTIL_HH
