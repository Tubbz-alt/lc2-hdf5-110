#ifndef LC2DAQ_HH
#define LC2DAQ_HH

#define CHECK_NONNEG(x , msg) check_nonneg(x, msg, __LINE__, __FILE__)
void check_nonneg(int, const char *, int, const char *);

struct DsetInfo {
  hid_t dset_id;
  hsize_t extent;
  DsetInfo(hid_t _dset_id, hsize_t _extent) : dset_id(_dset_id), extent(_extent) {}
  DsetInfo() : dset_id(-1), extent(-1) {}
  DsetInfo(const DsetInfo &o) :dset_id(o.dset_id), extent(o.extent) {}
  DsetInfo &operator=(const DsetInfo &o) { dset_id = o.dset_id; extent = o.extent; return *this; }
};

DsetInfo create_1d_dataset(hid_t, const char *, hid_t, hsize_t, size_t);
DsetInfo create_3d_dataset(hid_t, const char *, hid_t, int, int, int, size_t);
void append_to_1d_dset(DsetInfo &, long);
hsize_t append_many_to_1d_dset(DsetInfo &, hsize_t, long *);
void append_to_3d_dset(DsetInfo &, int, int, short *);

int foo();

#endif // LC2DAQ_HH
