#ifndef H5_OPEN_OBJECTS_HH
#define H5_OPEN_OBJECTS_HH

#include "hdf5.h"
#include <vector>
#include <string>

struct H5OpenObjects {

  std::vector<hid_t> openFile, openDataset, openGroup, openDatatype, openAttr;
  ssize_t FILE, DATASET, GROUP, DATATYPE, ATTR, ALL;
  
  H5OpenObjects(hid_t file);
  std::string dumpStr(bool detailed=false);
  void closeOpenNonFileIds();

private:

  void closeIds(std::vector<hid_t> &ids, const char *label);
  std::string reportOpenNames(std::vector<hid_t> &ids, const char *label);
  void setOpen(hid_t fileid, std::vector<hid_t> &toFill, ssize_t count, unsigned types);

}; 

#endif  // H5_OPEN_OBJECTS_HH
