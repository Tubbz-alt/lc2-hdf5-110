#include <iostream>
#include <sstream>

#include "H5OpenObjects.h"

H5OpenObjects::H5OpenObjects(hid_t file) {
  FILE = H5Fget_obj_count(file, H5F_OBJ_FILE | H5F_OBJ_LOCAL);
  DATASET = H5Fget_obj_count(file, H5F_OBJ_DATASET | H5F_OBJ_LOCAL);
  GROUP = H5Fget_obj_count(file, H5F_OBJ_GROUP | H5F_OBJ_LOCAL);
  ATTR = H5Fget_obj_count(file, H5F_OBJ_ATTR | H5F_OBJ_LOCAL);
  DATATYPE = H5Fget_obj_count(file, H5F_OBJ_DATATYPE | H5F_OBJ_LOCAL);
  ALL = FILE + DATASET + GROUP + ATTR + DATATYPE;
  setOpen(file, openFile,     FILE,     H5F_OBJ_FILE);
  setOpen(file, openGroup,    GROUP,    H5F_OBJ_GROUP);
  setOpen(file, openDatatype, DATATYPE, H5F_OBJ_DATATYPE);
  setOpen(file, openAttr,     ATTR,     H5F_OBJ_ATTR);
  setOpen(file, openDataset,  DATASET,  H5F_OBJ_DATASET);
}
    
std::string H5OpenObjects::dumpStr(bool detailed) {
  std::ostringstream msg;
  msg << "GROUP=" << GROUP
      << " DATASET=" << DATASET
      << " ATTR=" << ATTR
      << " DATATYPE=" << DATATYPE
      << " FILE=" << FILE
      << " ALL=" << ALL;
  if (detailed) {
    msg << std::endl;
    msg << reportOpenNames(openFile, "File");
    msg << reportOpenNames(openGroup, "Group");
    msg << reportOpenNames(openDataset, "Dataset");
    msg << reportOpenNames(openDatatype, "Datatype");
    msg << reportOpenNames(openAttr, "Attr");
  }
  return msg.str();
}
    
void H5OpenObjects::closeOpenNonFileIds() {
  closeIds(openGroup, "Group");
  closeIds(openDataset, "Dataset");
  closeIds(openDatatype, "Datatype");
  closeIds(openAttr, "Attr");
}

void H5OpenObjects::closeIds(std::vector<hid_t> &ids, const char *label) {
  for (unsigned idx = 0; idx < ids.size(); ++idx) {
    hid_t id = ids[idx];
    if (H5Iis_valid(id) > 0) {
      herr_t err = 0;
      switch(H5Iget_type(id)) {
      case H5I_FILE:
      case H5I_BADID:
        std::cerr << "WARNING: H5OpenObjects::closeIds - " << label << " id=" << id << " is file or badid" << std::endl;
        break;
      case H5I_GROUP:
        err = H5Gclose(id);
        break;
      case H5I_DATATYPE:
        err = H5Tclose(id);
        break;
      case H5I_DATASPACE:
        err = H5Sclose(id);
        break;
      case H5I_DATASET:
        err = H5Dclose(id);
        break;
      case H5I_ATTR:
        err = H5Aclose(id);
        break;
      default:
        break;
      } // switch
      if (err<0) std::cerr << "ERROR: H5OpenObjects::closeIds - " << label << " error closing id=" << id << std::endl;
    } else {
      std::cerr << "WARNING: H5OpenObjects::closeIds - " << label << " id=" << id << " is not valid" << std::endl;
    } 
  }
}
    
std::string H5OpenObjects::reportOpenNames(std::vector<hid_t> &ids, const char *label) {
  const int NAMELEN = 512;
  char name[NAMELEN];
  std::ostringstream msg;
  msg << "**" << label << "**" << std::endl;
  for (unsigned idx = 0; idx < ids.size(); ++idx) {
    msg << "  id=" << ids[idx] << " name=";
    ssize_t ret = H5Iget_name(ids[idx], name, NAMELEN);
    if (ret < 0) msg << " --error-- bad identifier";
    else if (ret == 0) msg << " -- no name associated with identifier --";
    else msg << name;
    msg << std::endl;
  }
  return msg.str();
}

void H5OpenObjects::setOpen(hid_t fileid, std::vector<hid_t> &toFill, ssize_t count, unsigned types) {
  if (count == 0) return;
  toFill.resize(count);
  hid_t *obj_id_list = &(toFill[0]);
  if (obj_id_list == 0) std::cerr << " ERROR: null obj id list?" << std::endl;
  ssize_t filled = H5Fget_obj_ids( fileid, types, count, &toFill[0]);
  if (filled != count) std::cerr << "ERROR: H5OpenObjects::setOpen did not fill expected " 
                                 << count << " for type " << types << " it filled: " << filled << std::endl;
}
