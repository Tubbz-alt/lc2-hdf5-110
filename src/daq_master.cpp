#include <iostream>
#include "hdf5.h"

#include "ana_daq_util.h"

int main() {
  std::cout << "daq_master: foo=" << foo() << std::endl;
  return 0;
}
