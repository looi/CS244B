#include <sstream>

#include "gfs_common.h"

std::string FormatStatus(const Status& status) {
  if (status.ok()) {
    return "OK";
  }
  std::ostringstream ss;
  ss << "(" << status.error_code() << ": " << status.error_message() << ")";
  return ss.str();
}
