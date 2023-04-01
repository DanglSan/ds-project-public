#include "leveldb/db.h"
#include <cstddef>
#include <cstdlib>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

struct snapshot {
  static const char DELIMITER = ',';

  static std::string serialize(const std::vector<leveldb::SequenceNumber>& seqCount) {
    std::stringstream ss;
    for (auto seq : seqCount) {
      ss << std::to_string(seq) << DELIMITER;
    }
    return ss.str();
  }

  static std::optional<std::vector<leveldb::SequenceNumber>> parse(const std::string serializedSeqCount) {
    std::vector<leveldb::SequenceNumber> seqCount;

    std::istringstream split(serializedSeqCount);
    for (std::string each; std::getline(split, each, DELIMITER);) {
      seqCount.emplace_back(std::stoull(each));
    }

    return seqCount;
  }
};
