#pragma once

#include <optional>
#include <string>
#include <mutex>
#include <utility>
#include <vector>
#include <variant>

#include "leveldb/db.h"
#include "src/utils/yamlConfig.hpp"
#include "src/db/comparator.hpp"

//TODO maybe refactor to use different structs
using lseqType = std::string;
using keyType = std::string;
using valueType = std::string;

using replyFormat = std::pair<lseqType, leveldb::Status>;
using pureReplyValue = std::tuple<lseqType, leveldb::Status, valueType>;
using batchValues = std::vector<std::tuple<lseqType, keyType, valueType>>;
using replyBatchFormat = std::pair<leveldb::Status, batchValues>;

using snapshotIdType = std::string;
using snapshotType = std::vector<leveldb::SequenceNumber>;
using getSnapshotReplyFormat = std::pair<snapshotType, leveldb::Status>;
using createSnapshotReplyFormat = std::pair<snapshotIdType, leveldb::Status>;


class dbConnector {
public:

    enum class LSEQ_COMPARE {GREATER_EQUAL, GREATER};

    explicit dbConnector(YAMLConfig config);

    dbConnector(const dbConnector&) = delete;

    dbConnector(dbConnector&&) = delete;

    ~dbConnector();

    replyFormat put(std::string key, std::string value);

    replyFormat remove(std::string key);

    pureReplyValue get(std::string key);

    pureReplyValue get(std::string key, int id);

    pureReplyValue get(std::string key, const snapshotType& snapshot);

    pureReplyValue get(std::string key, int id, const snapshotType& snapshot);

    leveldb::Status putBatch(const batchValues& keyValuePairs);

    replyBatchFormat getByLseq(leveldb::SequenceNumber seq, int id, int limit = -1, LSEQ_COMPARE isGreater = LSEQ_COMPARE::GREATER_EQUAL);

    replyBatchFormat getByLseq(std::string lseq, int limit = -1, LSEQ_COMPARE isGreater = LSEQ_COMPARE::GREATER_EQUAL, const std::optional<snapshotType>& snapshot = std::nullopt);

    replyBatchFormat getValuesForKey(const std::string& key, leveldb::SequenceNumber seq, int id, int limit = -1, LSEQ_COMPARE isGreater = LSEQ_COMPARE::GREATER_EQUAL);

    replyBatchFormat getValuesForKey(const std::string& key, leveldb::SequenceNumber seq, int id,
        int limit, LSEQ_COMPARE isGreater, const snapshotType& snapshot);

    replyBatchFormat getAllValuesForKey(const std::string& key, int id, int limit = -1, LSEQ_COMPARE isGreater = LSEQ_COMPARE::GREATER_EQUAL);

    leveldb::SequenceNumber sequenceNumberForReplica(int id);

    createSnapshotReplyFormat createSnapshot();

    getSnapshotReplyFormat getSnapshot(const snapshotIdType& snapshot_id);

    static std::string generateGetseqKey(std::string realKey);

    static std::string generateLseqKey(leveldb::SequenceNumber seq, int id);

    static std::string stampedKeyToRealKey(const std::string& stampedKey);

    static std::string generateNormalKey(std::string key, int id);

    static std::string idToString(int id);

    static std::string lseqToReplicaId(const std::string& lseq);

    static leveldb::SequenceNumber lseqToSeq(const std::string& lseq);

    static std::string seqToString(leveldb::SequenceNumber seq);
    
    static snapshotIdType generateSnapshotId(int id);

protected:

    leveldb::SequenceNumber getMaxSeqForReplica(int id);

private:
    std::mutex mx;
    std::vector<leveldb::SequenceNumber> seqCount_;
    leveldb::DB* db{};

    int selfId;

};