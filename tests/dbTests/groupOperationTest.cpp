#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>
#include <filesystem>
#include <tuple>

#include "src/utils/yamlConfig.hpp"
#include "src/utils/grpcConfig.hpp"
#include "src/db/dbConnector.hpp"
#include "leveldb/db.h"

class groupOperationTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::string fileName = config.getDbFile();
        std::filesystem::remove_all(fileName);
    }

    void TearDown() override {
        std::string fileName = config.getDbFile();
        std::filesystem::remove_all(fileName);
    }

    YAMLConfig config = YAMLConfig("resources/config.yaml");
    dbConnector db = dbConnector(config);
};

TEST_F(groupOperationTest, baseGroupInsert) {
    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(12, 1), dbConnector::generateNormalKey("ab", 1), "val"},
        {dbConnector::generateLseqKey(15, 1), dbConnector::generateNormalKey("ab2", 1), "val2"},
        {dbConnector::generateLseqKey(16, 1), dbConnector::generateNormalKey("abc", 1), "valc"},
        {dbConnector::generateLseqKey(19, 3), dbConnector::generateNormalKey("ab", 3), "val3"}
    }).ok());
    EXPECT_EQ(std::get<2>(db.get("ab", 1)), "val");
    EXPECT_EQ(std::get<2>(db.get("ab2", 1)), "val2");
    EXPECT_EQ(std::get<2>(db.get("abc", 1)), "valc");
    EXPECT_EQ(std::get<2>(db.get("ab", 3)), "val3");
    EXPECT_EQ(16, db.sequenceNumberForReplica(1));
    EXPECT_EQ(19, db.sequenceNumberForReplica(3));
    EXPECT_TRUE(std::get<1>(db.get("ab", 2)).IsNotFound());
}

TEST_F(groupOperationTest, lseqSeekNormalPut) {
    std::string firstLseq = db.put("valuekey", "valuevalue").first;

    replyBatchFormat repl = db.getByLseq(dbConnector::lseqToSeq(firstLseq), 2, 1);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 1);
    EXPECT_EQ(std::get<2>(repl.second[0]), "valuevalue");

    repl = db.getByLseq(firstLseq, 1);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 1);
    EXPECT_EQ(std::get<2>(repl.second[0]), "valuevalue");

    repl = db.getByLseq(firstLseq);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_GE(repl.second.size(), 1);
    EXPECT_EQ(std::get<2>(repl.second[0]), "valuevalue");

    std::string secondLseq = db.put("valuekey2", "valuevalue2").first;

    repl = db.getByLseq(dbConnector::lseqToSeq(firstLseq), 2, 2);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 2);
    EXPECT_EQ(std::get<2>(repl.second[0]), "valuevalue");
    EXPECT_EQ(std::get<2>(repl.second[1]), "valuevalue2");

    repl = db.getByLseq(firstLseq, 2);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 2);
    EXPECT_EQ(std::get<2>(repl.second[0]), "valuevalue");
    EXPECT_EQ(std::get<2>(repl.second[1]), "valuevalue2");

    repl = db.getByLseq(secondLseq, 1);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 1);
    EXPECT_EQ(std::get<2>(repl.second[0]), "valuevalue2");

    repl = db.getByLseq(0, 2, 1);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_GE(repl.second.size(), 2);
}

TEST_F(groupOperationTest, lseqSeek) {
    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(100, 2), dbConnector::generateNormalKey("abc", 2), "val"},
        {dbConnector::generateLseqKey(200, 2), dbConnector::generateNormalKey("abc2", 2), "val2"},
        {dbConnector::generateLseqKey(300, 2), dbConnector::generateNormalKey("abcc", 2), "val3"},
        {dbConnector::generateLseqKey(400, 2), dbConnector::generateNormalKey("abcd", 2), "val4"}
    }).ok());
    replyBatchFormat repl = db.getByLseq(100, 2);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 4);
    EXPECT_EQ(std::get<2>(repl.second[0]), "val");
    EXPECT_EQ(std::get<2>(repl.second[1]), "val2");
    EXPECT_EQ(std::get<2>(repl.second[2]), "val3");
    EXPECT_EQ(std::get<2>(repl.second[3]), "val4");

    repl = db.getByLseq(101, 2);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 3);
    EXPECT_EQ(std::get<2>(repl.second[0]), "val2");
    EXPECT_EQ(std::get<2>(repl.second[1]), "val3");
    EXPECT_EQ(std::get<2>(repl.second[2]), "val4");

    repl = db.getByLseq(100, 2, -1, dbConnector::LSEQ_COMPARE::GREATER);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 3);
    EXPECT_EQ(std::get<2>(repl.second[0]), "val2");
    EXPECT_EQ(std::get<2>(repl.second[1]), "val3");
    EXPECT_EQ(std::get<2>(repl.second[2]), "val4");

    repl = db.getByLseq(100, 1);
    EXPECT_TRUE(repl.first.ok());
    for (auto res : repl.second) {
        std::cerr << std::get<0>(res) << " " << std::get<1>(res) << " " << std::get<2>(res) << std::endl;
    }
    EXPECT_EQ(repl.second.size(), 0);
}

TEST_F(groupOperationTest, lseqSeekSnapshot) {
    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(100, 2), dbConnector::generateNormalKey("abc", 2), "val"},
        {dbConnector::generateLseqKey(200, 2), dbConnector::generateNormalKey("abc2", 2), "val2"},
    }).ok());

    auto snapshotResp = db.createSnapshot();
    EXPECT_TRUE(snapshotResp.second.ok());
    auto snapshot = db.getSnapshot(snapshotResp.first).first;

    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(300, 2), dbConnector::generateNormalKey("abcc", 2), "val3"},
        {dbConnector::generateLseqKey(400, 2), dbConnector::generateNormalKey("abcd", 2), "val4"}
    }).ok());

    replyBatchFormat repl = db.getByLseq(dbConnector::generateLseqKey(100, 2), -1, dbConnector::LSEQ_COMPARE::GREATER_EQUAL, snapshot);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 2);
    EXPECT_EQ(std::get<2>(repl.second[0]), "val");
    EXPECT_EQ(std::get<2>(repl.second[1]), "val2");

    repl = db.getByLseq(dbConnector::generateLseqKey(200, 2), -1, dbConnector::LSEQ_COMPARE::GREATER, snapshot);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 0);

    auto snapshotResp2 = db.createSnapshot();
    EXPECT_TRUE(snapshotResp2.second.ok());
    auto snapshot2 = db.getSnapshot(snapshotResp2.first).first;

    repl = db.getByLseq(dbConnector::generateLseqKey(100, 2), -1, dbConnector::LSEQ_COMPARE::GREATER_EQUAL, snapshot2);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 4);
    EXPECT_EQ(std::get<2>(repl.second[0]), "val");
    EXPECT_EQ(std::get<2>(repl.second[1]), "val2");
    EXPECT_EQ(std::get<2>(repl.second[2]), "val3");
    EXPECT_EQ(std::get<2>(repl.second[3]), "val4");
}

TEST_F(groupOperationTest, groupKeyGet) {
    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(1000, 2), dbConnector::generateNormalKey("abcde", 2), "val"},
        {dbConnector::generateLseqKey(2000, 2), dbConnector::generateNormalKey("abcde", 2), "val2"},
        {dbConnector::generateLseqKey(3000, 2), dbConnector::generateNormalKey("abcde", 2), "val3"},
        {dbConnector::generateLseqKey(1200, 2), dbConnector::generateNormalKey("abcf", 2), "val4"},
        {dbConnector::generateLseqKey(1500, 3), dbConnector::generateNormalKey("abcde", 2), "val5"}
    }).ok());

    replyBatchFormat repl = db.getAllValuesForKey("abcde", 0);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 4);
    EXPECT_EQ(std::get<2>(repl.second[0]), "val");
    EXPECT_EQ(std::get<2>(repl.second[1]), "val5");
    EXPECT_EQ(std::get<2>(repl.second[2]), "val2");
    EXPECT_EQ(std::get<2>(repl.second[3]), "val3");

    repl = db.getValuesForKey("abcde", 1001, 12);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 3);
    EXPECT_EQ(std::get<2>(repl.second[0]), "val5");
    EXPECT_EQ(std::get<2>(repl.second[1]), "val2");
    EXPECT_EQ(std::get<2>(repl.second[2]), "val3");
}

TEST_F(groupOperationTest, groupKeyGetSnapshot) {
    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(1000, 2), dbConnector::generateNormalKey("abcde", 2), "val"},
        {dbConnector::generateLseqKey(2000, 2), dbConnector::generateNormalKey("abcde", 2), "val2"},
        {dbConnector::generateLseqKey(3000, 2), dbConnector::generateNormalKey("abcde", 2), "val3"},
        {dbConnector::generateLseqKey(4000, 2), dbConnector::generateNormalKey("abcf", 2), "val4"},
    }).ok());

    auto snapshotResp = db.createSnapshot();
    EXPECT_TRUE(snapshotResp.second.ok());
    auto snapshot = db.getSnapshot(snapshotResp.first).first;

    EXPECT_TRUE(db.putBatch({
        {dbConnector::generateLseqKey(5000, 3), dbConnector::generateNormalKey("abcde", 2), "val5"}
    }).ok());

    replyBatchFormat repl = db.getValuesForKey("abcde", 0, 0, -1, dbConnector::LSEQ_COMPARE::GREATER_EQUAL, snapshot);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 3);
    EXPECT_EQ(std::get<2>(repl.second[0]), "val");
    EXPECT_EQ(std::get<2>(repl.second[1]), "val2");
    EXPECT_EQ(std::get<2>(repl.second[2]), "val3");

    repl = db.getValuesForKey("abcde", 3000, 0, -1, dbConnector::LSEQ_COMPARE::GREATER, snapshot);
    EXPECT_TRUE(repl.first.ok());
    EXPECT_EQ(repl.second.size(), 0);
}