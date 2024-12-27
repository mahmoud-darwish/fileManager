#ifndef STORAGE_HPP
#define STORAGE_HPP

#include <iostream>
#include <fstream>
#include <map>
#include <vector>
#include <string>
#include <filesystem>
#include "page.hpp"
#include "FileMetaData.hpp"
#include "tuple.hpp"

namespace fs = std::filesystem;

class Storage {

private:
    std::vector<Page> pages;
    uint32_t nextPageID = 1; // Unique page ID counter

    std::map<std::string, std::map<std::string, std::map<std::string, Tuple>>> databases;

public:
    static std::string tablePath;
    bool createDatabase(const std::string& dbName);
    bool tableExists(const std::string& dbName, const std::string& tableName);
    bool createTable(const std::string& dbName, const std::string& tableName, const std::map<std::string, std::string>& schema);
    bool deleteTable(const std::string& tablePath);
    Page loadPageByID(const std::string& tablePath, uint32_t pageID);
    std::vector<Tuple> getTuplesFromPage(const Page& page);
    std::string loadTuple(const std::string& tablePath, uint16_t tupleID);
    std::map<std::string, std::string> get(const std::string& dbName, const std::string& tableName, const std::string& id);
    bool addTupleToTable(const std::string& dbName, const std::string& tableName, const std::string& tupleSerialized, int id);
    bool checkTupleExists(const std::string& dbName, const std::string& tableName, const std::string& id);
    bool insert(const std::string& dbName, const std::string& tableName, const Tuple& tuple);
    bool deleteTupleFromTable(const std::string& dbName, const std::string& tableName, const std::string& id);
    bool updateTupleInTable(const std::string& dbName, const std::string& tableName, const std::string& id, const Tuple& updatedTuple);
};

#endif // STORAGE_HPP
