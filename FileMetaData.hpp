// FileMetadata.hpp
#ifndef FILEMETADATA_HPP
#define FILEMETADATA_HPP

#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <cstring>
#include <fstream>
#include <filesystem>
#include <stdexcept>

namespace fs = std::filesystem;

class FileMetadata {
private:
    static const int SCHEMA_SIZE = 512;        // Fixed size for schema
    static const int RESERVED_SIZE = 508;     // Reserved for future use
    static const int METADATA_SIZE = 8192;    // Total metadata size (8 KB)

    std::map<std::string, std::string> schema; // Maps attribute name to its type (e.g., "id" -> "int")
    uint16_t pageCount = 0;
    char reserved[RESERVED_SIZE] = {0};       // Reserved for future features
    std::map<int, int> tupleToPageMap;
    uint32_t nextPageID = 1;                  // Tracks the next page ID

public:
    FileMetadata();

    void setSchema(const std::map<std::string, std::string>& tableSchema);
    void setPageCount(uint16_t count);
    uint32_t getNextPageID() const;
    void incrementPageID();
    void addTupleToPageMap(int tupleId, int pageId);
    void removeTupleFromPageMap(int tupleId);
    bool hasTupleInPageMap(int tupleID) const;
    const std::map<std::string, std::string>& getSchema() const;
    uint16_t getPageCount() const;
    const std::map<int, int>& getTupleToPageMap() const;
    int getPageIDForTuple(int tupleID) const;
    std::streampos getPagePosition(int pageID) const;
    void setTupleAsDeleted(int tupleID);
    bool hasTupleWithID(int tupleID) const;
    void serialize(std::fstream& dbFile);
    void deserialize(std::fstream& file);
    void printMetadata() const;
};

#endif // FILEMETADATA_HPP
