// FileMetadata.cpp
#include "FileMetaData.hpp"
#include "storage.hpp"
FileMetadata::FileMetadata() {
    std::memset(reserved, 0, RESERVED_SIZE);
}

void FileMetadata::setSchema(const std::map<std::string, std::string>& tableSchema) {
    schema = tableSchema;
}

void FileMetadata::setPageCount(uint16_t count) {
    pageCount = count;
}

uint32_t FileMetadata::getNextPageID() const {
    return nextPageID;
}

void FileMetadata::incrementPageID() {
    if (nextPageID >= pageCount) {
        std::cerr << "Warning incrementPageID: Attempting to access invalid page ID: " << nextPageID << "\n";
        pageCount++;
    }
    nextPageID++;
}

void FileMetadata::addTupleToPageMap(int tupleId, int pageId) {
    if (tupleToPageMap.find(tupleId) != tupleToPageMap.end()) {
        std::cerr << "Warning addTupleToPageMap: Overwriting existing mapping for Tuple ID " << tupleId << ".\n";
    }
    tupleToPageMap[tupleId] = pageId;
}

void FileMetadata::removeTupleFromPageMap(int tupleId) {
    tupleToPageMap[tupleId] = -2; // Mark as deleted
}

bool FileMetadata::hasTupleInPageMap(int tupleID) const {
    auto it = tupleToPageMap.find(tupleID);
    return it != tupleToPageMap.end() && it->second != -1 && it->second != -2;
}

const std::map<std::string, std::string>& FileMetadata::getSchema() const {
    return schema;
}

uint16_t FileMetadata::getPageCount() const {
    return pageCount;
}

const std::map<int, int>& FileMetadata::getTupleToPageMap() const {
    return tupleToPageMap;
}

int FileMetadata::getPageIDForTuple(int tupleID) const {
    auto it = tupleToPageMap.find(tupleID);
    if (it == tupleToPageMap.end()) return -1; // Tuple does not exist
    if (it->second == -1 || it->second == -2) return -2; // Tuple is deleted
    return it->second;
}

std::streampos FileMetadata::getPagePosition(int pageID) const {
    if (pageID < 0 || pageID > pageCount) {
        std::cerr << "Error getPagePosition: Invalid pageID: " << pageID << " (pageCount: " << pageCount << ")\n";
        throw std::out_of_range("Invalid pageID: " + std::to_string(pageID));
    }
    return std::streampos(METADATA_SIZE + (pageID * 4096));
}

void FileMetadata::setTupleAsDeleted(int tupleID) {
    tupleToPageMap[tupleID] = -2;
    std::cout << "[DEBUG setTupleAsDeleted] Tuple " << tupleID << " marked as deleted." << std::endl;
}

bool FileMetadata::hasTupleWithID(int tupleID) const {
    auto it = tupleToPageMap.find(tupleID);
    if (it == tupleToPageMap.end()) {
        std::cout << "[DEBUG hasTupleWithID] Tuple " << tupleID << " not found in the map." << std::endl;
        return false;
    }
    if (it->second == -1 || it->second == -2) {
        std::cout << "[DEBUG hasTupleWithID] Tuple " << tupleID << " is marked as deleted." << std::endl;
        return false;
    }
    return true;
}

void FileMetadata::serialize(std::fstream& dbFile) {
    if (!dbFile.is_open() || !dbFile) {
        dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }
    }

    try {
        uint16_t schemaSize = schema.size();
        dbFile.write(reinterpret_cast<char*>(&schemaSize), sizeof(schemaSize));
        for (const auto& [key, value] : schema) {
            uint16_t keySize = key.size();
            uint16_t valueSize = value.size();
            dbFile.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
            dbFile.write(key.c_str(), keySize);
            dbFile.write(reinterpret_cast<const char*>(&valueSize), sizeof(valueSize));
            dbFile.write(value.c_str(), valueSize);
        }

        dbFile.write(reinterpret_cast<char*>(&pageCount), sizeof(pageCount));
        dbFile.write(reserved, RESERVED_SIZE);

        uint16_t mapSize = tupleToPageMap.size();
        dbFile.write(reinterpret_cast<char*>(&mapSize), sizeof(mapSize));
        for (const auto& [tupleId, pageId] : tupleToPageMap) {
            dbFile.write(reinterpret_cast<const char*>(&tupleId), sizeof(tupleId));
            dbFile.write(reinterpret_cast<const char*>(&pageId), sizeof(pageId));
        }

        std::cout << "[DEBUG File Metadata serialize] FileMetadata serialized successfully.\n";

    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("Serialization failed: ") + e.what());
    }
}

void FileMetadata::deserialize(std::fstream& file) {
    if (!file.is_open() || !file) {
        file.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!file.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }   
         }

    try {
        uint16_t schemaSize;
        file.read(reinterpret_cast<char*>(&schemaSize), sizeof(schemaSize));
        schema.clear();
        for (uint16_t i = 0; i < schemaSize; ++i) {
            uint16_t keySize, valueSize;
            file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
            std::string key(keySize, '\0');
            file.read(&key[0], keySize);

            file.read(reinterpret_cast<char*>(&valueSize), sizeof(valueSize));
            std::string value(valueSize, '\0');
            file.read(&value[0], valueSize);

            schema[key] = value;
        }

        file.read(reinterpret_cast<char*>(&pageCount), sizeof(pageCount));
        file.read(reserved, RESERVED_SIZE);

        uint16_t mapSize;
        file.read(reinterpret_cast<char*>(&mapSize), sizeof(mapSize));
        tupleToPageMap.clear();
        for (uint16_t i = 0; i < mapSize; ++i) {
            int tupleId, pageId;
            file.read(reinterpret_cast<char*>(&tupleId), sizeof(tupleId));
            file.read(reinterpret_cast<char*>(&pageId), sizeof(pageId));
            tupleToPageMap[tupleId] = pageId;
        }

        std::cout << "[DEBUG File Metadata deserialize] FileMetadata deserialized successfully.\n";

    } catch (const std::exception& e) {
        throw std::runtime_error(std::string("File Metadata Deserialization failed: ") + e.what());
    }
}

void FileMetadata::printMetadata() const {
    std::cout << "=== File Metadata ===\n";

    std::cout << "Schema:\n";
    if (schema.empty()) {
        std::cout << "  (Schema is empty)\n";
    } else {
        for (const auto& [key, value] : schema) {
            std::cout << "  " << key << ": " << value << "\n";
        }
    }

    std::cout << "Page Count: " << pageCount << "\n";
    std::cout << "Reserved Space: " << RESERVED_SIZE << " bytes\n";

    std::cout << "Tuple-to-Page Map:\n";
    if (tupleToPageMap.empty()) {
        std::cout << "  (Map is empty)\n";
    } else {
        for (const auto& [tupleId, pageId] : tupleToPageMap) {
            std::cout << "  Tuple ID: " << tupleId
                      << ", Page ID: " << (pageId == -1 ? "(Deleted)" : std::to_string(pageId)) << "\n";
        }
    }

    std::cout << "=====================\n";
}
