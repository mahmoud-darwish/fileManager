#ifndef PAGE_HPP
#define PAGE_HPP

#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <cstring>
#include <map>
#include "tuple.hpp"

#include"FileMetaData.hpp"
constexpr size_t PAGE_SIZE = 4096; // 4 KB

struct Slot {
    uint16_t offset;
    uint16_t length;
};

struct PageMetadata {
    uint16_t pageID;
    uint16_t slotCount;
    uint16_t freeSpace;
    uint16_t freeSpaceEnd;
};

class Page {
private:
    PageMetadata metadata;
    std::vector<Slot> slots;
    char data[PAGE_SIZE];

public:
    Page(uint16_t id);

    uint32_t getPageID() const;
    size_t getFreeSpace() const;
    uint16_t getTupleCount() const;
    const std::vector<Slot>& getSlots() const;
    Slot getSlot(size_t index) const;

    bool addTuple(const std::string& tuple, FileMetadata& fileMetadata, int tupleId);
    void serialize(std::fstream& dbFile);
    void deserialize(std::fstream& dbFile);
    std::string getTupleIndex(const std::string& tablePath, uint16_t tupleID);
    std::string getTupleData(uint16_t index)const;
    bool deleteTuple(uint16_t slotIndex, int tupleID, const std::string& tablePath) ;
    int getTupleIndexByID(const std::string& id) const;


};

#endif // PAGE_HPP
