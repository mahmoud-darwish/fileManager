#include "page.hpp"
#include <iostream>
#include <stdexcept>
#include "storage.hpp"
Page::Page(uint16_t id) {
    metadata.pageID = id;
    metadata.slotCount = 0;
    metadata.freeSpace = PAGE_SIZE - sizeof(PageMetadata);
    metadata.freeSpaceEnd = PAGE_SIZE;
    std::memset(data, 0, PAGE_SIZE);
}

uint32_t Page::getPageID() const {
    return metadata.pageID;
}

size_t Page::getFreeSpace() const {
    return metadata.freeSpace;
}

uint16_t Page::getTupleCount() const {
    return metadata.slotCount;
}

const std::vector<Slot>& Page::getSlots() const {
    return slots;
}

Slot Page::getSlot(size_t index) const {
    if (index < slots.size()) {
        return slots[index];
    }
    throw std::out_of_range("Slot index out of range");
}

bool Page::addTuple(const std::string& tuple, FileMetadata* fileMetadata, int tupleId) {
    std::cout << "Debug addTuple: Attempting to add tuple. Free space: " << metadata.freeSpace
                << ", Tuple size: " << tuple.size() + sizeof(Slot) << std::endl;

        // Check if there's enough space for the tuple and slot metadata
        if (metadata.freeSpace < tuple.size() + sizeof(Slot)) {
            std::cout << "Debug addTuple: Not enough space to add tuple.\n";
            return false; // Not enough space
        }

        // Calculate the offset where the tuple will be placed
        uint16_t tupleOffset = metadata.freeSpaceEnd - tuple.size();
        // Debug: Log tuple offset calculation
        std::cout << "Debug addTuple: Calculating tuple offset. Free space end: " << metadata.freeSpaceEnd
              << ", Tuple size: " << tuple.size() << std::endl;


        // Safety check to prevent writing out of bounds
        if (tupleOffset < sizeof(PageMetadata) + slots.size() * sizeof(Slot)) {
            std::cerr << "Error addTuple: Not enough space for tuple and slot metadata.\n";
            return false;
        }

        // Insert the tuple into the page's data array
        std::memcpy(data + tupleOffset, tuple.c_str(), tuple.size());
        std::cout << "Debug addTuple: Tuple added at offset: " << tupleOffset << " with size: " << tuple.size() << std::endl;


        // Create a new slot for the tuplef
        Slot slot = {tupleOffset, static_cast<uint16_t>(tuple.size())};
        slots.push_back(slot);

        // Update page metadata
        metadata.freeSpaceEnd = tupleOffset;
        metadata.freeSpace -= (tuple.size() + sizeof(Slot));
        metadata.slotCount++;
        std::cout << "Debug addTuple: Slot count after adding tuple: " << metadata.slotCount << std::endl;


        // Update FileMetadata with the new tuple location
        fileMetadata->addTupleToPageMap(tupleId, metadata.pageID);
        std::cout << "Debug addTuple: Adding tuple " << tupleId << " to page " << metadata.pageID << std::endl;


        std::cout << "Debug addTuple: Added tuple. Free space left: " << metadata.freeSpace
                << ", Slot count: " << metadata.slotCount << std::endl;

        return true;
}

void Page::serialize(std::fstream& dbFile) {
    if (!dbFile) {
             dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }        
           
        }

        // Write the page metadata
        dbFile.write(reinterpret_cast<char*>(&metadata), sizeof(PageMetadata));
        if (!dbFile) {
            dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }               
        }
        std::cout << "Debug  page serialize: Serialized page metadata (PageID: " << metadata.pageID << ", SlotCount: " << metadata.slotCount << ")\n";

        // Count and write the number of active slots
        uint16_t activeSlotCount = 0;
        for (const auto& slot : slots) {
            if (slot.length > 0) {
                ++activeSlotCount;
            }
        }
        dbFile.write(reinterpret_cast<char*>(&activeSlotCount), sizeof(activeSlotCount));
        if (!dbFile) {
            dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }    
            
        }

        std::cout << "Debug  page serialize: Active slots count: " << activeSlotCount << std::endl;

        // Serialize each active slot
        for (const auto& slot : slots) {
            if (slot.length > 0) {
                   dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }    
        if (!dbFile) {
                    std::cerr << "Error  page serialize: Failed to write slot metadata.\n";
                    
                }
                dbFile.write(reinterpret_cast<const char*>(&slot), sizeof(Slot));
                
                std::cout << "Debug  page serialize: Serialized Slot. Offset: " << slot.offset 
                        << ", Length: " << slot.length << std::endl;
            }
        }
         dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }  
        if (!dbFile) {
            std::cerr << "Error  page serialize: Failed to write page data.\n";
            return;
        }
        // Write the page data
        dbFile.write(data, PAGE_SIZE);
        
        std::cout << "Debug  page serialize: Finished serializing page.\n";
}

void Page::deserialize(std::fstream& dbFile) {
    std::cout << "Debug: Deserializing page.\n";

    if (!dbFile) {
        dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }               
    }

    // Read the page metadata
    dbFile.read(reinterpret_cast<char*>(&metadata), sizeof(PageMetadata));
    if (!dbFile) {
dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }            
    }
    std::cout << "Debug page deserialize: Deserialized page metadata successfully. PageID: " << metadata.pageID
              << ", SlotCount: " << metadata.slotCount << "\n";

    // Read the number of slots
    uint16_t slotCount;
    dbFile.read(reinterpret_cast<char*>(&slotCount), sizeof(slotCount));
    if (!dbFile) {
       dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }    
       
    }
    std::cout << "Debug page deserialize: Slot count from file: " << slotCount << std::endl;

    // Clear existing slots and prepare to load new ones
    slots.clear();
    for (int i = 0; i < slotCount; ++i) {
        Slot slot;
        dbFile.read(reinterpret_cast<char*>(&slot), sizeof(Slot));

         // Debugging output for each slot
        if (dbFile) {
            std::cout << "Deserialized Slot: Offset = " << slot.offset << ", Length = " << slot.length << std::endl;
        } else {
            std::cerr << "Error page deserialize: Failed to read slot " << i << std::endl;
            return;
        }

        // Ensure that the slot has a valid length and does not exceed the page size
        if (slot.length > 0 && slot.offset + slot.length <= PAGE_SIZE) {
            slots.push_back(slot);
            std::cout << "Debug page deserialize: Slot added. Offset: " << slot.offset 
                      << ", Length: " << slot.length << std::endl;
        } else {
            // Log error if the slot is invalid
            std::cerr << "Error page deserialize: Invalid slot at index " << i << ". Offset: " << slot.offset
                      << ", Length: " << slot.length << std::endl;
        }
    }

    // Read the page data
    dbFile.read(data, PAGE_SIZE);
    if (!dbFile) {
       dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }    
        
    }

    std::cout << "Debug page deserialize: Finished deserializing page. PageID: " << metadata.pageID << "\n";

}

std::string Page::getTupleIndex(const std::string& tablePath, uint16_t tupleID) {
    std::fstream dbFile;
    dbFile.open(tablePath, std::ios::in | std::ios::binary);
    if (!dbFile.is_open()) {
        dbFile.open(tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }        
        return "";
    }

    // Deserialize file metadata (assumed at the beginning of the file)
    FileMetadata fileMetadata;
    fileMetadata.deserialize(dbFile);

     // Check for deserialization success
    if (dbFile.fail()) {
        std::cerr << "Error getTupleIndex: Failed to deserialize file metadata.\n";
        dbFile.close();
        return "";
    }

    // Use the tuple-to-page map to find the page ID associated with the tupleID
    auto it = fileMetadata.getTupleToPageMap().find(tupleID);
    if (it == fileMetadata.getTupleToPageMap().end() || it->second == -2) {
        std::cerr << "Error getTupleIndex: Tuple ID not found or marked as deleted." << std::endl;
        dbFile.close();
        return "";
    }

    uint16_t pageID = it->second;
    std::cout << "Debug getTupleIndex: Found tuple with ID " << tupleID << " on page " << pageID << std::endl;

    // Use your getPagePosition function to get the page position
    uint64_t pagePosition = fileMetadata.getPagePosition(pageID); // Assuming getPagePosition handles the offset correctly
    std::cout << "Debug getTupleIndex: Seeking to page position " << pagePosition << std::endl;

    // Deserialize the page
    Page page(pageID);
    page.deserialize(dbFile);
    // Check for deserialization success
    if (dbFile.fail()) {
        std::cerr << "Error getTupleIndex: Failed to deserialize page " << pageID << ".\n";
        dbFile.close();
        return "";
    }

    // Map the tupleID to the correct slot index
    uint16_t slotIndex = -1;
    for (uint16_t i = 0; i < page.getTupleCount(); ++i) {
        // Assuming the tuple ID is stored in the tuple itself, you could compare here
        std::string tupleData = page.getTupleData(i);  // Retrieve the tuple data
        Tuple tuple;
        std::cout << "Debug getTupleIndex: Deserialized tuple ID: " << tuple.getAttributeValue("id") << std::endl;

        if (tuple.deserialize(tupleData)) {
            if (tuple.getAttributeValue("id") == std::to_string(tupleID)) {
                slotIndex = i;
                std::cout << "Debug getTupleIndex: Found matching tuple with ID " << tupleID << " at slot index " << i << std::endl;

                break;  // Found the tuple with the matching ID
            }
        }
    }

    if (slotIndex == -1) {
        std::cerr << "Error getTupleIndex: Tuple ID not found on the page." << std::endl;
        dbFile.close();
        return "";
    }

    // Now, retrieve the tuple data from the page using the found slotIndex
    std::string tupleData = page.getTupleData(slotIndex);
    std::cout << "Debug getTupleIndex: Retrieved tuple data: " << tupleData << std::endl;

    dbFile.close();
    return tupleData;
}

std::string Page::getTupleData(uint16_t index) const
{
    // Debug: Check if the index is valid
        std::cout << "Debug getTupleData: Retrieving tuple at index " << index << std::endl;

        if (index >= slots.size() || slots[index].length == 0) {
            std::cerr << "Error getTupleData: Tuple ID not found at index " << index << std::endl;
            throw std::out_of_range("Tuple ID not found");
        }
        if (slots[index].offset + slots[index].length > PAGE_SIZE) {
        std::cerr << "Error getTupleData: Corrupted page data. Tuple offset and length are out of bounds." << std::endl;
        throw std::runtime_error("Corrupted page data.");
    }
        const Slot& slot = slots[index];
        std::cout << "Debug getTupleData: Tuple found. Offset: " << slot.offset << ", Length: " << slot.length << std::endl;

        return std::string(data + slot.offset, slot.length);
}
bool Page::deleteTuple(uint16_t slotIndex, int tupleID, const std::string& tablePath)
{
    std::cout << "Debug deleteTuple: Attempting to delete tuple with ID " << tupleID << " at slot index " << slotIndex << std::endl;

    if (slotIndex >= slots.size() || slots[slotIndex].length == 0) {
        std::cerr << "Error deleteTuple: Tuple not found or already deleted at slot index " << slotIndex << std::endl;
        return false;  // Tuple not found or already deleted
    }
    
    Slot& slot = slots[slotIndex];
    
    // Open the file to update the tuple-to-page map
    std::fstream dbFile(tablePath, std::ios::in | std::ios::out | std::ios::binary);
    if (!dbFile.is_open()) {
         dbFile.open(tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }
    }
    
    // Deserialize the file metadata
    FileMetadata fileMetadata;
    fileMetadata.deserialize(dbFile);
    
    // Remove the tuple from the page map (mark as deleted)
    fileMetadata.removeTupleFromPageMap(tupleID);
    std::cout << "Debug deleteTuple: Tuple with ID " << tupleID << " is marked as deleted in the page map." << std::endl;
    
    // Clear the data associated with the slot
    std::memset(data + slot.offset, 0, slot.length);
    std::cout << "Debug deleteTuple: Cleared data at offset " << slot.offset << ", Length: " << slot.length << std::endl;

    // Reset the slot metadata to mark the tuple as deleted
    slot.length = 0;
    slot.offset = 0;
    metadata.slotCount--;
    std::cout << "Debug deleteTuple: Slot marked as deleted. Remaining slot count: " << metadata.slotCount << std::endl;

    
    // Move the file pointer back to the beginning of the file before writing the updated metadata
    dbFile.seekp(0, std::ios::beg);
    
    // Serialize the updated file metadata
    fileMetadata.serialize(dbFile);  // This will update the metadata in the file
    std::cout << "Debug deleteTuple: Updated file metadata written to the file." << std::endl;

    dbFile.close();
    return true;
}
int Page::getTupleIndexByID(const std::string& id) const
{
    std::cout << "Debug getTupleIndexByID: Searching for tuple with ID " << id << std::endl;

    // Iterate over all slots to find the tuple with the matching ID
    for (size_t i = 0; i < slots.size(); ++i) {
        if (slots[i].length == 0) {
            continue;  // Skip empty slots
        }

        // Extract tuple data from the page
        std::string tupleData(data + slots[i].offset, slots[i].length);

        // Deserialize the tuple
        Tuple tuple;
        if (tuple.deserialize(tupleData)) {
            std::string tupleID = tuple.getAttributeValue("id");
            std::cout << "Debug getTupleIndexByID: Checking tuple ID " << tupleID << " at slot " << i << std::endl;

            // Compare the deserialized ID with the requested ID
            if (tupleID == id) {
                std::cout << "Debug getTupleIndexByID: Found tuple with ID " << id << " at slot index " << i << std::endl;
                return i;  // Return the index of the found tuple
            }
        }
    }

   
    std::cout << "Debug getTupleIndexByID: Tuple with ID " << id << " not found." << std::endl;
    // If not found, return -1
    return -1;
}