#include "storage.hpp"

std::string Storage::tablePath = "";
// Function to create a new database
bool Storage::createDatabase(const std::string& dbName) {
    if (!fs::exists(dbName)) {
        if (fs::create_directory(dbName)) {
            std::cout << "Database folder created: " << dbName << std::endl;
        } else {
            std::cerr << "Error: Database could not be created.\n";
            return false;
        }
    }
    return true;
}

// Function to check if a table exists
bool Storage::tableExists(const std::string& dbName, const std::string& tableName) {
    tablePath = dbName + "/" + tableName + ".HAD";
    return fs::exists(tablePath);
}

// Function to create a new table with the provided schema
bool Storage::createTable(const std::string& dbName, const std::string& tableName, const std::map<std::string, std::string>& schema1) {
    tablePath = dbName + "/" + tableName + ".HAD";
    std::cout << "Debug createTable: Creating table at path: " << tablePath << std::endl;

    if (fs::exists(tablePath)) {
        std::cout << "Table already exists: " << tablePath << std::endl;
        return true; // Table exists, so continue
    }

    std::fstream newTable(tablePath, std::ios::binary | std::ios::out | std::ios::in | std::ios::trunc);
    if (newTable) {
        FileMetadata* metadata = FileMetadata::getInstance();
        metadata->setPageCount(0); // Start with 0 pages
        FileMetadata::schema=schema1;// Use the provided schema
        
        std::cout << "Debug createTable: Initialized metadata with 0 pages and provided schema." << std::endl;

        metadata->serialize(newTable);  // Serialize metadata
        if (newTable) {
            std::cout << "Debug createTable: Serialized metadata to table file successfully." << std::endl;
        } else {
            std::cerr << "Error createTable: Failed to write metadata to table file." << std::endl;
        }
        newTable.close();
        std::cout << "Created new table with metadata: " << tablePath << std::endl;
        return true;
    }

    std::cerr << "Error createTable: Failed to create table file at path: " << tablePath << std::endl;
    return false;
}

// Function to delete a table
bool Storage::deleteTable(const std::string& tablePath) {
    std::cout << "Debug deleteTable: Attempting to delete table at path: " << tablePath << std::endl;

    if (fs::exists(tablePath)) {
        try {
            fs::remove(tablePath); // Remove the table file
            std::cout << "Debug deleteTable: Table deleted successfully: " << tablePath << std::endl;
            return true;
        } catch (const fs::filesystem_error& e) {
            std::cerr << "Error deleting table: " << e.what() << std::endl;
        }
    } else {
        std::cerr << "Error deleteTable: Table not found: " << tablePath << std::endl;
    }
    return false;
}

// Helper function to load a page by ID
Page Storage::loadPageByID(const std::string& tablePath, uint32_t pageID) {
    std::fstream dbFile(tablePath, std::ios::in | std::ios::binary);
    if (!dbFile.is_open()) {
        dbFile.open(tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }
    }

    FileMetadata* fileMetadata = FileMetadata::getInstance();
    fileMetadata->deserialize(dbFile);
    uint32_t pagePosition = fileMetadata->getPagePosition(pageID);
    dbFile.seekg(pagePosition, std::ios::beg);

    Page page(pageID);
    page.deserialize(dbFile);
    dbFile.close();
    return page;
}

// Retrieve tuples from a page
std::vector<Tuple> Storage::getTuplesFromPage(const Page& page) {
    std::vector<Tuple> tuples;
    for (size_t i = 0; i < page.getTupleCount(); ++i) {
        try {
            std::string tupleData = page.getTupleData(i);
            Tuple tuple;
            if (tuple.deserialize(tupleData)) {
                tuples.push_back(tuple);
            }
        } catch (const std::exception& e) {
            std::cerr << "Error retrieving tuple at slot " << i << ": " << e.what() << std::endl;
        }
    }
    return tuples;
}

// Load a tuple by ID
std::string Storage::loadTuple(const std::string& tablePath, uint16_t tupleID) {
    std::fstream dbFile(tablePath, std::ios::in | std::ios::binary);
    if (!dbFile.is_open()) {
  dbFile.open(Storage::tablePath, std::ios::in | std::ios::out | std::ios::binary);
        if (!dbFile.is_open()) {
            throw std::runtime_error("Error File Metadata serialize: Unable to reopen the file stream.");
        }        
        return "";
    }

    FileMetadata* fileMetadata = FileMetadata::getInstance();
    try {
        fileMetadata->deserialize(dbFile);
    } catch (const std::exception& e) {
        dbFile.close();
        return "";
    }

    auto it = fileMetadata->getTupleToPageMap().find(tupleID);
    if (it == fileMetadata->getTupleToPageMap().end() || it->second == -2) {
        dbFile.close();
        return "";
    }

    uint16_t pageID = it->second;
    uint32_t pagePosition = fileMetadata->getPagePosition(pageID);
    dbFile.seekg(pagePosition, std::ios::beg);

    Page page(pageID);
    page.deserialize(dbFile);

    int slotIndex = page.getTupleIndexByID(std::to_string(tupleID));
    if (slotIndex == -1) {
        dbFile.close();
        return "";
    }

    std::string tupleData = page.getTupleData(slotIndex);
    dbFile.close();
    return tupleData;
}

std::map<std::string, std::string> Storage::get(const std::string& dbName, const std::string& tableName, const std::string& id) {
    // Construct the table path
     tablePath = dbName + "/" + tableName + ".HAD";

    // Open the table file
    std::fstream file(tablePath, std::ios::binary | std::ios::in);
    if (!file) {
        throw std::runtime_error("Failed to open the table file.");
    }

    // Read file metadata to get the tuple-to-page map
    FileMetadata* fileMetadata = FileMetadata::getInstance();
    try {
        fileMetadata->deserialize(file);
    } catch (const std::exception& e) {
        file.close();
        throw std::runtime_error("Error deserializing file metadata: " + std::string(e.what()));
    }
    
    uint32_t tupleId;
    try {
        tupleId = std::stoi(id);  // Convert string id to integer
    } catch (const std::invalid_argument& e) {
        file.close();
        throw std::invalid_argument("Invalid ID format: " + id);
    }

    // Check if the tuple exists in the map
    auto it = fileMetadata->getTupleToPageMap().find(stoi(id));
    if (it == fileMetadata->getTupleToPageMap().end() || it->second == -1) {
        // Tuple ID not found or is marked as deleted
        file.close();
        throw std::out_of_range("Tuple ID not found");
    }

    // Get the page ID from the map
    uint32_t pageID = it->second;

    // Calculate the position of the page in the file
    file.seekg(fileMetadata->getPagePosition(pageID), std::ios::beg);

     if (!file.good()) {
        file.close();
        throw std::runtime_error("Failed to seek to page position in file.");
    }

    // Load the page
    Page page(pageID);
    try {
        page.deserialize(file);
    } catch (const std::exception& e) {
        file.close();
        throw std::runtime_error("Error deserializing page with ID " + std::to_string(pageID) + ": " + std::string(e.what()));
    }

    // Search for the tuple in the page
    for (uint16_t i = 0; i < page.getTupleCount(); ++i) {
        std::string tupleData = page.getTupleData(i);
        Tuple tuple;

        // Deserialize the tuple and check if the ID matches
        if (tuple.deserialize(tupleData) && tuple.getAttributeValue("id") == id) {
            file.close();

            // Create a map to store the tuple's key-value pairs
            std::map<std::string, std::string> result;
            for (const auto& attribute : tuple.getAttributes()) {
                // Assuming tuple.getAttributes() returns a list of attributes as pairs of <key, value>
                result[attribute.first] = attribute.second.second;
            }
            return result; // Return the map of key-value pairs
        }
    }

    // If the tuple was not found
    file.close();
    throw std::out_of_range("Tuple with ID " + id + " not found on page " + std::to_string(pageID));

}

bool Storage::addTupleToTable(const std::string& dbName, const std::string& tableName, const std::string& tupleSerialized, int id) {
     tablePath = dbName + "/" + tableName + ".HAD";
    std::cout << "Debug addTupleToTable: Adding tuple to table file: " << tablePath << std::endl;

    // Check if the table exists
    if (!fs::exists(tablePath)) {
        std::cerr << "Table does not exist: " << tablePath << std::endl;
        return false;
    }

    // Open the table file for reading and writing
    std::fstream file(tablePath, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
        std::cerr << "Failed to open table file for reading and writing.\n";
        return false;
    }
    std::cout << "Debug addTupleToTable: Successfully opened table file for reading and writing.\n";

    FileMetadata* fileMetadata=FileMetadata::getInstance();
    try {
        fileMetadata->deserialize(file);
        std::cout << "Debug addTupleToTable: Deserialized file metadata.\n";
    } catch (const std::exception& e) {
        std::cerr << "Error deserializing file metadata: " << e.what() << std::endl;
        file.close();
        return false;
    }

    // Check if there is space in the existing pages
    int pageId = fileMetadata->getPageCount(); // Assuming this is the next available page ID
    size_t pagePosition = fileMetadata->getPagePosition(pageId);  // Calculate the position of the page

    // Read the page to check for space
    file.seekg(pagePosition);
    Page page(pageId);
    page.deserialize(file);

    std::cout << "Debug addTupleToTable: Page deserialized.\n";
    // Try to add the tuple to this page
    if (page.addTuple(tupleSerialized, fileMetadata,id)) {
        std::cout << "Debug addTupleToTable: Writing updated page at position " << pagePosition << "\n";
        
        file.seekp(pagePosition);
        page.serialize(file);
         std::cout << "Debug addTupleToTable: Updated page serialized and written to file.\n";
        
        // Update metadata after adding a tuple to an existing page
        file.seekp(0);  // Go to the beginning of the file to write metadata
        fileMetadata->serialize(file);  // Update the metadata
        // Flush and close the file
        file.flush();
        file.close();

        std::cout << "Debug addTupleToTable: Tuple successfully added to existing page.\n";
        return true;  // Tuple successfully added
    } else {
        std::cerr << "Error addTupleToTable: Failed to add tuple to page.\n";
    }

    // If no existing page had space, create a new page and append it
    
    Page newPage(fileMetadata->getNextPageID()); // Assuming this method gives the next available page ID
    std::cout << "Debug addTupleToTable: No space on existing pages. Creating a new page with ID: " << newPage.getPageID() << "\n";
    
    if (!newPage.addTuple(tupleSerialized, fileMetadata, id)) {
        std::cerr << "Failed to add tuple to a new page.\n";
        file.close();
        return false;
    }

    // Append the new page to the end of the file
    file.clear();
    file.seekp(0, std::ios::end);
    newPage.serialize(file);
    std::cout << "Debug addTupleToTable: New page serialized and appended to file.\n";

    // Increment the page ID after creating a new page
    fileMetadata->incrementPageID();
    file.seekp(0);  // Seek to the beginning of the file to write metadata
    fileMetadata->serialize(file);  // Write updated metadata
    file.close();
    
    std::cout << "Debug addTupleToTable: Tuple successfully added to a new page.\n";;
    return true;
}

bool Storage::checkTupleExists(const std::string& dbName, const std::string& tableName, const std::string& id) {
    // Check if the database exists
    if (!fs::exists(dbName)) {
        std::cerr << "Database '" << dbName << "' not found.\n";
        return false;
    }

    // Check if the table exists
     tablePath = dbName + "/" + tableName + ".HAD";
    if (!fs::exists(tablePath)) {
        std::cerr << "Table '" << tableName << "' not found in database '" << dbName << "'.\n";
        return false;
    }

    int tupleID;
    try {
        tupleID = std::stoi(id);  // Convert string id to integer
    } catch (const std::invalid_argument& e) {
        std::cerr << "Invalid ID format: '" << id << "'. ID must be a valid integer.\n";
        return false;
    } catch (const std::out_of_range& e) {
        std::cerr << "ID '" << id << "' is out of range.\n";
        return false;
    }
    // Open the table file for reading
    std::fstream file(tablePath, std::ios::binary | std::ios::in);
    if (!file) {
        std::cerr << "Failed to open table g: " << tablePath << "\n";
        return false;
    }

    // Read the file metadata
    FileMetadata* fileMetadata=FileMetadata::getInstance();
    try {
        fileMetadata->deserialize(file);
    } catch (const std::exception& e) {
        std::cerr << "Error deserializing file metadata: " << e.what() << "\n";
        file.close();
        return false;
    }

    // Check if the tuple ID exists in the tuple-to-page map in file metadata
    if (fileMetadata->hasTupleInPageMap(std::stoi(id))) {
        std::cout << "Tuple with ID '" << id << "' found in table: " << tableName << " (via metadata lookup).\n";
        file.close();
        return true; // Tuple found via metadata map
    }

    std::cerr << "Tuple with ID '" << id << "' not found in table: " << tableName << " (via metadata map).\n";
    file.close();
    return false; // Tuple does not exist
}
bool Storage::insert(const std::string& dbName, const std::string& tableName, const Tuple& tuple) {

        std::map<int, std::string> typeMap = {
        {1, "int"},
        {2, "string"},
        {3, "double"},
        // Add more types as needed
    };
    // Validate database existence
    if (!fs::exists(dbName)) {
        std::cerr << "Database does not exist: " << dbName << std::endl;
        return false;
    }

    // Validate table existence
    tablePath = dbName + "/" + tableName + ".HAD";
    if (!fs::exists(tablePath)) {
        std::cerr << "Table does not exist: " << tableName << std::endl;
        return false;
    }

    // Open the table file for reading
    std::fstream file(tablePath, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
        std::cerr << "Failed to open table file: " << tablePath << "\n";
        return false;
    }

    // Read file metadata, including schema
    FileMetadata* fileMetadata=FileMetadata::getInstance();
    std::map<std::string, std::string>  schema2=fileMetadata->deserialize(file);

    // Extract and validate tuple attributes against schema in file metadata
    std::map<std::string, std::pair<int, std::string>> attributes = tuple.getAttributes();
    for (const auto& attr : attributes) {
        std::cout<< attr.first<<" ";
    }
    std::cout<<std::endl;
    for (const auto& [key, type] : schema2) {
        std::cout<<key<<" "<<" type";
        std::cout<<std::endl;
        // Check if attribute exists in tuple
        // if (attributes.find(key) == attributes.end()) {
        //     std::cerr << "Missing required attribute: " << key << std::endl;
        //     return false;
        // }

        // Check data type and length
        const auto& [attrType, attrValue] = attributes[key];
        if (typeMap[attrType] != type) {
            std::cerr << "Type mismatch for attribute: " << key << std::endl;
            return false;
        }
    }

    // Check if 'id' is unique using tuple-to-page map in file metadata
    std::string idValue = attributes["id"].second; // Assuming "id" is always present
    int id = std::stoi(idValue);
    if (fileMetadata->hasTupleWithID(id)) {
        std::cerr << "Duplicate ID: " << id << " for table: " << tableName << std::endl;
        return false;
    }

    // Serialize tuple and add to the table
    std::string serializedTuple = tuple.serialize();


    if (!addTupleToTable(dbName, tableName, serializedTuple,id)) {
        std::cerr << "Failed to add tuple to table: " << tableName << std::endl;
        return false;
    }

    std::cout << "Debug insert: Tuple successfully added to table: " << tableName << std::endl;
    file.flush();
    return true;
}

bool Storage::deleteTupleFromTable(const std::string& dbName, const std::string& tableName, const std::string& id) {
     tablePath = dbName + "/" + tableName + ".HAD";
    std::cout << "Debug deleteTupleFromTable: Deleting tuple from table file: " << tablePath << std::endl;

    // Check if the table exists
    if (!fs::exists(tablePath)) {
        std::cerr << "Table does not exist: " << tablePath << std::endl;
        return false;
    }

    // Open the table file for reading and writing
    std::fstream file(tablePath, std::ios::binary | std::ios::in | std::ios::out);
    if (!file) {
        std::cerr << "Failed to open table file for reading and writing.\n";
        return false;
    }
    std::cout << "Debug deleteTupleFromTable: Table file opened for reading and writing.\n";


    // Read file metadata (including tuple-to-page map)
    FileMetadata fileMetadata;
    fileMetadata.deserialize(file);
    std::cout << "Debug deleteTupleFromTable: File metadata deserialized.\n";

    // Check if the tuple exists using the tuple-to-page map
    int tupleID = std::stoi(id);
    if (!fileMetadata.hasTupleWithID(tupleID)) {
        std::cerr << "Tuple with ID " << id << " does not exist.\n";
        file.close();
        return false;
    }

    std::cout << "Debug deleteTupleFromTable: Tuple with ID " << id << " found in the file metadata.\n";

    // Retrieve the page ID from the map
    int pageID = fileMetadata.getPageIDForTuple(tupleID);
    std::cout << "Debug deleteTupleFromTable: Found page ID " << pageID << " for tuple ID " << id << ".\n";


    // Locate the corresponding page and find the tuple
    Page page(pageID);
    std::streampos pagePos = file.tellg();
    file.seekg(fileMetadata.getPagePosition(pageID), std::ios::beg); // Move to the correct page position
    page.deserialize(file);

    bool tupleFound = false;
    for (uint16_t i = 0; i < page.getTupleCount(); ++i) {
        std::string tupleData = page.getTupleData(i);
        Tuple tuple;
        if (tuple.deserialize(tupleData) && tuple.getAttributeValue("id") == id) {
            tupleFound = true; // Tuple found, proceed to delete
             std::cout << "Debug deleteTupleFromTable: Tuple with ID " << id << " found in the page.\n";
            if (page.deleteTuple(i, tupleID, tablePath)) { // Call deleteTuple from Page class
                // Update the tuple-to-page map and mark the tuple as deleted
                fileMetadata.setTupleAsDeleted(tupleID);
                std::cout << "Debug deleteTupleFromTable: Tuple marked as deleted in file metadata.\n";

                // Write the modified page back to the file
                file.seekp(pagePos); // Move the write pointer to the start of the page
                page.serialize(file);
                fileMetadata.serialize(file); // Re-serialize the metadata
                std::cout << "Debug deleteTupleFromTable: Page and file metadata serialized back to file.\n";

                file.close();
                std::cout << "Successfully deleted tuple with ID: " << id << std::endl;
                return true; // Tuple successfully deleted
            }
        }
    }

    file.close();
    if (!tupleFound) {
        std::cerr << "Failed to delete tuple with ID: " << id << ". It may not exist.\n";
    }
    return false; // Tuple with the given ID was not found
}
bool Storage::updateTupleInTable(const std::string& dbName, const std::string& tableName, const std::string& id, const Tuple& updatedTuple) {
    tablePath = dbName + "/" + tableName + ".HAD";
    std::cout << "Debug: Attempting to update tuple in table file: " << tablePath << std::endl;

    // Check if the table exists
    if (!fs::exists(tablePath)) {
        std::cerr << "Table does not exist: " << tablePath << std::endl;
        return false;
    }
    std::cout << "Debug: Table exists: " << tablePath << std::endl;

    // Check if the tuple exists using the tuple-to-page map in Storage class
    std::cout << "Debug: Checking if the tuple with ID " << id << " exists in the table." << std::endl;
    if (!deleteTupleFromTable(dbName, tableName, id)) {
        std::cerr << "Failed to delete the tuple with ID " << id << std::endl;
        return false; // Exit if the tuple could not be deleted
    }
    std::cout << "Debug: Tuple with ID " << id << " marked for deletion.\n";

    // Now that the old tuple is deleted, insert the updated tuple into the table
    std::cout << "Debug: Attempting to insert the updated tuple." << std::endl;
    if (!insert(dbName, tableName, updatedTuple)) {
        std::cerr << "Failed to insert the updated tuple.\n";
        return false; // Exit if the updated tuple could not be inserted
    }
    std::cout << "Debug: Updated tuple inserted successfully into the table.\n";

    std::cout << "Successfully updated tuple with ID: " << id << std::endl;
    return true; // Tuple successfully updated
}

