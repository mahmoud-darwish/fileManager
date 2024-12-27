#include"storage.hpp"
int main() {
    // Create a Storage object to manage databases
    Storage storage;
    
    // Define the database and table names
    std::string dbName = "testDB";
    std::string tableName = "users";
    
    // Define a simple schema for the table
   

    // Step 3: Prepare a tuple to insert into the table
    Tuple newTuple;
    newTuple.addAttribute("id", 1, "1");       // ID attribute, value = 1
    newTuple.addAttribute("name", 2, "Alice");  // Name attribute, value = "Alice"
    newTuple.addAttribute("age", 1, "30");      // Age attribute, value = 30
    
    // Step 4: Try to insert the tuple into the table
    //faild to read page metadata 
    if (storage.insert(dbName, tableName, newTuple)) {
        std::cout << "Tuple inserted successfully!" << std::endl;
    } else {
        std::cerr << "Failed to insert the tuple." << std::endl;
        return -1;
    }

    // Step 5: Verify that the tuple is inserted
    std::string idValue = newTuple.getAttributeValue("id");
    std::cout << "Inserted tuple ID: " << idValue << std::endl;
    
    // Additional checks can be performed to ensure the data is correctly inserted.

    return 0;
}
