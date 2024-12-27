#ifndef TUPLE_HPP
#define TUPLE_HPP

#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <cstdint>



class Tuple {
private:
    std::vector<std::pair<std::string, std::pair<int, std::string>>> attributes;

public:
    void addAttribute(const std::string& key, int type, const std::string& value);
    std::string serialize() const;
    bool deserialize(const std::string& data);
    std::map<std::string, std::pair<int, std::string>> getAttributes() const;
    std::string getAttributeValue(const std::string& key) const;
};

#endif // TUPLE_HPP
