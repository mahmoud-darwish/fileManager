#include "tuple.hpp"

// Add a new attribute to the tuple
void Tuple::addAttribute(const std::string& key, int type, const std::string& value) {
    attributes.push_back({key, {type, value}});
    std::cout << "[DEBUG addAttribute] Added new attribute: " << key << " with value: " << value << std::endl;
}

// Serialize the tuple into a string
std::string Tuple::serialize() const {
    std::ostringstream oss;
    for (const auto& attr : attributes) {
        oss << attr.first << "(" << attr.second.first << "|" << attr.second.second << ")";
    }
    std::string result = oss.str();
    std::cout << "[DEBUG Tuple serialize] Serialized tuple: " << result << std::endl;
    return result;
}

// Deserialize a string into a tuple
bool Tuple::deserialize(const std::string& data) {
    attributes.clear();
    std::istringstream iss(data);
    std::string token;

    while (std::getline(iss, token, ')')) {
        if (token.empty()) continue;

        auto colonPos = token.find('(');
        if (colonPos == std::string::npos) {
            std::cerr << "[ERROR Tuple deserialize] Malformed token: " << token << std::endl;
            continue;
        }

        std::string key = token.substr(0, colonPos);
        std::string values = token.substr(colonPos + 1);
        auto commaPos = values.find('|');
        if (commaPos == std::string::npos) {
            std::cerr << "[ERROR Tuple deserialize] Malformed value part: " << values << std::endl;
            continue;
        }

        std::string valueFirst = values.substr(0, commaPos);
        std::string valueSecond = values.substr(commaPos + 1);

        if (!key.empty() && !valueFirst.empty() && !valueSecond.empty()) {
            try {
                int firstValue = std::stoi(valueFirst);
                addAttribute(key, firstValue, valueSecond);
            } catch (const std::exception& e) {
                std::cerr << "[ERROR Tuple deserialize] Failed to convert valueFirst to int: " 
                          << valueFirst << ". Exception: " << e.what() << std::endl;
            }
        } else {
            std::cerr << "[ERROR Tuple deserialize] Invalid key, valueFirst, or valueSecond: " 
                      << key << " - " << valueFirst << " - " << valueSecond << std::endl;
        }
    }

    bool success = !attributes.empty();
    std::cout << "[DEBUG Tuple deserialize] Deserialization " << (success ? "succeeded" : "failed") 
              << ". Total attributes: " << attributes.size() << std::endl;
    return success;
}

// Retrieve all attributes as a map
std::map<std::string, std::pair<int, std::string>> Tuple::getAttributes() const {
      static std::map<std::string, std::pair<int, std::string>> attributesMap;
    for (const auto& attr : attributes) {
        attributesMap[attr.first] = attr.second;
    }
    return attributesMap;
}

// Get the value of a specific attribute by key
std::string Tuple::getAttributeValue(const std::string& key) const {
    for (const auto& attr : attributes) {
        if (attr.first == key) {
            std::cout << "[DEBUG getAttributeValue] Found attribute: " << key << " with value: " << attr.second.second << std::endl;
            return attr.second.second;
        }
    }
    std::cerr << "[WARNING getAttributeValue] Attribute not found: " << key << std::endl;
    return "";
}
