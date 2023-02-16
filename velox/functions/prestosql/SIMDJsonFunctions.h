/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "simdjson.h"
#include "velox/functions/Macros.h"
#include "velox/functions/UDFOutputString.h"
#include "velox/functions/prestosql/json/JsonExtractor.h"
#include "velox/functions/prestosql/json/SimdJsonExtractor.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions {

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void 
  call(bool& result, const arg_type<Json>& json, const TInput& value) {

    std::string jsonData = json;
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document jsonObj;
    simdjson::padded_string padded_json(jsonData.data(), jsonData.length());
    std::string jsonpath = "";
    result = false;

    do {
      try {
        jsonObj = parser.iterate(padded_json);
      } catch (simdjson::simdjson_error& e) {
        printf(
            "error: Failed to parse json as document. error :%s\n",
            simdjson::error_message(e.error()));
        result = false;
        break;
      }

      if (jsonObj.type() != simdjson::ondemand::json_type::array) {
        result = false;
        break;
      }

      try {
        for (auto&& v : jsonObj) {
          if constexpr (std::is_same_v<TInput, bool>) {
            if (v.type() == simdjson::ondemand::json_type::boolean &&
                v.get_bool() == value) {
              result = true;
              break;
            }
          } else if constexpr (std::is_same_v<TInput, int64_t>) {
            if (v.type() == simdjson::ondemand::json_type::number &&
                ((v.get_number_type() ==
                      simdjson::ondemand::number_type::signed_integer &&
                  v.get_int64() == value) ||
                 (v.get_number_type() ==
                      simdjson::ondemand::number_type::unsigned_integer &&
                  v.get_uint64() == value))) {
              result = true;
              break;
            }
          } else if constexpr (std::is_same_v<TInput, double>) {
            if (v.type() == simdjson::ondemand::json_type::number &&
                v.get_number_type() ==
                    simdjson::ondemand::number_type::floating_point_number &&
                v.get_double() == value) {
              result = true;
              break;
            }
          } else {
            if (v.type() == simdjson::ondemand::json_type::string) {
              std::string_view rlt = v.get_string();
              std::string str_value = value.getString();
              if (rlt.compare(str_value) == 0) {
                result = true;
                break;
              }
            }
          }
        }
      } catch (simdjson::simdjson_error& e) {
        if (e.error() != simdjson::INCORRECT_TYPE &&
            e.error() != simdjson::NUMBER_ERROR) {
          result = true;
          break;
        }
      }
    } while (0);
  }
};

template <typename T>
struct SIMDJsonParseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& json) {

    std::string jsonData = json;

    simdjson::dom::parser parser;
    simdjson::dom::element jsonDoc;
    simdjson::padded_string padded_json(jsonData.data(),jsonData.length());

    try{
      jsonDoc = parser.parse(padded_json);
      std::string_view rlt_tmp = simdjson::to_string(jsonDoc);
      std::string rlt(rlt_tmp);
      UDFOutputString::assign(result, rlt);
      return true;
    } catch(simdjson::simdjson_error& e)
    {
      VELOX_USER_FAIL("Cannot convert '{}' to JSON", jsonData);
      return false;
    }
  }
};

template <typename T>
struct SIMDJsonExtractScalarFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& json,
      const arg_type<Varchar>& jsonPath) {

    std::string jsonData = json;
    std::string jsonPathStr = jsonPath;

    auto extractResult = SimdJsonExtractScalar(jsonData, jsonPathStr);

    if (extractResult.has_value()) {
      UDFOutputString::assign(result, extractResult.value());
      return true;
    } else {
      return false;
    }
  }
};

template <typename T>
struct SIMDJsonValidFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      int64_t& result,
      const arg_type<Varchar>& json) {
    std::string jsonData = json;
    simdjson::dom::parser parser;
    simdjson::dom::element jsonObj;
    simdjson::padded_string padded_json(jsonData.data(),jsonData.length());
    std::string jsonpath = "";
    
    try {
      jsonObj = parser.parse(padded_json);
      result = 1;
    } catch(simdjson::simdjson_error& e)
    {
      printf("error: Failed to parse json as document. error :%s\n",simdjson::error_message(e.error()));
      result = 0;
    }
  }
};

template <typename T>
struct SIMDJsonArrayLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const arg_type<Json>& json) {
    std::string jsonData = json;
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document jsonObj;
    simdjson::padded_string padded_json(jsonData.data(),jsonData.length());
    std::string jsonpath = "";
    
    try {
      jsonObj = parser.iterate(padded_json);
    }
    catch(simdjson::simdjson_error& e) {
      printf("error: Failed to parse json as document. error :%s\n",simdjson::error_message(e.error()));
      return false;
    }
    
    if (jsonObj.type() != simdjson::ondemand::json_type::array) {
      return false;
    }

    result = 0;
    try{
      for (auto &&v : jsonObj) {
        result++;
      }
      return true;
    }
    catch (simdjson::simdjson_error& e) {
      printf("error: Failed to count array length. error :%s\n",simdjson::error_message(e.error()));
    }
    return false;
  }
};

template <typename T>
struct SIMDJsonKeysFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json) {
    std::string jsonData = json;
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document jsonObj;
    simdjson::padded_string padded_json(jsonData.data(),jsonData.length());
    
    try {
      jsonObj = parser.iterate(padded_json);
    }
    catch(simdjson::simdjson_error& e) {
      printf("error: Failed to parse json as document. error :%s\n",simdjson::error_message(e.error()));
      return false;
    }
    
    if (jsonObj.type() != simdjson::ondemand::json_type::object) {
      return false;
    }

    std::string rlt = "[";
    int count = 0;
    int objCnt = jsonObj.count_fields();
    try{
      for (auto &&field : jsonObj.get_object()) {
        std::string_view tmp = field.unescaped_key();
        rlt += "\""+std::string(tmp)+"\"";
        if(++count != objCnt) {
          rlt += ",";
        }
      }
      rlt += "]";
      UDFOutputString::assign(result, rlt);
      return true;
    }
    catch (simdjson::simdjson_error& e) {
      printf("error: Failed to find json key. error :%s\n",simdjson::error_message(e.error()));
    }
    return false;
  }
};

template <typename T>
struct SIMDJsonLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const arg_type<Json>& json) {
    // TODO: implement Json Length function with simdjson.
    return false;
  }
};

} // namespace facebook::velox::functions
