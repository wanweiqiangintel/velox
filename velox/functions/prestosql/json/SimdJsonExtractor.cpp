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

#include "velox/functions/prestosql/json/SimdJsonExtractor.h"

#include <cctype>
#include <unordered_map>
#include <vector>

#include "boost/algorithm/string/trim.hpp"
#include "folly/String.h"
#include "simdjson.h"
#include "velox/common/base/Exceptions.h"
#include "velox/functions/prestosql/json/JsonPathTokenizer.h"

namespace facebook::velox::functions {

namespace {

std::optional<std::string> extract(
    const std::string& json,
    const std::vector<std::string>& tokens_);
std::optional<std::string> extractFromObject(
    int pathIndex,
    simdjson::dom::object obj,
    const std::vector<std::string>& tokens_);
std::optional<std::string> extractFromArray(
    int pathIndex,
    simdjson::dom::array arr,
    const std::vector<std::string>& tokens_);
std::optional<std::string> extractOndemand(
    const std::string& json,
    const std::vector<std::string>& tokens_);
std::optional<std::string> extractFromObjectOndemand(
    int pathIndex,
    simdjson::ondemand::object obj,
    const std::vector<std::string>& tokens_);
std::optional<std::string> extractFromArrayOndemand(
    int pathIndex,
    simdjson::ondemand::array arr,
    const std::vector<std::string>& tokens_);

bool isDocBasicType(simdjson::ondemand::document& doc) {
  return (doc.type() == simdjson::ondemand::json_type::number) ||
      (doc.type() == simdjson::ondemand::json_type::boolean) ||
      (doc.type() == simdjson::ondemand::json_type::null);
}
bool isValueBasicType(
    simdjson::simdjson_result<simdjson::ondemand::value> rlt) {
  return (rlt.type() == simdjson::ondemand::json_type::number) ||
      (rlt.type() == simdjson::ondemand::json_type::boolean) ||
      (rlt.type() == simdjson::ondemand::json_type::null);
}

std::optional<std::string> extract(
    const std::string& json,
    const std::vector<std::string>& tokens_) {
  ParserContext ctx(json.data(), json.length());

  try {
    ctx.parseElement();
  } catch (simdjson::simdjson_error& e) {
    // Return 'null' if JSON input is not valid.
    return std::nullopt;
  }

  std::optional<std::string> rlt;
  if (ctx.jsonEle.type() ==
      simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::dom::element_type::ARRAY) {
    rlt = extractFromArray(0, ctx.jsonEle, tokens_);
  } else if (
      ctx.jsonEle.type() ==
      simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::dom::element_type::OBJECT) {
    rlt = extractFromObject(0, ctx.jsonEle, tokens_);
  } else {
    return std::nullopt;
  }
  return rlt;
}

std::optional<std::string> extractScalar(
    const std::string& json,
    const std::vector<std::string>& tokens_) {
  ParserContext ctx(json.data(), json.length());
  std::string jsonpath = "";
  try {
    ctx.parseDocument();
  } catch (simdjson::simdjson_error& e) {
    // Return 'null' if JSON input is not valid.
    return std::nullopt;
  }

  for (auto& token : tokens_) {
    jsonpath = jsonpath + "/" + token;
  }

  std::string_view rlt_tmp;
  try {
    if (jsonpath == "") {
      if (isDocBasicType(ctx.jsonDoc)) {
        rlt_tmp = simdjson::to_json_string(ctx.jsonDoc);
      } else if (ctx.jsonDoc.type() == simdjson::ondemand::json_type::string) {
        rlt_tmp = ctx.jsonDoc.get_string();
      } else {
        return std::nullopt;
      }
    } else {
      simdjson::simdjson_result<simdjson::ondemand::value> rlt_value =
          ctx.jsonDoc.at_pointer(jsonpath);
      if (isValueBasicType(rlt_value)) {
        rlt_tmp = simdjson::to_json_string(rlt_value);
      } else if (rlt_value.type() == simdjson::ondemand::json_type::string) {
        rlt_tmp = rlt_value.get_string();
      } else {
        return std::nullopt;
      }
    }
  } catch (simdjson::simdjson_error& e) {
    // Return 'null' if jsonpath is not valid.
    return std::nullopt;
  }
  std::string rlt_s{rlt_tmp};
  return rlt_s;
}

std::optional<std::string> extractFromObject(
    int pathIndex,
    simdjson::dom::object obj,
    const std::vector<std::string>& tokens_) {
  if (pathIndex == tokens_.size()) {
    std::string tmp = simdjson::to_string(obj);
    return std::string(tmp);
  }
  if (tokens_[pathIndex] == "*") {
    printf("error: extractFromObject can't include *\n");
    return std::nullopt;
  }
  auto path = "/" + tokens_[pathIndex];
  std::optional<std::string> rlt_string;
  try {
    auto rlt = obj.at_pointer(path);
    if (rlt.type() ==
        simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::dom::element_type::OBJECT) {
      rlt_string = extractFromObject(pathIndex + 1, rlt, tokens_);
    } else if (
        rlt.type() ==
        simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::dom::element_type::ARRAY) {
      rlt_string = extractFromArray(pathIndex + 1, rlt, tokens_);
    } else {
      std::string tmp = simdjson::to_string(rlt);
      rlt_string = std::optional<std::string>(tmp);
    }
  } catch (simdjson::simdjson_error& e) {
    // Return 'null' if jsonpath is not valid.
    return std::nullopt;
  }
  return rlt_string;
}

std::optional<std::string> extractFromArray(
    int pathIndex,
    simdjson::dom::array arr,
    const std::vector<std::string>& tokens_) {
  if (pathIndex == tokens_.size()) {
    std::string tmp = simdjson::to_string(arr);
    return std::string(tmp);
  }
  if (tokens_[pathIndex] == "*") {
    std::optional<std::string> rlt_tmp;
    std::string rlt = "[";
    int ii = 0;
    for (auto&& a : arr) {
      ii++;
      if (a.type() ==
          simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::dom::element_type::
              OBJECT) {
        rlt_tmp = extractFromObject(pathIndex + 1, a, tokens_);
        if (rlt_tmp.has_value()) {
          rlt += rlt_tmp.value();
          if (ii != arr.size()) {
            rlt += ",";
          }
        }
      } else if (
          a.type() ==
          simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::dom::element_type::ARRAY) {
        rlt_tmp = extractFromArray(pathIndex + 1, a, tokens_);
        if (rlt_tmp.has_value()) {
          rlt += rlt_tmp.value();
          if (ii != arr.size()) {
            rlt += ",";
          }
        }
      } else {
        std::string tmp = simdjson::to_string(a);
        rlt += std::string(tmp);
        if (ii != arr.size()) {
          rlt += ",";
        }
      }
      if (ii == arr.size()) {
        rlt += "]";
      }
    }
    return rlt;
  } else {
    auto path = "/" + tokens_[pathIndex];
    std::optional<std::string> rlt_string;
    try {
      auto rlt = arr.at_pointer(path);
      if (rlt.type() ==
          simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::dom::element_type::
              OBJECT) {
        rlt_string = extractFromObject(pathIndex + 1, rlt, tokens_);
      } else if (
          rlt.type() ==
          simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::dom::element_type::ARRAY) {
        rlt_string = extractFromArray(pathIndex + 1, rlt, tokens_);
      } else {
        std::string tmp = simdjson::to_string(rlt);
        return std::string(tmp);
      }
    } catch (simdjson::simdjson_error& e) {
      // Return 'null' if jsonpath is not valid.
      return std::nullopt;
    }
    return rlt_string;
  }
}

std::optional<std::string> extractOndemand(
    const std::string& json,
    const std::vector<std::string>& tokens_) {
  ParserContext ctx(json.data(), json.length());

  try {
    ctx.parseDocument();
  } catch (simdjson::simdjson_error& e) {
    // Return 'null' if JSON input is not valid.
    return std::nullopt;
  }

  std::optional<std::string> rlt;
  if (ctx.jsonDoc.type() ==
      simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::array) {
    rlt = extractFromArrayOndemand(0, ctx.jsonDoc, tokens_);
  } else if (
      ctx.jsonDoc.type() ==
      simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::object) {
    rlt = extractFromObjectOndemand(0, ctx.jsonDoc, tokens_);
  } else {
    return std::nullopt;
  }
  return rlt;
}

std::optional<std::string> extractFromObjectOndemand(
    int pathIndex,
    simdjson::ondemand::object obj,
    const std::vector<std::string>& tokens_) {
  if (pathIndex == tokens_.size()) {
    std::string_view tmp = simdjson::to_json_string(obj);
    return std::string(tmp);
  }
  if (tokens_[pathIndex] == "*") {
    printf("error: extractFromObjectOndemand can't include *\n");
    return std::nullopt;
  }
  obj.reset();
  auto path = "/" + tokens_[pathIndex];
  std::optional<std::string> rlt_string;
  try {
    auto rlt = obj.at_pointer(path);
    if (rlt.type() ==
        simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::
            object) {
      rlt_string = extractFromObjectOndemand(pathIndex + 1, rlt, tokens_);
    } else if (
        rlt.type() ==
        simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::array) {
      rlt_string = extractFromArrayOndemand(pathIndex + 1, rlt, tokens_);
    } else {
      std::string_view tmp = simdjson::to_json_string(rlt);
      rlt_string = std::optional<std::string>(std::string(tmp));
    }
  } catch (simdjson::simdjson_error& e) {
    // Return 'null' if jsonpath is not valid.
    return std::nullopt;
  }
  return rlt_string;
}

std::optional<std::string> extractFromArrayOndemand(
    int pathIndex,
    simdjson::ondemand::array arr,
    const std::vector<std::string>& tokens_) {
  if (pathIndex == tokens_.size()) {
    std::string_view tmp = simdjson::to_json_string(arr);
    return std::string(tmp);
  }
  arr.reset();
  if (tokens_[pathIndex] == "*") {
    std::optional<std::string> rlt_tmp;
    std::string rlt = "[";
    for (simdjson::ondemand::array_iterator a = arr.begin(); a != arr.end();) {
      if ((*a).type() ==
          simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::
              object) {
        rlt_tmp = extractFromObjectOndemand(pathIndex + 1, *a, tokens_);
        if (rlt_tmp.has_value()) {
          rlt += rlt_tmp.value();
          if (++a != arr.end()) {
            rlt += ",";
          }
        } else {
          ++a;
        }
      } else if (
          (*a).type() ==
          simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::
              array) {
        rlt_tmp = extractFromArrayOndemand(pathIndex + 1, *a, tokens_);
        if (rlt_tmp.has_value()) {
          rlt += rlt_tmp.value();
          if (++a != arr.end()) {
            rlt += ",";
          }
        } else {
          ++a;
        }
      } else {
        std::string_view tmp = simdjson::to_json_string(*a);
        rlt += std::string(tmp);
        if (++a != arr.end()) {
          rlt += ",";
        }
      }
      if (a == arr.end()) {
        rlt += "]";
      }
    }
    return rlt;
  } else {
    auto path = "/" + tokens_[pathIndex];
    std::optional<std::string> rlt_string;
    try {
      auto rlt = arr.at_pointer(path);
      if (rlt.type() ==
          simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::
              object) {
        rlt_string = extractFromObjectOndemand(pathIndex + 1, rlt, tokens_);
      } else if (
          rlt.type() ==
          simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::
              array) {
        rlt_string = extractFromArrayOndemand(pathIndex + 1, rlt, tokens_);
      } else {
        std::string_view tmp = simdjson::to_json_string(rlt);
        return std::string(tmp);
      }
    } catch (simdjson::simdjson_error& e) {
      // Return 'null' if jsonpath is not valid.
      return std::nullopt;
    }
    return rlt_string;
  }
}

std::optional<int64_t> getJsonSize(
    const std::string& json,
    const std::vector<std::string>& tokens_) {
  ParserContext ctx(json.data(), json.length());
  std::string jsonpath = "";
  int64_t len = 0;

  try {
    ctx.parseDocument();
  } catch (simdjson::simdjson_error& e) {
    // Return 'null' if JSON input is not valid.
    return std::nullopt;
  }

  for (auto& token : tokens_) {
    jsonpath = jsonpath + "/" + token;
  }

  try {
    auto rlt = ctx.jsonDoc.at_pointer(jsonpath);
    if (rlt.type() ==
        simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::array) {
      for (auto&& v : rlt) {
        len++;
      }
    } else if (
        rlt.type() ==
        simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::json_type::
            object) {
      len = rlt.count_fields();
    }
  } catch (simdjson::simdjson_error& e) {
    // Return 'null' if jsonpath is not valid.
    return std::nullopt;
  }
  return len;
}

} // namespace

ParserContext::ParserContext() noexcept = default;
ParserContext::ParserContext(const char* data, size_t length) noexcept
    : padded_json(data, length) {}
void ParserContext::parseElement() {
  jsonEle = domParser.parse(padded_json);
}
void ParserContext::parseDocument() {
  jsonDoc = ondemandParser.iterate(padded_json);
}

std::optional<std::string> simdJsonExtractString(
    const std::string& json,
    const std::vector<std::string>& token) {
  return extractOndemand(json, token);
}

std::optional<std::string> simdJsonExtractObject(
    const std::string& json,
    const std::vector<std::string>& token) {
  return extract(json, token);
}

std::optional<std::string> simdJsonExtractScalar(
    const std::string& json,
    const std::vector<std::string>& token) {
  return extractScalar(json, token);
}

std::optional<int64_t> simdJsonSize(
    const std::string& json,
    const std::vector<std::string>& token) {
  return getJsonSize(json, token);
}

} // namespace facebook::velox::functions
