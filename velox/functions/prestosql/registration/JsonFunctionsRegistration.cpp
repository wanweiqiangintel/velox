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

#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/JsonFunctions.h"
#include "velox/functions/prestosql/SIMDJsonFunctions.h"

namespace facebook::velox::functions {
void registerJsonFunctions() {
  registerJsonType();

  registerFunction<IsJsonScalarFunction, bool, Json>({"is_json_scalar"});
  registerFunction<JsonExtractScalarFunction, Varchar, Json, Varchar>(
      {"json_extract_scalar"});
  registerFunction<JsonArrayLengthFunction, int64_t, Json>(
      {"json_array_length"});
  registerFunction<JsonArrayContainsFunction, bool, Json, bool>(
      {"json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Json, int64_t>(
      {"json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Json, double>(
      {"json_array_contains"});
  registerFunction<JsonArrayContainsFunction, bool, Json, Varchar>(
      {"json_array_contains"});
  registerFunction<JsonSizeFunction, int64_t, Json, Varchar>({"json_size"});
  VELOX_REGISTER_VECTOR_FUNCTION(udf_json_format, "json_format");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_json_parse, "json_parse");
}

void registerSIMDJsonFunctions() {
  registerType("json", std::make_unique<const JsonTypeFactories>());

  registerFunction<SIMDJsonArrayContainsFunction, bool, Json, bool>(
      {"simd_json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Json, int64_t>(
      {"simd_json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Json, double>(
      {"simd_json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Json, Varchar>(
      {"simd_json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Varchar, bool>(
      {"simd_json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Varchar, int64_t>(
      {"simd_json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Varchar, double>(
      {"simd_json_array_contains"});
  registerFunction<SIMDJsonArrayContainsFunction, bool, Varchar, Varchar>(
      {"simd_json_array_contains"});

  registerFunction<SIMDJsonParseFunction, Varchar, Varchar>({"simd_json_parse"});
  registerFunction<SIMDJsonExtractScalarFunction, Varchar, Varchar, Varchar>(
      {"simd_json_extract_scalar"});
  registerFunction<SIMDJsonValidFunction, int64_t, Varchar>({"simd_json_valid"});
  {
    registerFunction<SIMDJsonArrayLengthFunction, int64_t, Varchar>(
        {"simd_json_array_length"});
    registerFunction<SIMDJsonArrayLengthFunction, int64_t, Json>(
        {"simd_json_array_length"});
  }
  {
    registerFunction<SIMDJsonKeysFunction, Varchar, Json>({"simd_json_keys"});
    registerFunction<SIMDJsonKeysFunction, Varchar, Varchar>({"simd_json_keys"});
  }
}
} // namespace facebook::velox::functions
