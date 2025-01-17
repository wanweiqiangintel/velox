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
/**
 * This file tests the performance of each JsonXXXFunction.call() with
 * expression framework and Velox vectors.
 */
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/JsonFunctions.h"
#include "velox/functions/prestosql/SIMDJsonFunctions.h"
#include "velox/functions/prestosql/benchmarks/JsonBenchmarkUtil.h"
#include "velox/functions/prestosql/benchmarks/JsonFileReader.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::functions::prestosql {
namespace {

/// This function is only for test.
template <typename T>
struct JsonExtractFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    const folly::StringPiece& jsonStringPiece = json;
    const folly::StringPiece& jsonPathStringPiece = jsonPath;

    auto extractResult = jsonExtract(jsonStringPiece, jsonPathStringPiece);

    if (extractResult.hasValue()) {
      UDFOutputString::assign(result, folly::toJson(extractResult.value()));
      return true;
    } else {
      return false;
    }
  }
};

class JsonParseFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VectorPtr localResult;

    // Input can be constant or flat.
    // TODO(arpitporwal2293) Replace folly::parseJson with a lightweight
    // validation of JSON syntax that doesn't allocate memory or copy data.
    assert(args.size() > 0);
    const auto& arg = args[0];
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      try {
        folly::parseJson(value);
      } catch (const std::exception& e) {
        context.setErrors(rows, std::current_exception());
        return;
      }
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, JSON(), std::move(value));
    } else {
      auto flatInput = arg->asFlatVector<StringView>();

      auto stringBuffers = flatInput->stringBuffers();
      VELOX_CHECK_LE(rows.end(), flatInput->size());

      context.applyToSelectedNoThrow(
          rows, [&](auto row) { folly::parseJson(flatInput->valueAt(row)); });
      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          JSON(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::move(stringBuffers));
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> json
    return {exec::FunctionSignatureBuilder()
                .returnType("json")
                .argumentType("varchar")
                .build()};
  }
};

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_json_parse,
    JsonParseFunction::signatures(),
    std::make_unique<JsonParseFunction>());

class JsonBenchmark : public velox::functions::test::FunctionBenchmarkBase {
 public:
  JsonBenchmark() : FunctionBenchmarkBase() {
    velox::functions::prestosql::registerJsonFunctions(true);
    VELOX_REGISTER_VECTOR_FUNCTION(udf_json_parse, "folly_json_parse");
    registerFunction<JsonExtractFunction, Varchar, Varchar, Varchar>(
        {"folly_json_extract"});
    registerFunction<SIMDJsonExtractFunction, Varchar, Varchar, Varchar>(
        {"simd_json_extract"});
    registerFunction<SIMDJsonParseFunction, Varchar, Varchar>(
        {"simd_json_parse"});
  }

  std::string prepareData(const std::string& fileSize) {
    JsonFileReader reader;
    return reader.readJsonStringFromFile(fileSize);
  }

  velox::VectorPtr makeJsonData(const std::string& json, int vectorSize) {
    auto jsonVector = vectorMaker_.flatVector<velox::StringView>(vectorSize);
    for (auto i = 0; i < vectorSize; i++) {
      jsonVector->set(i, velox::StringView(json));
    }
    return jsonVector;
  }

  void runWithJsonPath(
      int iter,
      int vectorSize,
      const std::string& fnName,
      const std::string& json,
      const std::string& jsonPath) {
    folly::BenchmarkSuspender suspender;

    auto jsonVector = makeJsonData(json, vectorSize);
    auto jsonPathVector = velox::BaseVector::createConstant(
        VARCHAR(), jsonPath.data(), vectorSize, execCtx_.pool());

    auto rowVector = vectorMaker_.rowVector({jsonVector, jsonPathVector});
    auto exprSet =
        compileExpression(fmt::format("{}(c0, c1)", fnName), rowVector->type());
    suspender.dismiss();
    doRun(iter, exprSet, rowVector);
  }

  void runWithJson(
      int iter,
      int vectorSize,
      const std::string& fnName,
      const std::string& json) {
    folly::BenchmarkSuspender suspender;

    auto jsonVector = makeJsonData(json, vectorSize);

    auto rowVector = vectorMaker_.rowVector({jsonVector});
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", fnName), rowVector->type());
    suspender.dismiss();
    doRun(iter, exprSet, rowVector);
  }

  void doRun(
      const int iter,
      velox::exec::ExprSet& exprSet,
      const velox::RowVectorPtr& rowVector) {
    uint32_t cnt = 0;
    for (auto i = 0; i < iter; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

void VeloxJsonExtract(
    int iter,
    int vectorSize,
    const std::string& fileSize,
    const std::string& jsonPath) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(fileSize);
  suspender.dismiss();
  benchmark.runWithJsonPath(
      iter, vectorSize, "folly_json_extract", json, jsonPath);
}

void SIMDJsonExtract(
    int iter,
    int vectorSize,
    const std::string& fileSize,
    const std::string& jsonPath) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(fileSize);
  suspender.dismiss();
  benchmark.runWithJsonPath(
      iter, vectorSize, "simd_json_extract_scalar", json, jsonPath);
}

void VeloxJsonParse(int iter, int vectorSize, const std::string& fileSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(fileSize);
  suspender.dismiss();
  benchmark.runWithJson(iter, vectorSize, "folly_json_parse", json);
}

void SIMDJsonParse(int iter, int vectorSize, const std::string& fileSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(fileSize);
  suspender.dismiss();
  benchmark.runWithJson(iter, vectorSize, "simd_json_parse", json);
}

BENCHMARK_DRAW_LINE();

JSONEXTRACT_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonExtract,
    SIMDJsonExtract,
    100,
    1K,
    "$.statuses[0].friends_count",
    "$.statuses[0].user.entities.description.urls",
    "$.statuses[0].metadata.result_type")

JSONEXTRACT_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonExtract,
    SIMDJsonExtract,
    100,
    10K,
    "$.statuses[0].metadata.result_type",
    "$.statuses[5].metadata.result_type",
    "$.statuses[9].metadata.result_type")

JSONEXTRACT_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonExtract,
    SIMDJsonExtract,
    100,
    100K,
    "$.statuses[0].metadata.result_type",
    "$.statuses[8].metadata.result_type",
    "$.statuses[15].metadata.result_type")

JSONEXTRACT_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonExtract,
    SIMDJsonExtract,
    100,
    1000K,
    "$.statuses[0].metadata.result_type",
    "$.statuses[500].metadata.result_type",
    "$.statuses[999].metadata.result_type")

JSONEXTRACT_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonExtract,
    SIMDJsonExtract,
    100,
    10000K,
    "$.statuses[0].metadata.result_type",
    "$.statuses[5000].metadata.result_type",
    "$.statuses[9999].metadata.result_type")

JSONPARSE_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonParse,
    SIMDJsonParse,
    100,
    1K)

JSONPARSE_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonParse,
    SIMDJsonParse,
    100,
    10K)

JSONPARSE_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonParse,
    SIMDJsonParse,
    100,
    100K)

JSONPARSE_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonParse,
    SIMDJsonParse,
    100,
    1000K)

JSONPARSE_BENCHMARK_NAMED_PARAM_TWO_FUNCS(
    func,
    VeloxJsonParse,
    SIMDJsonParse,
    100,
    10000K)

BENCHMARK_DRAW_LINE();

} // namespace
} // namespace facebook::velox::functions::prestosql

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
