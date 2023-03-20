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

#pragma once

#include <iostream>
#include <vector>

#include "velox/dwio/common/QplJobPool.h"
#include "velox/dwio/common/TypeUtil.h"
#include "velox/vector/BaseVector.h"
#include "velox/dwio/parquet/qpl_reader/QplDictionaryColumnVisitor.h"


namespace facebook::velox::parquet::qpl_reader {

// This class will be used for dictionary Ids or other data that is
// RLE/BP encoded and inflate compressed data

class DeflateRleBpDecoder {
 public:
    DeflateRleBpDecoder(
        const char* FOLLY_NONNULL pageData,
        thrift::PageHeader pageHeader,
        const char* FOLLY_NONNULL dictPageData,
        thrift::PageHeader dictPageHeader,
        ParquetTypeWithIdPtr type)
        : pageData_(pageData),
          pageHeader_(pageHeader),
          dictPageData_(dictPageData),
          dictPageHeader_(dictPageHeader),
          type_(type) {}


      template <bool hasNulls, typename Visitor>
  uint32_t decodeWithVisitor(const uint64_t* FOLLY_NULLABLE nulls, Visitor visitor) {
    if (useQplPath<hasNulls>(visitor)) {
        constexpr bool hasFilter =
            !std::is_same_v<typename Visitor::FilterType, common::AlwaysTrue>;
        constexpr bool hasHook =
            !std::is_same_v<typename Visitor::HookType, dwio::common::NoHook>;
        auto rows = visitor.rows();
        auto numRows = visitor.numRows();
        auto rowsAsRange = folly::Range<const int32_t*>(rows, numRows);

        return rleDecode<hasFilter, hasHook, false>(rowsAsRange, nullptr, visitor);
    } else {
        std::cout << "not support yet!" << std::endl;
    }
    return 0;
  }

    template <bool hasNulls, typename Visitor>
  void filterWithVisitor(const uint64_t* FOLLY_NULLABLE nulls, Visitor visitor) {
    if (useQplPath<hasNulls>(visitor)) {
        constexpr bool hasFilter =
            !std::is_same_v<typename Visitor::FilterType, common::AlwaysTrue>;
        constexpr bool hasHook =
            !std::is_same_v<typename Visitor::HookType, dwio::common::NoHook>;
        auto rows = visitor.rows();
        auto numRows = visitor.numRows();
        auto rowsAsRange = folly::Range<const int32_t*>(rows, numRows);

        filterScan<hasFilter, hasHook, false>(rowsAsRange, nullptr, visitor);
    } else {
        std::cout << "not support yet!" << std::endl;
    }
    return;
  }
  
    template <bool hasNulls, typename Visitor>
  void readWithVisitor(const uint64_t* FOLLY_NULLABLE nulls, Visitor visitor) {
    if (useQplPath<hasNulls>(visitor)) {
        constexpr bool hasFilter =
            !std::is_same_v<typename Visitor::FilterType, common::AlwaysTrue>;
        constexpr bool hasHook =
            !std::is_same_v<typename Visitor::HookType, dwio::common::NoHook>;
        auto rows = visitor.rows();
        auto numRows = visitor.numRows();
        auto rowsAsRange = folly::Range<const int32_t*>(rows, numRows);

        bulkScan<hasFilter, hasHook, false>(rowsAsRange, nullptr, visitor);
    } else {
        std::cout << "not support yet!" << std::endl;
    }
    return;
  }

  void skip(uint64_t numValues) {
    return;
  }

 protected:
    const char* FOLLY_NULLABLE pageData_{nullptr};    // uncompressed page data
    thrift::PageHeader pageHeader_;

    // uncompressed dictionary page data
    const char* FOLLY_NULLABLE dictPageData_{nullptr};
    thrift::PageHeader dictPageHeader_;

    ParquetTypeWithIdPtr type_;


 private:
  template <bool hasNulls, typename ColumnVisitor>
  bool useQplPath(const ColumnVisitor& visitor) {
    if (hasNulls) {
        return false;
    }

    // TODO: check data is uint16 uint32

    // auto numRows = visitor.numRows();
    // auto values = visitor.rawValues(numRows);
    // using TValues = typename std::remove_reference<decltype(values[0])>::type;
    // using TIndex = typename std::make_signed_t<typename dwio::common::make_index<TValues>::type>;

    // if (sizeof(TIndex) <= sizeof(uint32_t)) {
    //   return true;
    // }

    // TODO: check filter type
    return true;
  }


  template <bool hasFilter, bool hasHook, bool scatter,  typename ColumnVisitor>
  void bulkScan(folly::Range<const int32_t*> nonNullRows,
      const int32_t* FOLLY_NULLABLE scatterRows,
      ColumnVisitor& visitor) {
      auto& reader = visitor.reader();

      // Step1. uncompress + scan with mask data pate to get output vector
      auto numRows = visitor.numRows();
      auto numAllRows = visitor.numRows();
      auto values = visitor.rawValues(numRows);
      using TValues = typename std::remove_reference<decltype(values[0])>::type;
      using TIndex = typename std::make_signed_t<typename dwio::common::make_index<TValues>::type>;
      
      dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
      uint32_t job_id = 0;

      qpl_job* job = qpl_job_pool.AcquireJob(job_id);

      // uint32_t job_size = 0;
      // qpl_get_job_size(qpl_path_hardware, &job_size);
      // qpl_job* job = (qpl_job *) std::malloc(job_size);
      // auto status_1 = qpl_init_job(qpl_path_hardware, job);

      job->op=qpl_op_extract;
      job->next_in_ptr=reinterpret_cast<uint8_t*>(const_cast<char*>(pageData_));
      job->available_in=pageHeader_.compressed_page_size;
      job->parser = qpl_p_parquet_rle;
      job->param_low = 0;
      job->param_high = numAllRows;
      job->out_bit_width = qpl_ow_32;
      job->next_out_ptr = reinterpret_cast<uint8_t*>(values);
      job->available_out = static_cast<uint32_t>(numRows * sizeof(TIndex)); 
      job->num_input_elements = numAllRows;
      job->flags   = QPL_FLAG_DECOMPRESS_ENABLE | QPL_FLAG_FIRST | QPL_FLAG_LAST;

      auto status = qpl_submit_job(job);
      VELOX_DCHECK(status == QPL_STS_OK, "Execturion of QPL Job failed");
      // status = qpl_wait_job(job);
      do
      {
          _tpause(1, __rdtsc() + 1000);
      } while (qpl_check_job(job) == QPL_STS_BEING_PROCESSED);
      // std::cout << "deflate rle success" << std::endl;
      qpl_fini_job(qpl_job_pool.GetJobById(job_id));
      qpl_fini_job(job);
      // std::free(job);
      qpl_job_pool.ReleaseJob(job_id);
      // job = nullptr;

      // Step 2.  filter dictionary to get output
      int32_t numValues = 0;
      auto filterHits = hasFilter ? visitor.outputRows(numRows) : nullptr;
      visitor.template processRun<hasFilter, hasHook, scatter>(
          values,
          numRows,
          nullptr,
          filterHits,
          values,
          numValues);
      if (visitor.atEnd()) {
        visitor.setNumValues(hasFilter ? numValues : numAllRows);
        return;
      }       

  }

  template <bool hasFilter, bool hasHook, bool scatter,  typename ColumnVisitor>
  uint32_t rleDecode(folly::Range<const int32_t*> nonNullRows,
      const int32_t* FOLLY_NULLABLE scatterRows,
      ColumnVisitor& visitor) {

      // Step1. uncompress + scan with mask data pate to get output vector
      auto numRows = visitor.numRows();
      auto numAllRows = visitor.numRows();
      auto values = visitor.rawValues(numRows);
      using TValues = typename std::remove_reference<decltype(values[0])>::type;
      using TIndex = typename std::make_signed_t<typename dwio::common::make_index<TValues>::type>;
      
      dwio::common::QplJobHWPool& qpl_job_pool = dwio::common::QplJobHWPool::GetInstance();
      uint32_t job_id = 0;

      qpl_job* job = qpl_job_pool.AcquireJob(job_id);

      // uint32_t job_size = 0;
      // qpl_get_job_size(qpl_path_hardware, &job_size);
      // qpl_job* job = (qpl_job *) std::malloc(job_size);
      // auto status_1 = qpl_init_job(qpl_path_hardware, job);

    job->op = qpl_op_extract;
    job->next_in_ptr = reinterpret_cast<uint8_t*>(const_cast<char*>(pageData_));
    job->available_in = pageHeader_.compressed_page_size;
    job->parser = qpl_p_parquet_rle;
    job->param_low = 0;
    job->param_high = numAllRows;
    job->out_bit_width = qpl_ow_32;
    job->next_out_ptr = reinterpret_cast<uint8_t*>(values);
    job->available_out = static_cast<uint32_t>(numRows * sizeof(TIndex));
    job->num_input_elements = numAllRows;
    job->flags   = QPL_FLAG_DECOMPRESS_ENABLE | QPL_FLAG_FIRST | QPL_FLAG_LAST;

      auto status = qpl_submit_job(job);
      VELOX_DCHECK(status == QPL_STS_OK, "Execturion of QPL Job failed");
    // std::cout << "submit decode job: " << (int)job_id << std::endl;
      return job_id;
  }

  template <bool hasFilter, bool hasHook, bool scatter,  typename ColumnVisitor>
  void filterScan(folly::Range<const int32_t*> nonNullRows,
      const int32_t* FOLLY_NULLABLE scatterRows,
      ColumnVisitor& visitor) {
      auto numRows = visitor.numRows();
      auto numAllRows = visitor.numRows();
      auto values = visitor.rawValues(numRows);

      //  filter dictionary to get output
      int32_t numValues = 0;
      auto filterHits = hasFilter ? visitor.outputRows(numRows) : nullptr;
      visitor.template processRun<hasFilter, hasHook, scatter>(
          values,
          numRows,
          nullptr,
          filterHits,
          values,
          numValues);
      if (visitor.atEnd()) {
        visitor.setNumValues(hasFilter ? numValues : numAllRows);
        return;
      }       

  }  

};

}  // namespace facebook::velox::parquet::qpl_reader