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

#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

class CollectListAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }
};

TEST_F(CollectListAggregateTest, groupBy) {
  std::vector<RowVectorPtr> batches;
  // Creating 3 batches of input data.
  // 0: {0, null} {0, 1}    {0, 2}
  // 1: {1, 1}    {1, null} {1, 3}
  // 2: {2, 2}    {2, 3}    {2, null}
  // 3: {3, 3}    {3, 4}    {3, 5}
  // 4: {4, 4}    {4, 5}    {4, 6}
  for (auto i = 0; i < 3; i++) {
    RowVectorPtr data = makeRowVector(
        {makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
         makeFlatVector<int64_t>(
             5,
             [&i](const vector_size_t& row) { return i + row; },
             [&i](const auto& row) { return i == row; })});
    batches.push_back(data);
  }

  auto expected = makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2, 3, 4}),
       makeArrayVectorFromJson<int64_t>(
           {"[1, 2]", "[1, 3]", "[2, 3]", "[3, 4, 5]", "[4, 5, 6]"})});

  testAggregations(
      batches,
      {"c0"},
      {"spark_collect_list(c1)"},
      {"c0", "array_sort(a0)"},
      {expected});
  testAggregationsWithCompanion(
      batches,
      [](auto& /*builder*/) {},
      {"c0"},
      {"spark_collect_list(c1)"},
      {{BIGINT()}},
      {"c0", "array_sort(a0)"},
      {expected},
      {});
}

TEST_F(CollectListAggregateTest, global) {
  auto data = makeRowVector({makeNullableFlatVector<int32_t>(
      {std::nullopt, 1, 2, std::nullopt, 4, 5})});
  auto expected =
      makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 2, 4, 5]"})});

  testAggregations(
      {data}, {}, {"spark_collect_list(c0)"}, {"array_sort(a0)"}, {expected});
  testAggregationsWithCompanion(
      {data},
      [](auto& /*builder*/) {},
      {},
      {"spark_collect_list(c0)"},
      {{INTEGER()}},
      {"array_sort(a0)"},
      {expected});
}

TEST_F(CollectListAggregateTest, ignoreNulls) {
  auto input = makeRowVector({makeNullableFlatVector<int32_t>(
      {1, 2, std::nullopt, 4, std::nullopt, 6})});
  // Spark will ignore all null values in the input.
  auto expected =
      makeRowVector({makeArrayVectorFromJson<int32_t>({"[1, 2, 4, 6]"})});
  testAggregations(
      {input}, {}, {"spark_collect_list(c0)"}, {"array_sort(a0)"}, {expected});
  testAggregationsWithCompanion(
      {input},
      [](auto& /*builder*/) {},
      {},
      {"spark_collect_list(c0)"},
      {{INTEGER()}},
      {"array_sort(a0)"},
      {expected},
      {});
}

TEST_F(CollectListAggregateTest, allNullsInput) {
  auto input = makeRowVector({makeAllNullFlatVector<int64_t>(100)});
  // If all input data is null, Spark will output an empty array.
  auto expected = makeRowVector({makeArrayVectorFromJson<int32_t>({"[]"})});
  testAggregations({input}, {}, {"spark_collect_list(c0)"}, {expected});
  testAggregationsWithCompanion(
      {input},
      [](auto& /*builder*/) {},
      {},
      {"spark_collect_list(c0)"},
      {{BIGINT()}},
      {},
      {expected},
      {});
}

std::unordered_map<std::string, std::string> makeConfig(bool ignoreNulls) {
  return {{"spark.collect_list.ignore_nulls", ignoreNulls ? "true" : "false"}};
}

TEST_F(CollectListAggregateTest, respectNulls) {
  // When ignoreNulls is false (RESPECT NULLS), nulls should be included.
  auto input = makeRowVector({makeNullableFlatVector<int32_t>(
      {1, 2, std::nullopt, 4, std::nullopt, 6})});
  auto expected = makeRowVector({makeNullableArrayVector<int32_t>(
      std::vector<std::vector<std::optional<int32_t>>>{
          {1, 2, std::nullopt, 4, std::nullopt, 6}})});
  std::vector<RowVectorPtr> expectedResult{expected};
  testAggregations(
      {input},
      {},
      {"spark_collect_list(c0)"},
      expectedResult,
      makeConfig(false));
}

TEST_F(CollectListAggregateTest, respectNullsGroupBy) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({0, 0, 1, 1, 1}),
       makeNullableFlatVector<int64_t>({std::nullopt, 1, 2, std::nullopt, 3})});
  auto expected = makeRowVector(
      {makeFlatVector<int32_t>({0, 1}),
       makeNullableArrayVector<int64_t>(
           std::vector<std::vector<std::optional<int64_t>>>{
               {std::nullopt, 1}, {2, std::nullopt, 3}})});
  std::vector<RowVectorPtr> expectedResult{expected};
  testAggregations(
      {data},
      {"c0"},
      {"spark_collect_list(c1)"},
      {"c0", "a0"},
      expectedResult,
      makeConfig(false));
}

TEST_F(CollectListAggregateTest, respectNullsAllNulls) {
  // When all inputs are null and ignoreNulls is false, output should be an
  // array of nulls (not an empty array).
  auto input = makeRowVector({makeAllNullFlatVector<int32_t>(3)});
  auto expected = makeRowVector({makeNullableArrayVector<int32_t>(
      std::vector<std::vector<std::optional<int32_t>>>{
          {std::nullopt, std::nullopt, std::nullopt}})});
  std::vector<RowVectorPtr> expectedResult{expected};
  testAggregations(
      {input},
      {},
      {"spark_collect_list(c0)"},
      expectedResult,
      makeConfig(false));
}

// collect_list(x, ignoreNulls) takes the flag as a constant argument, which
// overrides the 'spark.collect_list.ignore_nulls' config. This is how Spark's
// collect_list(x) RESPECT NULLS is passed per aggregate.
TEST_F(CollectListAggregateTest, ignoreNullsArgument) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({0, 0, 1, 1, 1, 2}),
       makeNullableFlatVector<int64_t>(
           {std::nullopt, 1, 2, std::nullopt, 3, std::nullopt})});
  auto respectNulls = makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2}),
       makeNullableArrayVector<int64_t>(
           std::vector<std::vector<std::optional<int64_t>>>{
               {std::nullopt, 1}, {2, std::nullopt, 3}, {std::nullopt}})});
  auto ignoreNulls = makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2}),
       makeArrayVectorFromJson<int64_t>({"[1]", "[2, 3]", "[]"})});

  for (const bool configIgnoreNulls : {true, false}) {
    SCOPED_TRACE(fmt::format("configIgnoreNulls: {}", configIgnoreNulls));
    testAggregations(
        {data},
        {"c0"},
        {"spark_collect_list(c1, false)"},
        {"c0", "a0"},
        {respectNulls},
        makeConfig(configIgnoreNulls));
    testAggregations(
        {data},
        {"c0"},
        {"spark_collect_list(c1, true)"},
        {"c0", "a0"},
        {ignoreNulls},
        makeConfig(configIgnoreNulls));
  }
}

// Aggregates in the same query can use different ignoreNulls flags.
TEST_F(CollectListAggregateTest, mixedIgnoreNulls) {
  auto data = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, std::nullopt, 2, std::nullopt, 3})});
  auto expected = makeRowVector(
      {makeArrayVectorFromJson<int32_t>({"[1, 2, 3]"}),
       makeNullableArrayVector<int32_t>(
           std::vector<std::vector<std::optional<int32_t>>>{
               {1, std::nullopt, 2, std::nullopt, 3}}),
       makeArrayVectorFromJson<int32_t>({"[1, 2, 3]"})});
  testAggregations(
      {data},
      {},
      {"spark_collect_list(c0)",
       "spark_collect_list(c0, false)",
       "spark_collect_list(c0, true)"},
      {expected});

  // All inputs are null.
  data = makeRowVector({makeAllNullFlatVector<int32_t>(3)});
  expected = makeRowVector(
      {makeArrayVectorFromJson<int32_t>({"[]"}),
       makeNullableArrayVector<int32_t>(
           std::vector<std::vector<std::optional<int32_t>>>{
               {std::nullopt, std::nullopt, std::nullopt}})});
  testAggregations(
      {data},
      {},
      {"spark_collect_list(c0)", "spark_collect_list(c0, false)"},
      {expected});
}

// The partial companion function receives the constant ignoreNulls argument.
TEST_F(CollectListAggregateTest, ignoreNullsArgumentCompanion) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({0, 0, 1, 1, 1, 2, 0, 1}),
       makeNullableFlatVector<int64_t>(
           {std::nullopt, 1, 2, std::nullopt, 3, std::nullopt, 4, 5})});
  // array_sort puts nulls last.
  auto expected = makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2}),
       makeNullableArrayVector<int64_t>(
           std::vector<std::vector<std::optional<int64_t>>>{
               {1, 4, std::nullopt},
               {2, 3, 5, std::nullopt},
               {std::nullopt}})});
  testAggregationsWithCompanion(
      {data},
      [](auto& /*builder*/) {},
      {"c0"},
      {"spark_collect_list(c1, false)"},
      {{BIGINT(), BOOLEAN()}},
      {"c0", "array_sort(a0)"},
      {expected});

  expected = makeRowVector(
      {makeFlatVector<int32_t>({0, 1, 2}),
       makeArrayVectorFromJson<int64_t>({"[1, 4]", "[2, 3, 5]", "[]"})});
  testAggregationsWithCompanion(
      {data},
      [](auto& /*builder*/) {},
      {"c0"},
      {"spark_collect_list(c1, true)"},
      {{BIGINT(), BOOLEAN()}},
      {"c0", "array_sort(a0)"},
      {expected},
      makeConfig(false));
}

// Window functions receive the constant ignoreNulls argument.
TEST_F(CollectListAggregateTest, ignoreNullsArgumentWindow) {
  auto data = makeRowVector(
      {makeFlatVector<int32_t>({0, 0, 0, 1, 1}),
       makeFlatVector<int32_t>({1, 2, 3, 1, 2}),
       makeNullableFlatVector<int64_t>(
           {std::nullopt, 1, std::nullopt, 2, std::nullopt})});

  auto plan =
      exec::test::PlanBuilder()
          .values({data})
          .window(
              {"spark_collect_list(c2, false) over (partition by c0 order by c1)",
               "spark_collect_list(c2, true) over (partition by c0 order by c1)"})
          .planNode();
  auto expected = makeRowVector(
      {makeFlatVector<int32_t>({0, 0, 0, 1, 1}),
       makeFlatVector<int32_t>({1, 2, 3, 1, 2}),
       makeNullableFlatVector<int64_t>(
           {std::nullopt, 1, std::nullopt, 2, std::nullopt}),
       makeNullableArrayVector<int64_t>(
           std::vector<std::vector<std::optional<int64_t>>>{
               {std::nullopt},
               {std::nullopt, 1},
               {std::nullopt, 1, std::nullopt},
               {2},
               {2, std::nullopt}}),
       makeArrayVectorFromJson<int64_t>({"[]", "[1]", "[1]", "[2]", "[2]"})});
  exec::test::AssertQueryBuilder(plan).assertResults(expected);
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
