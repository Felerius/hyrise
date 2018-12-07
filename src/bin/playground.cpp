#include <iostream>
#include <tpch/tpch_db_generator.hpp>

#include "sql/sql_pipeline_builder.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

using namespace opossum;  // NOLINT

int main() {
  TpchDbGenerator{0.01f}.generate_and_store();

  auto query_string = R"(SELECT * FROM customer)";

  auto sql_pipeline = SQLPipelineBuilder{query_string}.create_pipeline_statement();
  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  lqp->print();
}
