#include <iostream>
#include <tpch/tpch_db_generator.hpp>

#include "sql/sql_pipeline_builder.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "operators/print.hpp"
#include "expression/expression_functional.hpp"
#include "scheduler/operator_task.hpp"

using namespace opossum;  // NOLINT
using namespace opossum::expression_functional;  // NOLINT

int main() {
  TpchDbGenerator{0.01f}.generate_and_store();

  auto query_string = R"(
SELECT
  p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
FROM partsupp, part
WHERE
   p_partkey = ps_partkey
   AND ps_suppkey not in (
       SELECT s_suppkey
       FROM supplier
       WHERE s_comment like '%Customer%Complaints%'
   )
)";

  auto query_string_opt = R"(SELECT
  p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
FROM partsupp, part, supplier
WHERE p_partkey = ps_partkey
  AND ps_suppkey = s_suppkey
  AND s_comment NOT LIKE '%Customer%Complaints%'
GROUP BY p_brand, p_type, p_size
)";

  auto sql_pipeline = SQLPipelineBuilder{query_string}.create_pipeline_statement();
  const auto& lqp_from_sql = sql_pipeline.get_optimized_logical_plan();

  auto sql_pipeline_opt = SQLPipelineBuilder{query_string_opt}.create_pipeline_statement();
  const auto& lqp_from_sql_opt = sql_pipeline_opt.get_optimized_logical_plan();

  auto supplier_table = StoredTableNode::make("supplier");
  auto partsupp_table = StoredTableNode::make("partsupp");
  auto part_table = StoredTableNode::make("part");
  auto ps_suppkey = partsupp_table->get_column("ps_suppkey");
  auto p_partkey = part_table->get_column("p_partkey");
  auto ps_partkey = partsupp_table->get_column("ps_partkey");
  auto s_comment = supplier_table->get_column("s_comment");

  auto subselect = PredicateNode::make(like_(s_comment, "%Customer%Complaints%"), supplier_table);
  auto manual_sql = ProjectionNode::make(
      expression_vector(p_partkey),
      AggregateNode::make(
          expression_vector(),
          expression_vector(count_distinct_(ps_suppkey)),
          JoinNode::make(
              JoinMode::Inner,
              equals_(p_partkey, ps_partkey),
              part_table,
              PredicateNode::make(
                  not_in_(ps_suppkey, lqp_select_(subselect)),
                  partsupp_table))));

  lqp_from_sql->print();
  std::cout << '\n';
  manual_sql->print();
  std::cout << '\n';
  lqp_from_sql_opt->print();

  auto pqp = LQPTranslator{}.translate_node(manual_sql);
  auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
  for (auto& task : tasks) {
    task->schedule();
  }
  Print::print(tasks.back()->get_operator()->get_output());
}
