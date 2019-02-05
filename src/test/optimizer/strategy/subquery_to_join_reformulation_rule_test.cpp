#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/subquery_to_join_reformulation_rule.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class SubqueryToJoinReformulationRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_int2.tbl"));
    StorageManager::get().add_table("table_b", load_table("src/test/tables/int_int3.tbl"));
    StorageManager::get().add_table("table_c", load_table("src/test/tables/int_int4.tbl"));
    StorageManager::get().add_table("table_d", load_table("src/test/tables/int_int_int.tbl"));
    StorageManager::get().add_table("table_e", load_table("src/test/tables/int_int_int2.tbl"));

    node_table_a = StoredTableNode::make("table_a");
    node_table_a_col_a = node_table_a->get_column("a");
    node_table_a_col_b = node_table_a->get_column("b");

    node_table_b = StoredTableNode::make("table_b");
    node_table_b_col_a = node_table_b->get_column("a");
    node_table_b_col_b = node_table_b->get_column("b");

    node_table_c = StoredTableNode::make("table_c");
    node_table_c_col_a = node_table_c->get_column("a");
    node_table_c_col_b = node_table_c->get_column("b");

    node_table_d = StoredTableNode::make("table_d");
    node_table_d_col_a = node_table_d->get_column("a");
    node_table_d_col_b = node_table_d->get_column("b");
    node_table_d_col_c = node_table_d->get_column("c");

    node_table_e = StoredTableNode::make("table_e");
    node_table_e_col_a = node_table_e->get_column("a");
    node_table_e_col_b = node_table_e->get_column("b");
    node_table_e_col_c = node_table_e->get_column("c");

    _rule = std::make_shared<SubqueryToJoinReformulationRule>();
  }

  std::shared_ptr<AbstractLQPNode> apply_in_rule(const std::shared_ptr<AbstractLQPNode>& lqp) {
    auto copied_lqp = lqp->deep_copy();
    StrategyBaseTest::apply_rule(_rule, copied_lqp);

    return copied_lqp;
  }

  std::shared_ptr<SubqueryToJoinReformulationRule> _rule;

  std::shared_ptr<StoredTableNode> node_table_a, node_table_b, node_table_c, node_table_d, node_table_e;
  LQPColumnReference node_table_a_col_a, node_table_a_col_b, node_table_b_col_a, node_table_b_col_b, node_table_c_col_a,
      node_table_c_col_b, node_table_d_col_a, node_table_d_col_b, node_table_d_col_c, node_table_e_col_a,
      node_table_e_col_b, node_table_e_col_c;
};

TEST_F(SubqueryToJoinReformulationRuleTest, UncorrelatedInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b)
  // clang-format off
  const auto subquery_lqp =
      ProjectionNode::make(expression_vector(node_table_b_col_a), node_table_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
      PredicateNode::make(in_(node_table_a_col_a, subquery),
          node_table_a);

  const auto expected_lqp =
      JoinNode::make(JoinMode::Semi, equals_(node_table_a_col_a, node_table_b_col_a),
                     node_table_a,
                     ProjectionNode::make(expression_vector(node_table_b_col_a), node_table_b));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinReformulationRuleTest, UncorrelatedNotInToAntiJoin) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b)
  // clang-format off
  const auto subquery_lqp =
      ProjectionNode::make(expression_vector(node_table_b_col_a), node_table_b);

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
      PredicateNode::make(not_in_(node_table_a_col_a, subquery),
                          node_table_a);

  const auto expected_lqp =
      JoinNode::make(JoinMode::Anti, equals_(node_table_a_col_a, node_table_b_col_a),
                     node_table_a,
                     ProjectionNode::make(expression_vector(node_table_b_col_a), node_table_b));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinReformulationRuleTest, SimpleCorrelatedInToInnerJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.b = a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_b);

  // clang-format off
  const auto subquery_lqp =
      ProjectionNode::make(expression_vector(node_table_b_col_a),
          PredicateNode::make(equals_(node_table_b_col_b, parameter),
              node_table_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, node_table_a_col_b));

  const auto input_lqp =
      PredicateNode::make(in_(node_table_a_col_a, subquery),
                          node_table_a);

  const auto expected_lqp =
      AggregateNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b), expression_vector(),
                          ProjectionNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b),
                                               PredicateNode::make(equals_(node_table_b_col_b, node_table_a_col_b),
                                                                   JoinNode::make(JoinMode::Inner,
                                                                                  equals_(node_table_a_col_a,
                                                                                          node_table_b_col_a),
                                                                                  node_table_a,
                                                                                  node_table_b))));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// We currently do not support this reformulation, because an anti join would not preserve the columns from the right sub-tree.
TEST_F(SubqueryToJoinReformulationRuleTest, ShouldNotReformulateSimpleCorrelatedNotInWithEqualityPredicate) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b WHERE b.b = a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_b);

  // clang-format off
  const auto subquery_lqp =
      ProjectionNode::make(expression_vector(node_table_b_col_a),
                           PredicateNode::make(equals_(node_table_b_col_b, parameter),
                                               node_table_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, node_table_a_col_b));

  const auto input_lqp =
      PredicateNode::make(not_in_(node_table_a_col_a, subquery),
                          node_table_a);

  //  const auto expected_lqp =
  //      AggregateNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b), expression_vector(),
  //                          ProjectionNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b),
  //                                               PredicateNode::make(equals_(node_table_b_col_b, node_table_a_col_b),
  //                                                                   JoinNode::make(JoinMode::Inner,
  //                                                                                  not_equals_(node_table_a_col_a,
  //                                                                                              node_table_b_col_a),
  //                                                                                  node_table_a,
  //                                                                                  node_table_b))));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, input_lqp);
}

// We currently do not support this reformulation, because an anti join would not preserve the columns from the right sub-tree.
TEST_F(SubqueryToJoinReformulationRuleTest, ShouldNotReformulateSimpleCorrelatedNotInWithLessThanPredicate) {
  // SELECT * FROM a WHERE a.a NOT IN (SELECT b.a FROM b WHERE b.b < a.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_b);

  // clang-format off
  const auto subquery_lqp =
      ProjectionNode::make(expression_vector(node_table_b_col_a),
                           PredicateNode::make(less_than_(node_table_b_col_b, parameter),
                                               node_table_b));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, node_table_a_col_b));

  const auto input_lqp =
      PredicateNode::make(not_in_(node_table_a_col_a, subquery),
                          node_table_a);

  //  const auto expected_lqp =
  //      AggregateNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b), expression_vector(),
  //                          ProjectionNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b),
  //                                               PredicateNode::make(less_than_(node_table_b_col_b, node_table_a_col_b),
  //                                                                   JoinNode::make(JoinMode::Inner,
  //                                                                                  not_equals_(node_table_a_col_a,
  //                                                                                          node_table_b_col_a),
  //                                                                                  node_table_a,
  //                                                                                  node_table_b))));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, input_lqp);
}

TEST_F(SubqueryToJoinReformulationRuleTest, UncorrelatedNestedInToSemiJoins) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.a IN (SELECT c.a FROM c))

  // clang-format off
  const auto inner_subquery_lqp =
      ProjectionNode::make(expression_vector(node_table_c_col_a), node_table_c);

  const auto inner_subquery = lqp_subquery_(inner_subquery_lqp);

  const auto subquery_lqp =
      ProjectionNode::make(expression_vector(node_table_b_col_a),
                           PredicateNode::make(in_(node_table_b_col_a, inner_subquery),
                           node_table_b));

  const auto subquery = lqp_subquery_(subquery_lqp);

  const auto input_lqp =
      PredicateNode::make(in_(node_table_a_col_a, subquery),
                          node_table_a);


  const auto expected_lqp =
      JoinNode::make(JoinMode::Semi, equals_(node_table_a_col_a, node_table_b_col_a),
                     node_table_a,
                     ProjectionNode::make(expression_vector(node_table_b_col_a),
                         JoinNode::make(JoinMode::Semi, equals_(node_table_b_col_a, node_table_c_col_a),
                             node_table_b,
                             ProjectionNode::make(expression_vector(node_table_c_col_a), node_table_c))));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(SubqueryToJoinReformulationRuleTest, DoubleCorrelatedInToInnerJoin) {
  // SELECT * FROM d WHERE d.a IN (SELECT e.a FROM e WHERE e.b = d.b AND e.c < d.c)
  const auto parameter0 = correlated_parameter_(ParameterID{0}, node_table_d_col_b);
  const auto parameter1 = correlated_parameter_(ParameterID{1}, node_table_d_col_c);

  // clang-format off
  const auto subquery_lqp =
      ProjectionNode::make(expression_vector(node_table_e_col_a),
                           PredicateNode::make(and_(
                               equals_(node_table_e_col_b, parameter0),
                               less_than_(node_table_e_col_c, parameter1)),
                                               node_table_e));

  const auto subquery = lqp_subquery_(subquery_lqp,
                                     std::make_pair(ParameterID{0}, node_table_d_col_b),
                                     std::make_pair(ParameterID{1}, node_table_d_col_c));

  const auto input_lqp =
      PredicateNode::make(in_(node_table_d_col_a, subquery),
                          node_table_d);

  const auto expected_lqp =
      AggregateNode::make(expression_vector(node_table_d_col_a, node_table_d_col_b, node_table_d_col_c),
                          expression_vector(),
                          ProjectionNode::make(
                              expression_vector(node_table_d_col_a, node_table_d_col_b, node_table_d_col_c),
                              PredicateNode::make(and_(equals_(node_table_e_col_b, node_table_d_col_b),
                                                      less_than_(node_table_e_col_c, node_table_d_col_c)),
                                                      JoinNode::make(JoinMode::Inner,
                                                                     equals_(node_table_d_col_a,
                                                                             node_table_e_col_a),
                                                                     node_table_d,
                                                                     node_table_e))));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
