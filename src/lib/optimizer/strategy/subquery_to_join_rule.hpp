#pragma once

#include <unordered_map>

#include "abstract_rule.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

// Optimizes (NOT) IN and (NOT) EXISTS expressions into semi/anti joins.
// Does not currently optimize:
//    - (NOT) IN expressions where
//        - the left value is not a column reference.
//        - the subquery produces something other than a column reference
//    - NOT IN with a correlated subquery
//    - Correlated subqueries where the correlated parameter
//        - is used outside predicates
//        - is used in predicates at a point where it cannot be pulled up into a join predicate (e.g., below joins,
//          limits, etc.)

class SubqueryToJoinRule : public AbstractRule {
 public:
  std::string name() const override;
  void apply_to(const std::shared_ptr<AbstractLQPNode>& node) const override;
};

}  // namespace opossum
