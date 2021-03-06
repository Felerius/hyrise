#pragma once

#include <boost/preprocessor/seq/for_each_product.hpp>
#include <boost/preprocessor/tuple/elem.hpp>

#include <cmath>

#include "jit_types.hpp"
#include "operators/table_scan/column_like_table_scan_impl.hpp"
#include "resolve_type.hpp"

namespace opossum {

/* This file contains the type dispatching mechanisms that allow generic operations on JitTupleValues.
 *
 * Each binary operation takes three JitTupleValues as parameters: a left input (lhs), a right input (rhs) and an
 * output (result). Each value has one of the supported data types and can be
 * nullable or non-nullable. This leaves us with (number_of_datatypes * 2) ^ 2 combinations for each operation.
 *
 * To make things easier, all arithmetic and comparison operations can be handled the same way:
 * A set of generic lambdas defines type-independent versions of these operations. These lambdas can be passed to the
 * "jit_compute" function to perform the actual computation. The lambdas work on raw, concrete values. It is the
 * responsibility of "jit_compute" to take care of NULL values, unpack input values and pack the result value.
 * This way all NULL-value semantics are kept in one place. If either of the inputs is NULL, the result of the
 * computation is also NULL. If neither input is NULL, the computation lambda is called.
 *
 * Inside "jit_compute", a switch statement (generated by the preprocessor) dispatches the 36 data-type combinations
 * and calls the lambda with the appropriately typed parameters. Invalid type combinations (e.g. adding an int32_t
 * to a std::string) are handled via the SFINAE pattern (substitution failure is not an error): The lambdas fail the
 * template substitution if they cannot perform their operation on a given combination of input types. In this case,
 * the "InvalidTypeCatcher" provides a default implementation that throws an exception.
 *
 * The generic lambdas can also be passed to the "jit_compute_type" function. The function uses the same dispatching
 * mechanisms. But instead of executing a computation, it only determines the result type the computation would
 * have if it were carried out. This functionality is used to determine the type of intermediate values and
 * computed output columns.
 *
 * Logical operators, IsNull and IsNotNull must be handled separately, since their NULL value semantics are
 * different (i.e. a NULL as either input does not result in the output being NULL as well).
 */

// Returns the enum value (e.g., DataType::Int, DataType::String) of a data type defined in the DATA_TYPE_INFO sequence
#define JIT_GET_ENUM_VALUE(index, s) APPEND_ENUM_NAMESPACE(_, _, BOOST_PP_TUPLE_ELEM(3, 1, BOOST_PP_SEQ_ELEM(index, s)))

// Returns the data type (e.g., int32_t, std::string) of a data type defined in the DATA_TYPE_INFO sequence
#define JIT_GET_DATA_TYPE(index, s) BOOST_PP_TUPLE_ELEM(3, 0, BOOST_PP_SEQ_ELEM(index, s))

#define JIT_COMPUTE_CASE(r, types)                                                                                   \
  case static_cast<uint8_t>(JIT_GET_ENUM_VALUE(0, types)) << 8 | static_cast<uint8_t>(JIT_GET_ENUM_VALUE(1, types)): \
    catching_func(lhs.get<JIT_GET_DATA_TYPE(0, types)>(context), rhs.get<JIT_GET_DATA_TYPE(1, types)>(context));     \
    break;

#define JIT_COMPUTE_TYPE_CASE(r, types)                                                                              \
  case static_cast<uint8_t>(JIT_GET_ENUM_VALUE(0, types)) << 8 | static_cast<uint8_t>(JIT_GET_ENUM_VALUE(1, types)): \
    return catching_func(JIT_GET_DATA_TYPE(0, types)(), JIT_GET_DATA_TYPE(1, types)());

#define JIT_AGGREGATE_COMPUTE_CASE(r, types)                                 \
  case JIT_GET_ENUM_VALUE(0, types):                                         \
    catching_func(lhs.get<JIT_GET_DATA_TYPE(0, types)>(context),             \
                  rhs.get<JIT_GET_DATA_TYPE(0, types)>(rhs_index, context)); \
    break;

/* The lambdas below are instanciated by the compiler for different combinations of data types. The decltype return
 * types are necessary in most cases, since we rely on the SFINAE pattern to divert to an alternative implementation
 * (which throws a runtime error) for invalid data type combinations. Without the decltype return type declaration, the
 * compiler is unable to detect invalid data type combinations at template instanciation time and produces compilation
 * errors.
 * Functions handling strings are declared as structs with operator() as the previously used lambdas caused the LLVM
 * ERROR: "Undefined temporary symbol" during specialization. Without the check for string or non-string type, the
 * compiler also produces compiler errors.
 */

/* Arithmetic operators */
struct JitAddition {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  auto operator()(const T1 a, const T2 b) const {
    return a + b;
  }
};
const JitAddition jit_addition{};
const auto jit_subtraction = [](const auto a, const auto b) -> decltype(a - b) { return a - b; };
const auto jit_multiplication = [](const auto a, const auto b) -> decltype(a * b) { return a * b; };
const auto jit_division = [](const auto a, const auto b) -> decltype(a / b) { return a / b; };
const auto jit_modulo = [](const auto a, const auto b) -> decltype(a % b) { return a % b; };
const auto jit_power = [](const auto a, const auto b) -> decltype(std::pow(a, b)) { return std::pow(a, b); };

/* Aggregate operations */
const auto jit_increment = [](const auto a, const auto b) -> decltype(b + 1) { return b + 1; };
struct JitMaximum {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  auto operator()(const T1 a, const T2 b) const {
    return std::max(a, b);
  }
};
const JitMaximum jit_maximum{};
struct JitMinimum {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  auto operator()(const T1 a, const T2 b) const {
    return std::min(a, b);
  }
};
const JitMinimum jit_minimum{};

/* Comparison operators */
struct JitEquals {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  bool operator()(const T1 a, const T2 b) const {
    return a == b;
  }
};
const JitEquals jit_equals{};
struct JitNotEquals {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  bool operator()(const T1 a, const T2 b) const {
    return a != b;
  }
};
const JitNotEquals jit_not_equals{};
struct JitLessThan {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  bool operator()(const T1 a, const T2 b) const {
    return a < b;
  }
};
const JitLessThan jit_less_than{};
struct JitLessThanEquals {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  bool operator()(const T1 a, const T2 b) const {
    return a <= b;
  }
};
const JitLessThanEquals jit_less_than_equals{};
struct JitGreaterThan {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  bool operator()(const T1 a, const T2 b) const {
    return a > b;
  }
};
const JitGreaterThan jit_greater_than{};
struct JitGreaterThanEquals {
  template <typename T1, typename T2,
            typename = typename std::enable_if_t<std::is_scalar_v<T1> == std::is_scalar_v<T2>>>
  bool operator()(const T1 a, const T2 b) const {
    return a >= b;
  }
};
const JitGreaterThanEquals jit_greater_than_equals{};

bool jit_like(const std::string& a, const std::string& b);
bool jit_not_like(const std::string& a, const std::string& b);

// The InvalidTypeCatcher acts as a fallback implementation, if template specialization
// fails for a type combination.
template <typename Functor, typename Result>
struct InvalidTypeCatcher : Functor {
  explicit InvalidTypeCatcher(Functor f) : Functor(std::move(f)) {}

  using Functor::operator();

  template <typename... Ts>
  Result operator()(const Ts...) const {
    Fail("Invalid combination of types for operation.");
  }
};

template <typename T>
void jit_compute(const T& op_func, const JitTupleValue& lhs, const JitTupleValue& rhs, const JitTupleValue& result,
                 JitRuntimeContext& context) {
  // Handle NULL values and return if either input is NULL.
  const bool result_is_null = lhs.is_null(context) || rhs.is_null(context);
  result.set_is_null(result_is_null, context);
  if (result_is_null) {
    return;
  }

  // This lambda calls the op_func (a lambda that performs the actual computation) with typed arguments and stores
  // the result.
  const auto store_result_wrapper = [&](const auto& typed_lhs,
                                        const auto& typed_rhs) -> decltype(op_func(typed_lhs, typed_rhs), void()) {
    using ResultType = decltype(op_func(typed_lhs, typed_rhs));
    result.template set<ResultType>(op_func(typed_lhs, typed_rhs), context);
  };

  const auto catching_func = InvalidTypeCatcher<decltype(store_result_wrapper), void>(store_result_wrapper);

  // The type information from the lhs and rhs are combined into a single value for dispatching without nesting.
  const auto combined_types = static_cast<uint8_t>(lhs.data_type()) << 8 | static_cast<uint8_t>(rhs.data_type());
  // catching_func is called in this switch:
  switch (combined_types) { BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_COMPUTE_CASE, (JIT_DATA_TYPE_INFO)(JIT_DATA_TYPE_INFO)) }
}

template <typename T>
DataType jit_compute_type(const T& op_func, const DataType lhs, const DataType rhs) {
  // This lambda calls the op_func (a lambda that could performs the actual computation) and determines the return type
  // of that lambda.
  const auto determine_return_type_wrapper =
      [&](const auto& typed_lhs, const auto& typed_rhs) -> decltype(op_func(typed_lhs, typed_rhs), DataType()) {
    using ResultType = decltype(op_func(typed_lhs, typed_rhs));
    // This templated function returns the DataType enum value for a given ResultType.
    return data_type_from_type<ResultType>();
  };

  const auto catching_func =
      InvalidTypeCatcher<decltype(determine_return_type_wrapper), DataType>(determine_return_type_wrapper);

  // The type information from the lhs and rhs are combined into a single value for dispatching without nesting.
  const auto combined_types = static_cast<uint8_t>(lhs) << 8 | static_cast<uint8_t>(rhs);
  // catching_func is called in this switch:
  switch (combined_types) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_COMPUTE_TYPE_CASE, (JIT_DATA_TYPE_INFO)(JIT_DATA_TYPE_INFO))
    default:
      // when lhs or rhs is null
      if (lhs == DataType::Null) return rhs;
      return lhs;
  }
}

void jit_not(const JitTupleValue& lhs, const JitTupleValue& result, JitRuntimeContext& context);
void jit_and(const JitTupleValue& lhs, const JitTupleValue& rhs, const JitTupleValue& result,
             JitRuntimeContext& context);
void jit_or(const JitTupleValue& lhs, const JitTupleValue& rhs, const JitTupleValue& result,
            JitRuntimeContext& context);
void jit_is_null(const JitTupleValue& lhs, const JitTupleValue& result, JitRuntimeContext& context);
void jit_is_not_null(const JitTupleValue& lhs, const JitTupleValue& result, JitRuntimeContext& context);

// The following functions are used within loop bodies in the JitAggregate operator. They should not be inlined
// automatically to reduce the amount of code produced during loop unrolling in the specialization process (a function
// call vs the entire inlined body). These functions will be manually inlined more efficiently after loop unrolling by
// the code specializer, since we can apply load replacement and branch pruning and only inline the code necessary for
// each specific loop iteration.
// Example: If we compute aggregates in a loop in the JitAggregate operator, the generic loop body will call the
// jit_aggregate_compute function, which can handle different data types. Nothing can be specialized here, because
// different iterations may work with different data types. Inlining the jit_aggregate_compute function into the loop
// would require inlining the entire (generic) function.
// However, after loop unrolling each copy of the unrolled body only computes a single aggregate with a definite data
// type. When inlining the function now, the code specializer will prune all code related to other data types,
// nullability etc.

// Computes the hash value for a JitTupleValue
__attribute__((noinline)) uint64_t jit_hash(const JitTupleValue& value, JitRuntimeContext& context);

// Compares a JitTupleValue to a JitHashmapValue using NULL == NULL semantics
__attribute__((noinline)) bool jit_aggregate_equals(const JitTupleValue& lhs, const JitHashmapValue& rhs,
                                                    const size_t rhs_index, JitRuntimeContext& context);

// Copies a JitTupleValue to a JitHashmapValue. Both values MUST be of the same data type.
__attribute__((noinline)) void jit_assign(const JitTupleValue& from, const JitHashmapValue& to, const size_t to_index,
                                          JitRuntimeContext& context);

// Adds an element to a column represented by some JitHashmapValue
__attribute__((noinline)) size_t jit_grow_by_one(const JitHashmapValue& value,
                                                 const JitVariantVector::InitialValue initial_value,
                                                 JitRuntimeContext& context);

// Updates an aggregate by applying an operation to a JitTupleValue and a JitHashmapValue. The result is stored in the
// hashmap value.
template <typename T>
__attribute__((noinline)) void jit_aggregate_compute(const T& op_func, const JitTupleValue& lhs,
                                                     const JitHashmapValue& rhs, const size_t rhs_index,
                                                     JitRuntimeContext& context) {
  // NULL values are ignored in aggregate computations
  if (lhs.is_null(context)) {
    return;
  }

  // Since we are updating the aggregate with a valid value, the aggregate is no longer NULL
  if (rhs.is_nullable()) {
    rhs.set_is_null(false, rhs_index, context);
  }

  // This lambda calls the op_func (a lambda that performs the actual computation) with typed arguments and stores
  // the result.
  const auto store_result_wrapper = [&](const auto typed_lhs,
                                        const auto typed_rhs) -> decltype(op_func(typed_lhs, typed_rhs), void()) {
    using ResultType = std::remove_const_t<decltype(typed_rhs)>;
    rhs.set<ResultType>(op_func(typed_lhs, typed_rhs), rhs_index, context);
  };

  const auto catching_func = InvalidTypeCatcher<decltype(store_result_wrapper), void>(store_result_wrapper);

  // The left-hand side is the column being aggregated.
  // The right-hand side is the temporary value required to calculate the aggregate value.
  // JIT_AGGREGATE_COMPUTE_CASE assumes that the types of the left-hand and right-hand side are the same.
  // However, this is not the case when the sum or average of a int or float column is calculated as the temporary sum
  // is stored in the according 64 bit data type long or double.
  // Left side |    Right side
  //   Column  | Sum/Avg | Min/Max
  //       Int |    Long |     Int
  //      Long |    Long |    Long
  //     Float |  Double |   Float
  //    Double |  Double |  Double
  //    String |       - |  String

  if (lhs.data_type() == DataType::Int && rhs.data_type() == DataType::Long) {
    return catching_func(static_cast<int64_t>(lhs.get<int32_t>(context)), rhs.get<int64_t>(rhs_index, context));
  } else if (lhs.data_type() == DataType::Float && rhs.data_type() == DataType::Double) {
    return catching_func(static_cast<double>(lhs.get<float>(context)), rhs.get<double>(rhs_index, context));
  }

  // else, catching_func is called here:
  switch (rhs.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_AGGREGATE_COMPUTE_CASE, (JIT_DATA_TYPE_INFO))
    default:
      break;
  }
}

// cleanup
#undef JIT_GET_ENUM_VALUE
#undef JIT_GET_DATA_TYPE
#undef JIT_COMPUTE_CASE
#undef JIT_COMPUTE_TYPE_CASE
#undef JIT_AGGREGATE_COMPUTE_CASE

}  // namespace opossum
