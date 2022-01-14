//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
    if (left_executor_ != nullptr) {
        left_executor_->Init();
    }
    if (right_executor_ != nullptr) {
        right_executor_->Init();
    }
}

Tuple NestedLoopJoinExecutor::GenerateOutputTuple(const Tuple *left_tuple, const Schema *left_schema, const Tuple *right_tuple, const Schema *right_schema) {
    std::vector<Value> values;
    values.reserve(GetOutputSchema()->GetColumnCount());
    for (auto column : GetOutputSchema()->GetColumns()) {
        values.push_back(column.GetExpr()->EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema));
    }
    return Tuple(values, GetOutputSchema());
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
    //TODO: extract the tuple value from left_tuple and right_tuple
    RID tmp_rid;
    while (left_executor_->Next(&left_tuple_, &tmp_rid)) {
        while (right_executor_->Next(&right_tuple_, &tmp_rid)) {
            if (plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_, right_executor_->GetOutputSchema()).GetAs<bool>()) {
                *tuple = GenerateOutputTuple(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_, right_executor_->GetOutputSchema());
                return true;
            }
        }
        right_executor_->Init();
    }
    return false; 
}

}  // namespace bustub
