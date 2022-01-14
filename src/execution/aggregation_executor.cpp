//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)), aht_{plan_->GetAggregates(), plan_->GetAggregateTypes()}, aht_iterator_{aht_.Begin()} {
    }

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
    //auto child_executor = GetChildExecutor();
    Tuple tuple;
    RID rid;
    while (child_->Next(&tuple, &rid)) {
        AggregateValue agg_value = MakeVal(&tuple);
        AggregateKey agg_key = MakeKey(&tuple);
        aht_.InsertCombine(agg_key, agg_value);
    }
    aht_iterator_ = aht_.Begin();
}

Tuple AggregationExecutor::GetOutputValue(std::vector<Value> &agg_vals, std::vector<Value> &group_bys) {
    std::vector<Value> values;
    for (auto column : GetOutputSchema()->GetColumns()) {
        values.push_back(column.GetExpr()->EvaluateAggregate(group_bys, agg_vals));
    }
    Tuple tmp(values, GetOutputSchema());
    return tmp;
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
    while (aht_iterator_ != aht_.End()) {
        std::vector<Value> agg_vals = aht_iterator_.Val().aggregates_;
        std::vector<Value> group_bys = aht_iterator_.Key().group_bys_;
        ++aht_iterator_;
        if (plan_->GetHaving() == nullptr || plan_->GetHaving()->EvaluateAggregate(group_bys, agg_vals).GetAs<bool>()) {
            *tuple = GetOutputValue(agg_vals, group_bys);
            return true;
        }
    }
    return false;
}

}  // namespace bustub
