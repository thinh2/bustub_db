//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan) {
  auto catalog = exec_ctx_->GetCatalog();
  table_metadata_ = catalog->GetTable(plan_->GetTableOid());
  table_iterator_ = (table_metadata_->table_)->Begin(exec_ctx_->GetTransaction());
}

void SeqScanExecutor::Init() {
}

Tuple SeqScanExecutor::GenerateOutputValue(const Tuple *tuple, const Schema *tuple_schema) {
  std::vector<Value> values;
  values.reserve(GetOutputSchema()->GetColumnCount());
  for (auto column : GetOutputSchema()->GetColumns()) {
    values.push_back(column.GetExpr()->Evaluate(tuple, tuple_schema));
  }
  return Tuple(values, GetOutputSchema());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (plan_->GetPredicate() != nullptr && 
          table_iterator_ != table_metadata_->table_->End() &&
          plan_->GetPredicate()->Evaluate(&(*table_iterator_), &table_metadata_->schema_).GetAs<bool>() == false) {
            table_iterator_++;
  }

  if (table_iterator_ == table_metadata_->table_->End()) {
    return false;
  }

  *tuple = GenerateOutputValue(&(*table_iterator_), &table_metadata_->schema_);
  *rid = table_iterator_->GetRid();
  table_iterator_++;
  return true;
}

}  // namespace bustub
