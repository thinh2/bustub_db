//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
        table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
        table_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_metadata_->name_);
    }

void InsertExecutor::Init() {
    if (child_executor_ != nullptr) {
        child_executor_->Init();
    }
}

void InsertExecutor::InsertIndex(Tuple &tuple, RID &rid) {
    for (size_t i = 0; i < table_indexes_.size(); i++) {
        Tuple key = tuple.KeyFromTuple(table_metadata_->schema_, *(table_indexes_[i]->index_->GetKeySchema()), table_indexes_[i]->index_->GetKeyAttrs());
        table_indexes_[i]->index_.get()->InsertEntry(key, rid, exec_ctx_->GetTransaction());
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    bool success = true;
    if (plan_->IsRawInsert()) {
        for (size_t idx = 0; idx < plan_->RawValues().size(); ++idx) {
            Tuple insert_tuple(plan_->RawValuesAt(idx), &table_metadata_->schema_);
            success = table_metadata_->table_->InsertTuple(insert_tuple, rid, exec_ctx_->GetTransaction());
            if (!success) {
                return success;
            }
            InsertIndex(insert_tuple, *rid);
        }
    } else {
        std::unique_ptr<Tuple> tmp = std::make_unique<Tuple>();
        success = child_executor_->Next(tmp.get(), rid);
        if (!success) {
            return false;
        }
        success = table_metadata_->table_->InsertTuple(*tmp, rid, exec_ctx_->GetTransaction());
        if (!success) {
            return false;
        }
        InsertIndex(*tmp, *rid);
        return true;
    }
    return false;
}

}  // namespace bustub
