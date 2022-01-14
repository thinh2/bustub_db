//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
        delete_table_metadata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
        delete_indexes_ = exec_ctx_->GetCatalog()->GetTableIndexes(delete_table_metadata_->name_);
    }

void DeleteExecutor::Init() {
    child_executor_->Init();
}

void DeleteExecutor::DeleteIndex(Tuple &tuple, RID &rid) {
    for (size_t i = 0; i < delete_indexes_.size(); i++) {
        Tuple key = tuple.KeyFromTuple(delete_table_metadata_->schema_, *(delete_indexes_[i]->index_->GetKeySchema()), delete_indexes_[i]->index_->GetKeyAttrs());
        delete_indexes_[i]->index_.get()->DeleteEntry(key, rid, exec_ctx_->GetTransaction());
    }
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
    // TODO: add delete for index also
    Tuple delete_tuple;
    bool success;
    
    success = child_executor_->Next(&delete_tuple, rid);
    if (!success) {
        return false;
    }
    
    DeleteIndex(delete_tuple, *rid);

    success = delete_table_metadata_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
    if (!success) {
        LOG_DEBUG("failed to mark delete");
        return false;
        // TODO: throw error later
    }

    return true;
}

}  // namespace bustub
