// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{
    new_null_array, ArrayRef, Int64Array, RecordBatch, StringViewArray,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use paimon::table::ObjectTable;

use crate::error::to_datafusion_error;

use super::datafusion_arrow_schema;

/// DataFusion provider for a native read-only Paimon object table.
#[derive(Debug, Clone)]
pub(crate) struct ObjectTableProvider {
    table: ObjectTable,
    schema: SchemaRef,
}

impl ObjectTableProvider {
    pub(crate) fn try_new(table: ObjectTable, schema_force_view_types: bool) -> DFResult<Self> {
        let schema = datafusion_arrow_schema(&ObjectTable::fields(), schema_force_view_types)?;
        Ok(Self { table, schema })
    }
}

#[async_trait]
impl TableProvider for ObjectTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut entries = self
            .table
            .list_objects()
            .await
            .map_err(to_datafusion_error)?;
        if let Some(limit) = limit {
            entries.truncate(limit);
        }

        let paths = entries
            .iter()
            .map(|entry| entry.path().to_string())
            .collect::<Vec<_>>();
        let names = entries
            .iter()
            .map(|entry| entry.name().to_string())
            .collect::<Vec<_>>();
        let lengths = entries
            .iter()
            .map(|entry| entry.length())
            .collect::<Vec<_>>();
        let mtimes = entries
            .iter()
            .map(|entry| entry.mtime())
            .collect::<Vec<_>>();
        let atimes = entries
            .iter()
            .map(|entry| entry.atime())
            .collect::<Vec<_>>();
        let owners = entries
            .iter()
            .map(|entry| entry.owner())
            .collect::<Vec<_>>();
        let row_count = entries.len();

        let string_array = |values: Vec<String>, index: usize| -> ArrayRef {
            if matches!(
                self.schema.field(index).data_type(),
                datafusion::arrow::datatypes::DataType::Utf8View
            ) {
                Arc::new(StringViewArray::from(values))
            } else {
                Arc::new(datafusion::arrow::array::StringArray::from(values))
            }
        };
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                string_array(paths, 0),
                string_array(names, 1),
                Arc::new(Int64Array::from(lengths)),
                Arc::new(Int64Array::from(mtimes)),
                Arc::new(Int64Array::from(atimes)),
                if owners.iter().all(Option::is_none) {
                    new_null_array(self.schema.field(5).data_type(), row_count)
                } else if matches!(
                    self.schema.field(5).data_type(),
                    datafusion::arrow::datatypes::DataType::Utf8View
                ) {
                    Arc::new(StringViewArray::from(owners))
                } else {
                    Arc::new(datafusion::arrow::array::StringArray::from(owners))
                },
            ],
        )?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?)
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        _input: Arc<dyn ExecutionPlan>,
        _insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::NotImplemented(format!(
            "Object table '{}' is read-only",
            self.table.identifier().full_name()
        )))
    }
}
