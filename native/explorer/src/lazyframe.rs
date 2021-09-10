use polars::prelude::*;
use std::result::Result;

use crate::{ExDataFrame, ExLazyFrame, ExplorerError};

macro_rules! lf_read {
    ($data: ident, $df: ident, $body: block) => {
        match $data.resource.0.read() {
            Ok($df) => $body,
            Err(_) => Err(ExplorerError::Internal(
                "Failed to take read lock for df".into(),
            )),
        }
    };
}

macro_rules! lf_read_read {
    ($data: ident, $other: ident, $df: ident, $df1: ident, $body: block) => {
        match ($data.resource.0.read(), $other.resource.0.read()) {
            (Ok($df), Ok($df1)) => $body,
            _ => Err(ExplorerError::Internal(
                "Failed to take read locks for left and right".into(),
            )),
        }
    };
}

#[rustler::nif]
pub fn lf_cache(data: ExLazyFrame) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, { Ok(ExLazyFrame::new(lf.clone().cache())) })
}

#[rustler::nif]
pub fn lf_collect(data: ExLazyFrame) -> Result<ExDataFrame, ExplorerError> {
    lf_read!(data, lf, { Ok(ExDataFrame::new(lf.clone().collect()?)) })
}

#[rustler::nif]
pub fn lf_lazy(data: ExDataFrame) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, df, { 
        Ok(ExLazyFrame::new(df.clone().lazy()))
    })
}

#[rustler::nif]
pub fn lf_select(data: ExLazyFrame, selection: Vec<&str>) -> Result<ExLazyFrame, ExplorerError> {
    lf_read!(data, lf, {
        let cols: Vec<Expr> = selection.iter().map(|s| col(s)).collect();
        let new_lf = lf.clone().select(cols);

        Ok(ExLazyFrame::new(new_lf))
    })
}

#[rustler::nif]
pub fn lf_describe_optimized_plan(data: ExLazyFrame) -> Result<String, ExplorerError> {
    lf_read!(data, lf, {
        let plan = &lf.describe_optimized_plan()?;

        Ok(plan.to_string())
    })
}

#[rustler::nif]
pub fn lf_describe_plan(data: ExLazyFrame) -> Result<String, ExplorerError> {
    lf_read!(data, lf, { Ok(lf.describe_plan()) })
}