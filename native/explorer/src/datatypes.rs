use polars::prelude::*;
use rustler::resource::ResourceArc;
use rustler::{Encoder, Env, NifStruct, Term};
use std::sync::RwLock;

use std::result::Result;

pub struct ExDataFrameRef(pub RwLock<DataFrame>);
pub struct ExLazyFrameRef(pub RwLock<polars::lazy::frame::LazyFrame>);
pub struct ExSeriesRef(pub Series);

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.DataFrame"]
pub struct ExDataFrame {
    pub resource: ResourceArc<ExDataFrameRef>,
}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.Series"]
pub struct ExSeries {
    pub resource: ResourceArc<ExSeriesRef>,
}

#[derive(NifStruct)]
#[module = "Explorer.PolarsBackend.LazyFrame"]
pub struct ExLazyFrame {
    pub resource: ResourceArc<ExLazyFrameRef>,
}

impl ExDataFrameRef {
    pub fn new(df: DataFrame) -> Self {
        Self(RwLock::new(df))
    }
}

impl ExLazyFrameRef {
    pub fn new(lf: LazyFrame) -> Self {
        Self(RwLock::new(lf))
    }
}

impl ExSeriesRef {
    pub fn new(s: Series) -> Self {
        Self(s)
    }
}

impl ExDataFrame {
    pub fn new(df: DataFrame) -> Self {
        Self {
            resource: ResourceArc::new(ExDataFrameRef::new(df)),
        }
    }
}

impl ExSeries {
    pub fn new(s: Series) -> Self {
        Self {
            resource: ResourceArc::new(ExSeriesRef::new(s)),
        }
    }
}

impl ExLazyFrame {
    pub fn new(lf: LazyFrame) -> Self {
        Self {
            resource: ResourceArc::new(ExLazyFrameRef::new(lf)),
        }
    }
}

macro_rules! encode {
    ($s:ident, $env:ident, $convert_function:ident, $out_type:ty) => {
        $s.$convert_function()
            .unwrap()
            .into_iter()
            .map(|item| item)
            .collect::<Vec<Option<$out_type>>>()
            .encode($env)
    };
    ($s:ident, $env:ident, $convert_function:ident) => {
        $s.$convert_function()
            .unwrap()
            .into_iter()
            .map(|item| item)
            .collect::<Vec<Option<$convert_function>>>()
            .encode($env)
    };
}

macro_rules! encode_list {
    ($s:ident, $env:ident, $convert_function:ident, $out_type:ty) => {
        $s.list()
            .unwrap()
            .into_iter()
            .map(|item| item)
            .collect::<Vec<Option<Series>>>()
            .iter()
            .map(|item| {
                item.clone()
                    .unwrap()
                    .$convert_function()
                    .unwrap()
                    .into_iter()
                    .map(|item| item)
                    .collect::<Vec<Option<$out_type>>>()
            })
            .collect::<Vec<Vec<Option<$out_type>>>>()
            .encode($env)
    };
}

impl<'a> Encoder for ExSeriesRef {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let s = &self.0;
        match s.dtype() {
            DataType::Boolean => encode!(s, env, bool),
            DataType::Utf8 => encode!(s, env, utf8, &str),
            DataType::Int32 => encode!(s, env, i32),
            DataType::Int64 => encode!(s, env, i64),
            DataType::UInt32 => encode!(s, env, u32),
            DataType::Float64 => encode!(s, env, f64),
            DataType::Date32 => encode!(s, env, date32, i32),
            DataType::Date64 => encode!(s, env, date64, i64),
            DataType::List(ArrowDataType::UInt32) => encode_list!(s, env, u32, u32),
            dt => panic!("to_list/1 not implemented for {:?}", dt),
        }
    }
}
