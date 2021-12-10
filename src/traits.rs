use crate::arrow_shim::{
    array::Array,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use digest::{Output, OutputSizeUser};

pub trait RecordDigest: OutputSizeUser {
    fn digest(batch: &RecordBatch) -> Output<Self>;
    fn new(schema: &Schema) -> Self;
    fn update(&mut self, batch: &RecordBatch);
    fn finalize(self) -> Output<Self>;
}

pub trait ArrayDigest: OutputSizeUser {
    fn digest(array: &dyn Array) -> Output<Self>;
    fn new(data_type: &DataType) -> Self;
    fn update(&mut self, array: &dyn Array);
    fn finalize(self) -> Output<Self>;
}
