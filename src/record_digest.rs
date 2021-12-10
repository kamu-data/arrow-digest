use crate::arrow_shim::{datatypes::Schema, record_batch::RecordBatch};
use digest::{Digest, Output, OutputSizeUser};

use crate::{ArrayDigest, ArrayDigestV0, RecordDigest};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct RecordDigestV0<Dig: Digest> {
    columns: Vec<ArrayDigestV0<Dig>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<Dig: Digest> OutputSizeUser for RecordDigestV0<Dig> {
    type OutputSize = Dig::OutputSize;
}

impl<Dig: Digest> RecordDigest for RecordDigestV0<Dig> {
    fn digest(batch: &RecordBatch) -> Output<Dig> {
        let mut d = Self::new(batch.schema().as_ref());
        d.update(batch);
        d.finalize()
    }

    // TODO: Support nesting
    fn new(schema: &Schema) -> Self {
        let mut columns = Vec::new();

        for f in schema.fields() {
            let data_type = f.data_type();
            columns.push(ArrayDigestV0::new(data_type));
        }

        Self { columns }
    }

    fn update(&mut self, batch: &RecordBatch) {
        for (array, digest) in batch.columns().iter().zip(self.columns.iter_mut()) {
            digest.update(array.as_ref());
        }
    }

    fn finalize(self) -> Output<Dig> {
        let mut hasher = Dig::new();
        for c in self.columns {
            let column_hash = c.finalize();
            hasher.update(column_hash.as_slice());
        }
        hasher.finalize()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Tests
/////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use crate::arrow_shim::{
        array::{Array, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use sha3::Sha3_256;

    #[test]
    fn test_batch_mixed() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let a: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let c: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        let d: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "d"]));

        let record_batch1 =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)])
                .unwrap();
        let record_batch2 =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)])
                .unwrap();
        let record_batch3 =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&c), Arc::clone(&d)])
                .unwrap();

        assert_eq!(
            RecordDigestV0::<Sha3_256>::digest(&record_batch1),
            RecordDigestV0::<Sha3_256>::digest(&record_batch2),
        );

        assert_ne!(
            RecordDigestV0::<Sha3_256>::digest(&record_batch2),
            RecordDigestV0::<Sha3_256>::digest(&record_batch3),
        );
    }
}
