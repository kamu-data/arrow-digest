use arrow::{
    array::{Array, GenericStringArray, LargeStringArray, StringArray, StringOffsetSizeTrait},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use digest::{Digest, Output};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct RecordsDigestV0<Dig: Digest> {
    columns: Vec<ArrayDigestV0<Dig>>,
}

pub struct ArrayDigestV0<Dig: Digest> {
    fixed_size: Option<usize>,
    hasher: Dig,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<Dig: Digest> RecordsDigestV0<Dig> {
    // TODO: Support nesting
    pub fn new(schema: &Schema) -> Self {
        let mut columns = Vec::new();

        for f in schema.fields() {
            let data_type = f.data_type();
            columns.push(ArrayDigestV0::new(data_type));
        }

        Self { columns }
    }

    pub fn update(&mut self, batch: &RecordBatch) {
        for (array, digest) in batch.columns().iter().zip(self.columns.iter_mut()) {
            digest.update(array.as_ref());
        }
    }

    pub fn finalize(self) -> Output<Dig> {
        let mut hasher = Dig::new();
        for c in self.columns {
            let column_hash = c.finalize();
            hasher.update(column_hash.as_slice());
        }
        hasher.finalize()
    }

    pub fn digest(batch: &RecordBatch) -> Output<Dig> {
        let mut d = Self::new(batch.schema().as_ref());
        d.update(batch);
        d.finalize()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<Dig: Digest> ArrayDigestV0<Dig> {
    // TODO: Include schema into the hash
    pub fn new(data_type: &DataType) -> Self {
        let fixed_size = match data_type {
            DataType::Int8 => Some(1),
            DataType::Int16 => Some(2),
            DataType::Int32 => Some(4),
            DataType::Int64 => Some(8),
            DataType::UInt8 => Some(1),
            DataType::UInt16 => Some(2),
            DataType::UInt32 => Some(4),
            DataType::UInt64 => Some(8),
            DataType::Float16 => Some(2),
            DataType::Float32 => Some(4),
            DataType::Float64 => Some(8),
            DataType::Timestamp(_, _) => Some(8),
            DataType::Date32 => Some(4),
            DataType::Date64 => Some(8),
            DataType::Time32(_) => Some(4),
            DataType::Time64(_) => Some(8),
            DataType::Decimal(_, _) => Some(16),
            _ => None,
        };

        Self {
            fixed_size,
            hasher: Dig::new(),
        }
    }

    pub fn digest(array: &dyn Array) -> Output<Dig> {
        let mut d = Self::new(array.data_type());
        d.update(array);
        d.finalize()
    }

    fn update(&mut self, array: &dyn Array) {
        let data_type = array.data_type();

        if self.fixed_size.is_some() {
            self.hash_fixed_size(array)
        } else if *data_type == DataType::Utf8 {
            self.hash_array_string(array.as_any().downcast_ref::<StringArray>().unwrap());
        } else if *data_type == DataType::LargeUtf8 {
            self.hash_array_string(array.as_any().downcast_ref::<LargeStringArray>().unwrap());
        } else {
            unimplemented!("Type {} is not yet supported", data_type);
        }
    }

    pub fn finalize(self) -> Output<Dig> {
        self.hasher.finalize()
    }

    fn hash_fixed_size(&mut self, array: &dyn Array) {
        let item_size = self.fixed_size.unwrap();

        // Ensure single buffer
        assert_eq!(array.data().buffers().len(), 1);

        let buf = &array.data().buffers()[0];

        // Ensure no padding
        assert_eq!(array.len() * item_size, buf.len());

        if array.null_count() == 0 {
            // In case of no nulls we can hash the whole buffer in one go
            let slice = buf.as_slice();
            self.hasher.update(slice);
        } else {
            // Otherwise have to go element-by-element
            let slice = buf.as_slice();
            let bitmap = array.data().null_bitmap().as_ref().unwrap();
            for i in 0..array.len() {
                if bitmap.is_set(i) {
                    let pos = i * item_size;
                    self.hasher.update(&slice[pos..pos + item_size]);
                } else {
                    self.hasher.update(&[0]);
                }
            }
        }
    }

    fn hash_array_string<Off: StringOffsetSizeTrait>(&mut self, array: &GenericStringArray<Off>) {
        if array.null_count() == 0 {
            for i in 0..array.len() {
                let s = array.value(i);
                self.hasher.update(&(s.len() as u64).to_le_bytes());
                self.hasher.update(s.as_bytes());
            }
        } else {
            let bitmap = array.data().null_bitmap().as_ref().unwrap();
            for i in 0..array.len() {
                if bitmap.is_set(i) {
                    let s = array.value(i);
                    self.hasher.update(&(s.len() as u64).to_le_bytes());
                    self.hasher.update(s.as_bytes());
                } else {
                    self.hasher.update(&[0]);
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Tests
/////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{Field, Schema},
    };
    use sha3::Sha3_256;

    #[test]
    fn test_ints() {
        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&Int32Array::from(vec![1, 2, 3])),
            ArrayDigestV0::<Sha3_256>::digest(&Int32Array::from(vec![1, 2, 3])),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&Int32Array::from(vec![1, 2, 3]),),
            ArrayDigestV0::<Sha3_256>::digest(&Int32Array::from(vec![
                Some(1),
                Some(2),
                None,
                Some(3)
            ]),),
        );
    }

    #[test]
    fn test_string_array() {
        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "baz"]),),
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "baz"]),),
        );

        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "baz"]),),
            ArrayDigestV0::<Sha3_256>::digest(&LargeStringArray::from(vec!["foo", "bar", "baz"]),),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "baz"]),),
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "", "baz"]),),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec![
                Some("foo"),
                Some("bar"),
                Some("baz")
            ]),),
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec![
                Some("foo"),
                Some("bar"),
                None,
                Some("baz")
            ]),),
        );
    }

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
            RecordsDigestV0::<Sha3_256>::digest(&record_batch1),
            RecordsDigestV0::<Sha3_256>::digest(&record_batch2),
        );

        assert_ne!(
            RecordsDigestV0::<Sha3_256>::digest(&record_batch2),
            RecordsDigestV0::<Sha3_256>::digest(&record_batch3),
        );
    }
}
