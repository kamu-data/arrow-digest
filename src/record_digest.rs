use crate::bitmap_slice::BitmapSlice;
use crate::{ArrayDigest, ArrayDigestV0, RecordDigest};
use arrow::{
    array::{Array, ArrayRef, StructArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use digest::{Digest, Output, OutputSizeUser};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct RecordDigestV0<Dig: Digest> {
    columns: Vec<ArrayDigestV0<Dig>>,
    hasher: Dig,
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

    fn new(schema: &Schema) -> Self {
        let mut hasher = Dig::new();
        let mut columns = Vec::new();

        Self::walk_nested_fields(schema.fields(), 0, &mut |field, level| {
            hasher.update(&(field.name().len() as u64).to_le_bytes());
            hasher.update(field.name().as_bytes());
            hasher.update(&(level as u64).to_le_bytes());

            match field.data_type() {
                DataType::Struct(_) => (),
                _ => columns.push(ArrayDigestV0::new(field.data_type())),
            }
        });

        Self { columns, hasher }
    }

    fn update(&mut self, batch: &RecordBatch) {
        let mut col_index = 0;
        Self::walk_nested_columns(
            batch.columns().iter(),
            None,
            &mut |array, parent_null_bitmap| {
                let col_digest = &mut self.columns[col_index];
                col_digest.update(array.as_ref(), parent_null_bitmap);
                col_index += 1;
            },
        );
    }

    fn finalize(mut self) -> Output<Dig> {
        for c in self.columns {
            let column_hash = c.finalize();
            self.hasher.update(column_hash.as_slice());
        }
        self.hasher.finalize()
    }
}

impl<Dig: Digest> RecordDigestV0<Dig> {
    fn walk_nested_fields<'a>(fields: &[Field], level: usize, fun: &mut impl FnMut(&Field, usize)) {
        for field in fields {
            match field.data_type() {
                DataType::Struct(nested_fields) => {
                    fun(field, level);
                    Self::walk_nested_fields(nested_fields, level + 1, fun);
                }
                _ => fun(field, level),
            }
        }
    }

    fn walk_nested_columns<'a>(
        arrays: impl Iterator<Item = &'a ArrayRef>,
        parent_null_bitmap: Option<BitmapSlice>,
        fun: &mut impl FnMut(&ArrayRef, Option<BitmapSlice>),
    ) {
        for array in arrays {
            match array.data_type() {
                DataType::Struct(_) => {
                    let array = array.as_any().downcast_ref::<StructArray>().unwrap();

                    let combined_null_bitmap = if array.null_count() == 0 {
                        parent_null_bitmap.clone()
                    } else {
                        let own = BitmapSlice::from_null_bitmap(array.data()).unwrap();
                        if let Some(parent) = &parent_null_bitmap {
                            Some(&own & parent)
                        } else {
                            Some(own)
                        }
                    };

                    for i in 0..array.num_columns() {
                        Self::walk_nested_columns(
                            [array.column(i)].into_iter(),
                            combined_null_bitmap.clone(),
                            fun,
                        );
                    }
                }
                _ => fun(array, parent_null_bitmap.clone()),
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Tests
/////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, Int32Array, StringArray},
        buffer::Buffer,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use sha3::Sha3_256;
    use std::sync::Arc;

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

    #[test]
    fn test_batch_nested() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new(
                "b",
                DataType::Struct(vec![
                    Field::new("c", DataType::Utf8, false),
                    Field::new("d", DataType::Int32, false),
                ]),
                false,
            ),
        ]));

        let a: Arc<dyn Array> = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let c: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let d: Arc<dyn Array> = Arc::new(Int32Array::from(vec![3, 2, 1]));
        let b = Arc::new(StructArray::from(vec![
            (Field::new("c", DataType::Utf8, false), c.clone()),
            (Field::new("d", DataType::Int32, false), d.clone()),
        ]));

        let record_batch1 = RecordBatch::try_new(schema, vec![a.clone(), b.clone()]).unwrap();

        assert_eq!(
            RecordDigestV0::<sha3::Sha3_256>::digest(&record_batch1),
            RecordDigestV0::<sha3::Sha3_256>::digest(&record_batch1),
        );

        // Different column name
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new(
                "bee",
                DataType::Struct(vec![
                    Field::new("c", DataType::Utf8, false),
                    Field::new("d", DataType::Int32, false),
                ]),
                false,
            ),
        ]));

        let record_batch2 = RecordBatch::try_new(schema, vec![a.clone(), b.clone()]).unwrap();

        assert_ne!(
            RecordDigestV0::<sha3::Sha3_256>::digest(&record_batch1),
            RecordDigestV0::<sha3::Sha3_256>::digest(&record_batch2),
        );

        // Nullability - equal
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new(
                "b",
                DataType::Struct(vec![
                    Field::new("c", DataType::Utf8, false),
                    Field::new("d", DataType::Int32, false),
                ]),
                true,
            ),
        ]));

        let b = Arc::new(StructArray::from((
            vec![
                (Field::new("c", DataType::Utf8, false), c.clone()),
                (Field::new("d", DataType::Int32, false), d.clone()),
            ],
            Buffer::from([0b111]),
        )));

        let record_batch3 =
            RecordBatch::try_new(schema.clone(), vec![a.clone(), b.clone()]).unwrap();

        assert_eq!(
            RecordDigestV0::<sha3::Sha3_256>::digest(&record_batch1),
            RecordDigestV0::<sha3::Sha3_256>::digest(&record_batch3),
        );

        // Nullability - not equal
        let b = Arc::new(StructArray::from((
            vec![
                (Field::new("c", DataType::Utf8, false), c.clone()),
                (Field::new("d", DataType::Int32, false), d.clone()),
            ],
            Buffer::from([0b101]),
        )));

        let record_batch4 =
            RecordBatch::try_new(schema.clone(), vec![a.clone(), b.clone()]).unwrap();

        assert_ne!(
            RecordDigestV0::<sha3::Sha3_256>::digest(&record_batch1),
            RecordDigestV0::<sha3::Sha3_256>::digest(&record_batch4),
        );
    }

    /*#[test]
    fn test_batch_parquet() {
        use crate::{RecordDigest, RecordDigestV0};
        use parquet::arrow::ArrowReader;
        use parquet::arrow::ParquetFileArrowReader;
        use parquet::file::reader::SerializedFileReader;

        let file = std::fs::File::open(
            ".priv/97dfa84bb29db02b46cb33f6e8a7e51be3f15b3bbdac2e3e61849dcf5c67de6b",
        )
        .unwrap();
        let parquet_reader = SerializedFileReader::new(file).unwrap();
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(parquet_reader));

        println!("{:?}", arrow_reader.get_schema());

        let mut hasher = RecordDigestV0::<sha3::Sha3_256>::new(&arrow_reader.get_schema().unwrap());

        for res_batch in arrow_reader.get_record_reader(100000).unwrap() {
            let batch = res_batch.unwrap();
            println!(".");
            hasher.update(&batch);
            println!("x");
        }

        println!("{:x}", hasher.finalize());
    }*/
}
