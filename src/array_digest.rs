use crate::ArrayDigest;
use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray,
        GenericBinaryArray, GenericListArray, GenericStringArray, LargeBinaryArray, LargeListArray,
        LargeStringArray, ListArray, OffsetSizeTrait, StringArray,
    },
    buffer::NullBuffer,
    datatypes::DataType,
};
use digest::{Digest, Output, OutputSizeUser};

/////////////////////////////////////////////////////////////////////////////////////////
pub struct ArrayDigestV0<Dig: Digest> {
    hasher: Dig,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<Dig: Digest> OutputSizeUser for ArrayDigestV0<Dig> {
    type OutputSize = Dig::OutputSize;
}

impl<Dig: Digest> ArrayDigest for ArrayDigestV0<Dig> {
    fn digest(array: &dyn Array) -> Output<Dig> {
        let mut d = Self::new(array.data_type());
        d.update(array, None);
        d.finalize()
    }

    fn new(data_type: &DataType) -> Self {
        let mut hasher = Dig::new();
        crate::schema_digest::hash_data_type(data_type, &mut hasher);
        Self { hasher }
    }

    fn update(&mut self, array: &dyn Array, parent_null_bitmap: Option<&NullBuffer>) {
        let combined_null_bitmap_val =
            crate::utils::maybe_combine_null_buffers(parent_null_bitmap, array.data().nulls());
        let combined_null_bitmap = combined_null_bitmap_val.as_option();

        let data_type = array.data_type();

        #[inline]
        fn unsupported(data_type: &DataType) -> ! {
            unimplemented!("Type {} is not yet supported", data_type);
        }

        match data_type {
            DataType::Null => unsupported(data_type),
            DataType::Boolean => self.hash_array_bool(array, combined_null_bitmap),
            DataType::Int8 | DataType::UInt8 => {
                self.hash_fixed_size(array, 1, combined_null_bitmap)
            }
            DataType::Int16 | DataType::UInt16 => {
                self.hash_fixed_size(array, 2, combined_null_bitmap)
            }
            DataType::Int32 | DataType::UInt32 => {
                self.hash_fixed_size(array, 4, combined_null_bitmap)
            }
            DataType::Int64 | DataType::UInt64 => {
                self.hash_fixed_size(array, 8, combined_null_bitmap)
            }
            DataType::Float16 => self.hash_fixed_size(array, 2, combined_null_bitmap),
            DataType::Float32 => self.hash_fixed_size(array, 4, combined_null_bitmap),
            DataType::Float64 => self.hash_fixed_size(array, 8, combined_null_bitmap),
            DataType::Timestamp(_, _) => self.hash_fixed_size(array, 8, combined_null_bitmap),
            DataType::Date32 => self.hash_fixed_size(array, 4, combined_null_bitmap),
            DataType::Date64 => self.hash_fixed_size(array, 8, combined_null_bitmap),
            DataType::Time32(_) => self.hash_fixed_size(array, 4, combined_null_bitmap),
            DataType::Time64(_) => self.hash_fixed_size(array, 8, combined_null_bitmap),
            DataType::Duration(_) => unsupported(data_type),
            DataType::Interval(_) => unsupported(data_type),
            DataType::Binary => self.hash_array_binary(
                array.as_any().downcast_ref::<BinaryArray>().unwrap(),
                combined_null_bitmap,
            ),
            DataType::LargeBinary => self.hash_array_binary(
                array.as_any().downcast_ref::<LargeBinaryArray>().unwrap(),
                combined_null_bitmap,
            ),
            DataType::FixedSizeBinary(size) => {
                self.hash_array_binary_fixed(
                    array.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap(),
                    *size as usize,
                    combined_null_bitmap,
                )
            },
            DataType::Utf8 => self.hash_array_string(
                array.as_any().downcast_ref::<StringArray>().unwrap(),
                combined_null_bitmap,
            ),
            DataType::LargeUtf8 => self.hash_array_string(
                array.as_any().downcast_ref::<LargeStringArray>().unwrap(),
                combined_null_bitmap,
            ),
            DataType::List(_) => self.hash_array_list(
                array.as_any().downcast_ref::<ListArray>().unwrap(),
                combined_null_bitmap,
            ),
            DataType::LargeList(_) => self.hash_array_list(
                array.as_any().downcast_ref::<LargeListArray>().unwrap(),
                combined_null_bitmap,
            ),
            DataType::FixedSizeList(..) => self.hash_array_list_fixed(
                array.as_any().downcast_ref::<FixedSizeListArray>().unwrap(),
                combined_null_bitmap,
            ),
            // TODO: Should structs be handled by array digest to allow use without record hasher?
            DataType::Struct(_) => panic!(
                "Structs are currently flattened by RecordDigest and cannot be processed by ArrayDigest"
            ),
            DataType::Union(_, _, _) => unsupported(data_type),
            DataType::Dictionary(..) => unsupported(data_type),
            DataType::Decimal128(_, _) => self.hash_fixed_size(array, 16, combined_null_bitmap),
            DataType::Decimal256(_, _) => self.hash_fixed_size(array, 32, combined_null_bitmap),
            DataType::Map(..) => unsupported(data_type),
            DataType::RunEndEncoded(..) => unsupported(data_type),
        }
    }

    fn finalize(self) -> Output<Dig> {
        self.hasher.finalize()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<Dig: Digest> ArrayDigestV0<Dig> {
    const NULL_MARKER: [u8; 1] = [0];

    fn hash_fixed_size(
        &mut self,
        array: &dyn Array,
        item_size: usize,
        null_bitmap: Option<&NullBuffer>,
    ) {
        // Ensure single buffer
        assert_eq!(
            array.data().buffers().len(),
            1,
            "Multiple buffers on a primitive type array"
        );

        let slice = {
            let data_start = array.data().offset() * item_size;
            let data_end = data_start + array.data().len() * item_size;
            &array.data().buffers()[0].as_slice()[data_start..data_end]
        };

        match null_bitmap {
            None => {
                // In case of no nulls we can hash the whole buffer in one go
                self.hasher.update(slice);
            }
            Some(null_bitmap) => {
                // Otherwise have to go element-by-element
                for i in 0..array.len() {
                    if null_bitmap.is_valid(i) {
                        let pos = i * item_size;
                        self.hasher.update(&slice[pos..pos + item_size]);
                    } else {
                        self.hasher.update(&Self::NULL_MARKER);
                    }
                }
            }
        }
    }

    // TODO: PERF: Hashing bool bitmaps is expensive because we have to deal with offsets
    fn hash_array_bool(&mut self, array: &dyn Array, null_bitmap: Option<&NullBuffer>) {
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        match null_bitmap {
            None => {
                for i in 0..bool_array.len() {
                    // Safety: boundary check is right above
                    let value = unsafe { bool_array.value_unchecked(i) };
                    self.hasher.update(&[value as u8 + 1]);
                }
            }
            Some(null_bitmap) => {
                for i in 0..bool_array.len() {
                    if null_bitmap.is_valid(i) {
                        // Safety: boundary check is right above
                        let value = unsafe { bool_array.value_unchecked(i) };
                        self.hasher.update(&[value as u8 + 1]);
                    } else {
                        self.hasher.update(&Self::NULL_MARKER);
                    }
                }
            }
        }
    }

    fn hash_array_string<OffsetSize: OffsetSizeTrait>(
        &mut self,
        array: &GenericStringArray<OffsetSize>,
        null_bitmap: Option<&NullBuffer>,
    ) {
        match null_bitmap {
            None => {
                for i in 0..array.len() {
                    let s = array.value(i);
                    self.hasher.update(&(s.len() as u64).to_le_bytes());
                    self.hasher.update(s.as_bytes());
                }
            }
            Some(null_bitmap) => {
                for i in 0..array.len() {
                    if null_bitmap.is_valid(i) {
                        let s = array.value(i);
                        self.hasher.update(&(s.len() as u64).to_le_bytes());
                        self.hasher.update(s.as_bytes());
                    } else {
                        self.hasher.update(&Self::NULL_MARKER);
                    }
                }
            }
        }
    }

    fn hash_array_binary<OffsetSize: OffsetSizeTrait>(
        &mut self,
        array: &GenericBinaryArray<OffsetSize>,
        null_bitmap: Option<&NullBuffer>,
    ) {
        match null_bitmap {
            None => {
                for i in 0..array.len() {
                    let slice = array.value(i);
                    self.hasher.update(&(slice.len() as u64).to_le_bytes());
                    self.hasher.update(slice);
                }
            }
            Some(null_bitmap) => {
                for i in 0..array.len() {
                    if null_bitmap.is_valid(i) {
                        let slice = array.value(i);
                        self.hasher.update(&(slice.len() as u64).to_le_bytes());
                        self.hasher.update(slice);
                    } else {
                        self.hasher.update(&Self::NULL_MARKER);
                    }
                }
            }
        }
    }

    fn hash_array_binary_fixed(
        &mut self,
        array: &FixedSizeBinaryArray,
        size: usize,
        null_bitmap: Option<&NullBuffer>,
    ) {
        match null_bitmap {
            None => {
                for i in 0..array.len() {
                    let slice = array.value(i);
                    self.hasher.update(&(size as u64).to_le_bytes());
                    self.hasher.update(slice);
                }
            }
            Some(null_bitmap) => {
                for i in 0..array.len() {
                    if null_bitmap.is_valid(i) {
                        let slice = array.value(i);
                        self.hasher.update(&(size as u64).to_le_bytes());
                        self.hasher.update(slice);
                    } else {
                        self.hasher.update(&Self::NULL_MARKER);
                    }
                }
            }
        }
    }

    fn hash_array_list<Off: OffsetSizeTrait>(
        &mut self,
        array: &GenericListArray<Off>,
        null_bitmap: Option<&NullBuffer>,
    ) {
        match null_bitmap {
            None => {
                for i in 0..array.len() {
                    let sub_array = array.value(i);
                    self.hasher.update(&(sub_array.len() as u64).to_le_bytes());
                    self.update(sub_array.as_ref(), None);
                }
            }
            Some(null_bitmap) => {
                for i in 0..array.len() {
                    if null_bitmap.is_valid(i) {
                        let sub_array = array.value(i);
                        self.hasher.update(&(sub_array.len() as u64).to_le_bytes());
                        self.update(sub_array.as_ref(), None);
                    } else {
                        self.hasher.update(&Self::NULL_MARKER);
                    }
                }
            }
        }
    }

    fn hash_array_list_fixed(
        &mut self,
        array: &FixedSizeListArray,
        null_bitmap: Option<&NullBuffer>,
    ) {
        match null_bitmap {
            None => {
                for i in 0..array.len() {
                    let sub_array = array.value(i);
                    self.hasher.update(&(sub_array.len() as u64).to_le_bytes());
                    self.update(sub_array.as_ref(), None);
                }
            }
            Some(null_bitmap) => {
                for i in 0..array.len() {
                    if null_bitmap.is_valid(i) {
                        let sub_array = array.value(i);
                        self.hasher.update(&(sub_array.len() as u64).to_le_bytes());
                        self.update(sub_array.as_ref(), None);
                    } else {
                        self.hasher.update(&Self::NULL_MARKER);
                    }
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
    use super::*;
    use arrow::{
        array::{
            ArrayData, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int32Array, StringArray,
            UInt32Array,
        },
        buffer::Buffer,
        datatypes::Int32Type,
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
    fn test_int_array() {
        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&Int32Array::from(vec![1, 2, 3])),
            ArrayDigestV0::<Sha3_256>::digest(&Int32Array::from(vec![1, 2, 3])),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&Int32Array::from(vec![1, 2, 3])),
            ArrayDigestV0::<Sha3_256>::digest(&UInt32Array::from(vec![1, 2, 3])),
        );
    }

    #[test]
    fn test_bool_array() {
        fn make_bool_array(data: Vec<u8>, len: usize, nulls: Option<Vec<u8>>) -> BooleanArray {
            let builder = ArrayData::builder(DataType::Boolean)
                .len(len)
                .add_buffer(Buffer::from(data));

            let builder = if let Some(nulls) = nulls {
                builder.null_bit_buffer(Some(Buffer::from(nulls)))
            } else {
                builder
            };

            BooleanArray::from(builder.build().unwrap())
        }

        let array1 = make_bool_array(vec![0b0011_0101u8], 6, None);
        let array2 = make_bool_array(vec![0b1111_0101u8], 6, None);

        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&array1),
            ArrayDigestV0::<Sha3_256>::digest(&BooleanArray::from(vec![
                true, false, true, false, true, true
            ])),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&array1),
            ArrayDigestV0::<Sha3_256>::digest(&BooleanArray::from(vec![
                true, false, true, false, true, false
            ])),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&array1),
            ArrayDigestV0::<Sha3_256>::digest(&BooleanArray::from(vec![
                false, false, true, false, true, true
            ])),
        );

        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&array1),
            ArrayDigestV0::<Sha3_256>::digest(&array2),
        );

        let array3 = make_bool_array(vec![0b1111_0101u8], 6, Some(vec![0b1110_1110]));

        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&array3),
            ArrayDigestV0::<Sha3_256>::digest(&BooleanArray::from(vec![
                None,
                Some(false),
                Some(true),
                Some(false),
                None,
                Some(true)
            ])),
        );
    }

    #[test]
    fn test_string_array() {
        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "baz"])),
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "baz"])),
        );

        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "baz"])),
            ArrayDigestV0::<Sha3_256>::digest(&LargeStringArray::from(vec!["foo", "bar", "baz"])),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "baz"])),
            ArrayDigestV0::<Sha3_256>::digest(&StringArray::from(vec!["foo", "bar", "", "baz"])),
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
    fn test_list_array() {
        /*let a = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(4), Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ]);

        let n = a.data().nulls().unwrap();
        println!(
            "top level: len {}, offset {}, count {}",
            n.len(),
            n.offset(),
            n.null_count()
        );

        let sub = a.value(3);
        let sn = sub.data().nulls().unwrap();
        println!(
            "sub level: len {}, offset {}, count {}",
            sn.len(),
            sn.offset(),
            sn.null_count()
        );
        return;*/

        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    Some(vec![Some(3), None, Some(4), Some(5)]),
                    Some(vec![Some(6), Some(7)]),
                ]
            )),
            ArrayDigestV0::<Sha3_256>::digest(&ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    Some(vec![Some(3), None, Some(4), Some(5)]),
                    Some(vec![Some(6), Some(7)]),
                ]
            )),
        );

        // Different primitive value
        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    Some(vec![Some(3), None, Some(4), Some(5)]),
                    Some(vec![Some(6), Some(7)]),
                ]
            )),
            ArrayDigestV0::<Sha3_256>::digest(&ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    Some(vec![Some(3), None, Some(4), Some(100)]),
                    Some(vec![Some(6), Some(7)]),
                ]
            )),
        );

        // Value slides to the next list
        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    Some(vec![Some(3), None, Some(4), Some(5)]),
                    Some(vec![Some(6), Some(7)]),
                ]
            )),
            ArrayDigestV0::<Sha3_256>::digest(&ListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    Some(vec![Some(3), None, Some(4)]),
                    Some(vec![Some(5), Some(6), Some(7)]),
                ]
            )),
        );
    }

    #[test]
    fn test_binary_array() {
        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_vec(vec![
                b"one", b"two", b"", b"three"
            ])),
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_vec(vec![
                b"one", b"two", b"", b"three"
            ])),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_vec(vec![
                b"one", b"two", b"", b"three"
            ])),
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_vec(vec![
                b"one", b"two", b"three"
            ])),
        );

        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_vec(vec![
                b"one", b"two", b"", b"three"
            ])),
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_opt_vec(vec![
                Some(b"one"),
                Some(b"two"),
                Some(b""),
                Some(b"three")
            ])),
        );

        assert_ne!(
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_vec(vec![
                b"one", b"two", b"", b"three"
            ])),
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_opt_vec(vec![
                Some(b"one"),
                Some(b"two"),
                None,
                Some(b"three")
            ])),
        );

        assert_eq!(
            ArrayDigestV0::<Sha3_256>::digest(&BinaryArray::from_vec(vec![b"one", b"two"])),
            ArrayDigestV0::<Sha3_256>::digest(&FixedSizeBinaryArray::from(
                ArrayData::builder(DataType::FixedSizeBinary(3))
                    .len(2)
                    .add_buffer(Buffer::from(b"onetwo"))
                    .build()
                    .unwrap()
            )),
        );
    }
}
