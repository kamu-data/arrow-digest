use crate::{
    arrow_shim::{
        array::{
            Array, BooleanArray, FixedSizeListArray, GenericListArray, GenericStringArray,
            LargeListArray, LargeStringArray, ListArray, OffsetSizeTrait, StringArray,
            StringOffsetSizeTrait,
        },
        datatypes::DataType,
    },
    bitmap_slice::BitmapSlice,
};
use digest::{Digest, Output, OutputSizeUser};

use crate::ArrayDigest;

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

    fn update(&mut self, array: &dyn Array, parent_null_bitmap: Option<BitmapSlice>) {
        let combined_null_bitmap = if array.null_count() == 0 {
            parent_null_bitmap
        } else {
            let own = BitmapSlice::from_null_bitmap(array.data()).unwrap();
            if let Some(parent) = &parent_null_bitmap {
                Some(&own & parent)
            } else {
                Some(own)
            }
        };

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
            DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => {
                unsupported(data_type)
            }
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
            DataType::Struct(_) => panic!("Structs are currently flattened by RecordDigest and cannot be processed by ArrayDigest"),
            DataType::Union(_) => unsupported(data_type),
            DataType::Dictionary(..) => unsupported(data_type),
            // TODO: arrow-rs does not support 256bit decimal
            DataType::Decimal(_, _) => self.hash_fixed_size(array, 16, combined_null_bitmap),
            #[cfg(not(feature = "use-arrow-5"))]
            DataType::Map(..) => unsupported(data_type),
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
        null_bitmap: Option<BitmapSlice>,
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
                    if null_bitmap.is_set(i) {
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
    fn hash_array_bool(&mut self, array: &dyn Array, null_bitmap: Option<BitmapSlice>) {
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
                    if null_bitmap.is_set(i) {
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

    fn hash_array_string<Off: StringOffsetSizeTrait>(
        &mut self,
        array: &GenericStringArray<Off>,
        null_bitmap: Option<BitmapSlice>,
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
                    if null_bitmap.is_set(i) {
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

    fn hash_array_list<Off: OffsetSizeTrait>(
        &mut self,
        array: &GenericListArray<Off>,
        null_bitmap: Option<BitmapSlice>,
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
                    if null_bitmap.is_set(i) {
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
        null_bitmap: Option<BitmapSlice>,
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
                    if null_bitmap.is_set(i) {
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

    use crate::arrow_shim::{
        array::{ArrayData, BooleanArray, Int32Array, StringArray, UInt32Array},
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
                builder.null_bit_buffer(Buffer::from(nulls))
            } else {
                builder
            };

            #[cfg(feature = "use-arrow-6")]
            {
                BooleanArray::from(builder.build().unwrap())
            }

            #[cfg(feature = "use-arrow-5")]
            {
                BooleanArray::from(builder.build())
            }
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
}
