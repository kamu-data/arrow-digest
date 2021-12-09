use arrow::{
    array::{
        Array, BooleanArray, GenericStringArray, LargeStringArray, StringArray,
        StringOffsetSizeTrait,
    },
    datatypes::DataType,
};
use digest::{Digest, Output, OutputSizeUser};

use crate::ArrayDigest;

/////////////////////////////////////////////////////////////////////////////////////////
pub struct ArrayDigestV0<Dig: Digest> {
    fixed_size: Option<usize>,
    hasher: Dig,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<Dig: Digest> OutputSizeUser for ArrayDigestV0<Dig> {
    type OutputSize = Dig::OutputSize;
}

impl<Dig: Digest> ArrayDigest for ArrayDigestV0<Dig> {
    fn digest(array: &dyn Array) -> Output<Dig> {
        let mut d = Self::new(array.data_type());
        d.update(array);
        d.finalize()
    }

    fn new(data_type: &DataType) -> Self {
        let mut hasher = Dig::new();
        crate::schema_digest::hash_data_type(data_type, &mut hasher);

        let fixed_size = crate::schema_digest::get_fixed_size(data_type);

        Self { fixed_size, hasher }
    }

    fn update(&mut self, array: &dyn Array) {
        let data_type = array.data_type();

        if self.fixed_size.is_some() {
            self.hash_fixed_size(array)
        } else if *data_type == DataType::Boolean {
            self.hash_array_bool(array)
        } else if *data_type == DataType::Utf8 {
            self.hash_array_string(array.as_any().downcast_ref::<StringArray>().unwrap());
        } else if *data_type == DataType::LargeUtf8 {
            self.hash_array_string(array.as_any().downcast_ref::<LargeStringArray>().unwrap());
        } else {
            unimplemented!("Type {} is not yet supported", data_type);
        }
    }

    fn finalize(self) -> Output<Dig> {
        self.hasher.finalize()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<Dig: Digest> ArrayDigestV0<Dig> {
    const NULL_MARKER: [u8; 1] = [0];

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
                    self.hasher.update(&Self::NULL_MARKER);
                }
            }
        }
    }

    // TODO: Speed up
    fn hash_array_bool(&mut self, array: &dyn Array) {
        let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        if bool_array.null_count() == 0 {
            for i in 0..bool_array.len() {
                // Safety: boundary check is right above
                let value = unsafe { bool_array.value_unchecked(i) };
                self.hasher.update(&[value as u8 + 1]);
            }
        } else {
            for i in 0..bool_array.len() {
                if bool_array.is_valid(i) {
                    // Safety: boundary check is right above
                    let value = unsafe { bool_array.value_unchecked(i) };
                    self.hasher.update(&[value as u8 + 1]);
                } else {
                    self.hasher.update(&Self::NULL_MARKER);
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
                    self.hasher.update(&Self::NULL_MARKER);
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
        array::{ArrayData, BooleanArray, Int32Array, StringArray, UInt32Array},
        buffer::Buffer,
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
        let array1 = BooleanArray::from(
            ArrayData::builder(DataType::Boolean)
                .len(6)
                .add_buffer(Buffer::from(vec![0b0011_0101u8]))
                .build()
                .unwrap(),
        );

        let array2 = BooleanArray::from(
            ArrayData::builder(DataType::Boolean)
                .len(6)
                .add_buffer(Buffer::from(vec![0b1111_0101u8]))
                .build()
                .unwrap(),
        );

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

        let array3 = BooleanArray::from(
            ArrayData::builder(DataType::Boolean)
                .len(6)
                .add_buffer(Buffer::from(vec![0b1111_0101u8]))
                .null_bit_buffer(Buffer::from(vec![0b1110_1110]))
                .build()
                .unwrap(),
        );

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
}
