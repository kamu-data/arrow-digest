use arrow::{array::ArrayData, bitmap::Bitmap};
use std::ops::BitAnd;

/// Similar to `ArrayData` but only holds a bitmap
#[derive(Debug, Clone)]
pub struct BitmapSlice {
    offset: usize,
    len: usize,
    bitmap: Bitmap,
}

impl BitmapSlice {
    /// Offset in bits from the start of a bitmap buffer (which may also have its own offset)
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Length of the slice in bits
    pub fn len(&self) -> usize {
        self.len
    }

    /// Checks if i'th bit is set
    pub fn is_set(&self, i: usize) -> bool {
        self.bitmap.is_set(self.offset + i)
    }

    pub fn from_null_bitmap(data: &ArrayData) -> Option<Self> {
        match data.null_buffer() {
            None => None,
            Some(buf) => Some(Self {
                offset: data.offset(),
                len: data.len(),
                bitmap: Bitmap::from(buf.clone()), // Cloning buffer should be cheap as it's a shared reference to data
            }),
        }
    }
}

// TODO: Use results instead of panicing
impl<'a, 'b> BitAnd<&'b BitmapSlice> for &'a BitmapSlice {
    type Output = BitmapSlice;

    fn bitand(self, rhs: &'b BitmapSlice) -> Self::Output {
        assert_eq!(self.len, rhs.len, "BitmapSlices have different lengths");
        assert_eq!(
            self.offset, rhs.offset,
            "Operations on BitmapSlices with different offsets are not supported"
        );
        BitmapSlice {
            offset: self.offset,
            len: self.len,
            bitmap: (&self.bitmap & &rhs.bitmap).unwrap(),
        }
    }
}
