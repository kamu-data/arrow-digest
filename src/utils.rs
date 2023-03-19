use arrow::buffer::{buffer_bin_or, BooleanBuffer, NullBuffer};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn combine_null_buffers(a: &NullBuffer, b: &NullBuffer) -> NullBuffer {
    assert_eq!(
        a.len(),
        b.len(),
        "Attempting to combine buffers of different size {} != {}",
        a.len(),
        b.len()
    );

    // In NullBuffer `1` stands for valid and `0` for null
    let buffer = buffer_bin_or(a.buffer(), a.offset(), b.buffer(), b.offset(), a.len());

    NullBuffer::new(BooleanBuffer::new(buffer, 0, a.len()))
}

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn maybe_combine_null_buffers<'a>(
    a: Option<&'a NullBuffer>,
    b: Option<&'a NullBuffer>,
) -> CombinedNullBuffer<'a> {
    match (a, b) {
        (None, Some(b)) if b.null_count() != 0 => CombinedNullBuffer::Borrowed(b),
        (Some(a), None) if a.null_count() != 0 => CombinedNullBuffer::Borrowed(a),
        (Some(a), Some(b)) if a.null_count() != 0 && b.null_count() != 0 => {
            CombinedNullBuffer::Owned(combine_null_buffers(a, b))
        }
        _ => CombinedNullBuffer::None,
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// An amalgamation of Option<T> and a Cow<'a, &T>
pub(crate) enum CombinedNullBuffer<'a> {
    None,
    Borrowed(&'a NullBuffer),
    Owned(NullBuffer),
}

impl<'a> CombinedNullBuffer<'a> {
    pub fn as_option(&'a self) -> Option<&'a NullBuffer> {
        match self {
            CombinedNullBuffer::None => None,
            CombinedNullBuffer::Borrowed(buf) => Some(*buf),
            CombinedNullBuffer::Owned(buf) => Some(buf),
        }
    }
}
