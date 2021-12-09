use arrow::datatypes::{DataType, TimeUnit};
use digest::Digest;

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
#[repr(u16)]
pub(crate) enum TypeID {
    Null = 0,
    Int = 1,
    FloatingPoint = 2,
    Binary = 3,
    Utf8 = 4,
    Bool = 5,
    Decimal = 6,
    Date = 7,
    Time = 8,
    Timestamp = 9,
    Interval = 10,
    List = 11,
    Struct = 12,
    Union = 13,
    FixedSizeBinary = 14,
    FixedSizeList = 15,
    Map = 16,
    Duration = 17,
    //LargeBinary = 3,
    //LargeUtf8 = 4,
    //LargeList = 11,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[repr(u16)]
pub(crate) enum DateUnitID {
    DAY = 0,
    MILLISECOND = 1,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[repr(u16)]
pub(crate) enum TimeUnitID {
    Second = 0,
    Millisecond = 1,
    Microsecond = 2,
    Nanosecond = 3,
}

impl From<&TimeUnit> for TimeUnitID {
    fn from(u: &TimeUnit) -> Self {
        match u {
            TimeUnit::Second => TimeUnitID::Second,
            TimeUnit::Millisecond => TimeUnitID::Millisecond,
            TimeUnit::Microsecond => TimeUnitID::Microsecond,
            TimeUnit::Nanosecond => TimeUnitID::Nanosecond,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Support nesting
pub(crate) fn hash_data_type<Dig: Digest>(data_type: &DataType, hasher: &mut Dig) {
    match data_type {
        DataType::Null => {
            hasher.update(&(TypeID::Null as u16).to_le_bytes());
        }
        DataType::Boolean => {
            hasher.update(&(TypeID::Bool as u16).to_le_bytes());
        }
        DataType::Int8 => {
            hasher.update(&(TypeID::Int as u16).to_le_bytes());
            hasher.update(&1u8.to_le_bytes());
            hasher.update(&8u64.to_le_bytes());
        }
        DataType::Int16 => {
            hasher.update(&(TypeID::Int as u16).to_le_bytes());
            hasher.update(&1u8.to_le_bytes());
            hasher.update(&16u64.to_le_bytes());
        }
        DataType::Int32 => {
            hasher.update(&(TypeID::Int as u16).to_le_bytes());
            hasher.update(&1u8.to_le_bytes());
            hasher.update(&32u64.to_le_bytes());
        }
        DataType::Int64 => {
            hasher.update(&(TypeID::Int as u16).to_le_bytes());
            hasher.update(&1u8.to_le_bytes());
            hasher.update(&64u64.to_le_bytes());
        }
        DataType::UInt8 => {
            hasher.update(&(TypeID::Int as u16).to_le_bytes());
            hasher.update(&0u8.to_le_bytes());
            hasher.update(&8u64.to_le_bytes());
        }
        DataType::UInt16 => {
            hasher.update(&(TypeID::Int as u16).to_le_bytes());
            hasher.update(&0u8.to_le_bytes());
            hasher.update(&16u64.to_le_bytes());
        }
        DataType::UInt32 => {
            hasher.update(&(TypeID::Int as u16).to_le_bytes());
            hasher.update(&0u8.to_le_bytes());
            hasher.update(&32u64.to_le_bytes());
        }
        DataType::UInt64 => {
            hasher.update(&(TypeID::Int as u16).to_le_bytes());
            hasher.update(&0u8.to_le_bytes());
            hasher.update(&64u64.to_le_bytes());
        }
        DataType::Float16 => {
            hasher.update(&(TypeID::FloatingPoint as u16).to_le_bytes());
            hasher.update(&16u64.to_le_bytes());
        }
        DataType::Float32 => {
            hasher.update(&(TypeID::FloatingPoint as u16).to_le_bytes());
            hasher.update(&32u64.to_le_bytes());
        }
        DataType::Float64 => {
            hasher.update(&(TypeID::FloatingPoint as u16).to_le_bytes());
            hasher.update(&64u64.to_le_bytes());
        }
        DataType::Timestamp(time_unit, time_zone) => {
            hasher.update(&(TypeID::Timestamp as u16).to_le_bytes());
            hasher.update(&(TimeUnitID::from(time_unit) as u16).to_le_bytes());
            match time_zone {
                None => hasher.update(&[0u8]),
                Some(tz) => {
                    hasher.update(&(tz.len() as u64).to_le_bytes());
                    hasher.update(tz.as_bytes());
                }
            }
        }
        DataType::Date32 => {
            hasher.update(&(TypeID::Date as u16).to_le_bytes());
            hasher.update(&32u64.to_le_bytes());
            hasher.update(&(DateUnitID::DAY as u16).to_le_bytes());
        }
        DataType::Date64 => {
            hasher.update(&(TypeID::Date as u16).to_le_bytes());
            hasher.update(&64u64.to_le_bytes());
            hasher.update(&(DateUnitID::MILLISECOND as u16).to_le_bytes());
        }
        DataType::Time32(time_unit) => {
            hasher.update(&(TypeID::Time as u16).to_le_bytes());
            hasher.update(&32u64.to_le_bytes());
            hasher.update(&(TimeUnitID::from(time_unit) as u16).to_le_bytes());
        }
        DataType::Time64(time_unit) => {
            hasher.update(&(TypeID::Time as u16).to_le_bytes());
            hasher.update(&64u64.to_le_bytes());
            hasher.update(&(TimeUnitID::from(time_unit) as u16).to_le_bytes());
        }
        DataType::Duration(_) => unimplemented!(),
        DataType::Interval(_) => unimplemented!(),
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => {
            unimplemented!()
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            hasher.update(&(TypeID::Utf8 as u16).to_le_bytes());
        }
        DataType::List(_) | DataType::FixedSizeList(..) | DataType::LargeList(_) => {
            unimplemented!()
        }
        DataType::Struct(_) => unimplemented!(),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(..) => unimplemented!(),
        DataType::Decimal(p, s) => {
            // TODO: arrow-rs does not support 256bit decimal
            hasher.update(&(TypeID::Utf8 as u16).to_le_bytes());
            hasher.update(&128u64.to_le_bytes());
            hasher.update(&(*p as u64).to_le_bytes());
            hasher.update(&(*s as u64).to_le_bytes());
        }
        DataType::Map(..) => unimplemented!(),
    }
}

pub(crate) fn get_fixed_size(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Null => unimplemented!(),
        DataType::Boolean => None,
        DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 => Some(2),
        DataType::Int32 | DataType::UInt32 => Some(4),
        DataType::Int64 | DataType::UInt64 => Some(8),
        DataType::Float16 => Some(2),
        DataType::Float32 => Some(4),
        DataType::Float64 => Some(8),
        DataType::Timestamp(_, _) => Some(8),
        DataType::Date32 => Some(4),
        DataType::Date64 => Some(8),
        DataType::Time32(_) => Some(4),
        DataType::Time64(_) => Some(8),
        DataType::Duration(_) => unimplemented!(),
        DataType::Interval(_) => unimplemented!(),
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => None,
        DataType::Utf8 | DataType::LargeUtf8 => None,
        DataType::List(_) | DataType::FixedSizeList(..) | DataType::LargeList(_) => None,
        DataType::Struct(_) => unimplemented!(),
        DataType::Union(_) => unimplemented!(),
        DataType::Dictionary(..) => unimplemented!(),
        DataType::Decimal(_, _) => Some(16), // TODO: arrow-rs does not support 256bit decimal
        DataType::Map(..) => unimplemented!(),
    }
}
