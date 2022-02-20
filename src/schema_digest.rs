use crate::arrow_shim::datatypes::{DataType, TimeUnit};
use digest::Digest;

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
#[repr(u16)]
pub(crate) enum TypeID {
    Null = 0,
    Int = 1,
    FloatingPoint = 2,
    Binary = 3,
    // LargeBinary = 3,
    // FixedSizeBinary = 3,
    Utf8 = 4,
    // LargeUtf8 = 4,
    Bool = 5,
    Decimal = 6,
    Date = 7,
    Time = 8,
    Timestamp = 9,
    Interval = 10,
    List = 11,
    // LargeList = 11,
    // FixedSizeList = 11,
    Struct = 12,
    Union = 13,
    Map = 16,
    Duration = 17,
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
            hasher.update(&(TypeID::Binary as u16).to_le_bytes());
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            hasher.update(&(TypeID::Utf8 as u16).to_le_bytes());
        }
        DataType::List(field) | DataType::FixedSizeList(field, _) | DataType::LargeList(field) => {
            hasher.update(&(TypeID::List as u16).to_le_bytes());
            hash_data_type(field.data_type(), hasher);
        }
        DataType::Struct(_) => unimplemented!(),
        DataType::Union(_, _) => unimplemented!(),
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
