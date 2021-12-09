# arrow-digest

[![Crates.io](https://img.shields.io/crates/v/arrow-digest.svg)](https://crates.io/crates/arrow-digest)

Unofficial Apache Arrow crate that aims to standardize stable hashing of structured data.


## Motivation
Today, structured data formats like Parquet are binary-unstable / non-reproducible - writing the same logical data may result in different files on binary level depending on which writer implementation and you use and may vary with each version.

This crate provides a method and implementation for computing **stable hashes of structured data** (logical hash) based on Apache Arrow in-memory format.

Benefits:
- Fast way to check for equality / equivalence of large datasets
- Two parties can compare data without needing to transfer it or reveal its contents
- A step towards **content addressability** of structured data (e.g. when storing dataset chunks in DHTs like IPFS)

## Use

```rust
// Hash single array
let array = Int32Array::from(vec![1, 2, 3]);
let digest = ArrayDigestV0::<Sha3_256>::digest(&array);
println!("{:x}", digest);

// Alternatively: Use `.update(&array)` to hash multiple arrays of the same type

// Hash record batches
let schema = Arc::new(Schema::new(vec![
    Field::new("a", DataType::Int32, false),
    Field::new("b", DataType::Utf8, false),
]));

let record_batch = RecordBatch::try_new(Arc::new(schema), vec![
    Arc::new(Int32Array::from(vec![1, 2, 3])), 
    Arc::new(StringArray::from(vec!["a", "b", "c"])),
]).unwrap();

let digest = RecordsDigestV0::<Sha3_256>::digest(&record_batch);
println!("{:x}", digest);

// Alternatively: Use `.update(&batch)` to hash multiple batches with same schema
```

## Design Goals
- Be reasonably fast
- Same hash no matter how many batches the input was split into
- Same hash no matter if dictionary encoding is used

## Drawbacks
- Logical hasing stops short of perfect content addressibility
  - Logical hashing would need to be supported by `IPFS` and the likes, but this is a stretch as this is not a general-purpose hashing algo
  - A fully deterministic binary encoding with Parquet compatibility may be a better approach
- Proposed method is order-dependent - it will produce different hashes if records are reordered
- Boolean hashing could be more efficient

## Hashing Process
Starting from primitives and building up:

- **Endinanness** - always assume little endian
- `{U}Int{8,16,32,64}, Float{16,32,64}` - hashed using their in-memory binary representation
- `Utf8, LargeUtf8` - hash length (as `u64`) followed by in-memory representation of the string
  - Empty strings affect the hash - `digest(["foo", "bar"]) != hash(["f", "oobar"])`
- **Nullability** - every null value is represented by a `0` (zero) byte
  - Arrays without validity bitmap have same hashes as arrays that do and all items are valid
- **Boolean Bitmaps** - hash the individual values as byte-sized values `1` for `false` and `2` for `true`
  - Using `1` and `2` differentiates them from nulls
- **Array Data** - data in array hashed sequentially using the above rules
  - Arrays without gaps and null items can be hashed as a single memory slice
- **Record Batch Data** - every column is hashed independently, hashes of individual columns are then combined together into the final digest
- **Schema** - at the start of the process every column hashes its data type according to the following rules

| Type (in `Schema.fb`) | TypeID (as `u16`) | Followed by                                           |
| --------------------- | :---------------: | ----------------------------------------------------- |
| Null                  |         0         |                                                       |
| Int                   |         1         | `unsigned/signed (0/1) as u8`, `bitwidth as u64`      |
| FloatingPoint         |         2         | `bitwidth as u64`                                     |
| Binary                |         3         |                                                       |
| Utf8                  |         4         |                                                       |
| Bool                  |         5         |                                                       |
| Decimal               |         6         | `bitwidth as u64`, `precision as u64`, `scale as u64` |
| Date                  |         7         | `bitwidth as u64`, `DateUnitID`                       |
| Time                  |         8         | `bitwidth as u64`, `TimeUnitID`                       |
| Timestamp             |         9         | `TimeUnitID`, `timeZone as nullable Utf8`             |
| Interval              |        10         |                                                       |
| List                  |        11         |                                                       |
| Struct                |        12         |                                                       |
| Union                 |        13         |                                                       |
| FixedSizeBinary       |        14         |                                                       |
| FixedSizeList         |        15         |                                                       |
| Map                   |        16         |                                                       |
| Duration              |        17         |                                                       |
| LargeBinary           |         3         |                                                       |
| LargeUtf8             |         4         |                                                       |
| LargeList             |        11         |                                                       |

Note that some types (`Utf8` and `LargeUtf8`, `Binary` `FixedSizeBinary` and `LargeBinary`, `List` `FixedSizeList` and `LargeList`) are represented in the hash the same, as the difference between them is purely an encoding concern.

| DateUnit (in `Schema.fb`) | DateUnitID (as `u16`) |
| ------------------------- | :-------------------: |
| DAY                       |           0           |
| MILLISECOND               |           1           |

| TimeUnit (in `Schema.fb`) | TimeUnitID (as `u16`) |
| ------------------------- | :-------------------: |
| SECOND                    |           0           |
| MILLISECOND               |           1           |
| MICROSECOND               |           2           |
| NANOSECOND                |           3           |

## TODO
- Metadata endianness check
- Schema should be part of the hash to exclude manipulation of representation (e.g. changing `decimal` precision)
- Support nested data
- Support lists
- Fuzzing

## References
- [Arrow memory layout](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout)
- [Arrow Flatbuffers schema](https://github.com/apache/arrow/blob/master/format/Schema.fbs)
