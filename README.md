# arrow-digest

[![Crates.io](https://img.shields.io/crates/v/arrow-digest.svg?style=for-the-badge)](https://crates.io/crates/arrow-digest)
[![CI](https://img.shields.io/github/actions/workflow/status/sergiimk/arrow-digest/build.yaml?logo=githubactions&label=CI&logoColor=white&style=for-the-badge&branch=master)](https://github.com/sergiimk/arrow-digest/actions)
[![Dependencies](https://deps.rs/repo/github/sergiimk/arrow-digest/status.svg?&style=for-the-badge)](https://deps.rs/repo/github/sergiimk/arrow-digest)


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

## Status
While we're working towards `v1` we reserve the right to break the hash stability. Create an issue if you're planning to use this crate.

- [x] Schema hashing
- [x] Fixed size types
- [x] Nullability: primitive types  
- [X] Binaries
- [x] Uft8 variants
- [x] Structs
- [x] Nested structs
- [x] Nullability: nested structs  
- [x] Lists
- [ ] Lists of structs
- [ ] Dictionaries
- [ ] Intervals
- [ ] Unions
- [ ] Maps
- [ ] Metadata endianness check
- [ ] Better test coverage + fuzzing
- [ ] Performance: Benchmarks
- [ ] Performance: Parallelism
- [ ] Performance: Code optimization

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
- **Fixed Size Types**
  - `Int, FloatingPoint, Decimal, Date, Time, Timestamp` - hashed using their in-memory binary representation
  - `Bool` - hash the individual values as byte-sized values `1` for `false` and `2` for `true`
- **Variable Size Types**
  - `Binary, LargeBinary, FixedSizeBinary, Utf8, LargeUtf8` - hash length (as `u64`) followed by in-memory representation of the value
  - `List, LargeList, FixedSizeList` - hash length of the list (as `u64`) followed by the hash of the sub-array list according to its data type
- **Nullability** - every null value is represented by a `0` (zero) byte
  - Arrays without validity bitmap have same hashes as arrays that do and all items are valid
- **Array Data**
  - *(once per hashing session)* Hash data type according to the table below
  - Hash items sequentially using the above rules
- **Record Batch Data**
  - *(once per hashing session)* For every field hash `filed_name as utf8`, `nesting_level (zero-based) as u64` recursively traversing the schema in the **depth-first** order
  - For every leaf column:
    - Produce a **combined nullability bitmap** from nullability of every parent
    - Update corresponding column's hasher using above rules
  - *(final step)* Digests of every array are fed into the combined hasher to produce the final digest

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
| List                  |        11         | `items data type`                                     |
| Struct                |        12         |                                                       |
| Union                 |        13         |                                                       |
| FixedSizeBinary       |         3         |                                                       |
| FixedSizeList         |        11         | `items data type`                                     |
| Map                   |        16         |                                                       |
| Duration              |        17         |                                                       |
| LargeBinary           |         3         |                                                       |
| LargeUtf8             |         4         |                                                       |
| LargeList             |        11         | `items data type`                                     |

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

## References
- [Arrow memory layout](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout)
- [Arrow Flatbuffers schema](https://github.com/apache/arrow/blob/master/format/Schema.fbs)
