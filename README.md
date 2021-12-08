# arrow-digest
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

// Or use `.update(&array)` to hash multiple arrays of the same type
let digest = ArrayDigestV0::<Sha3_256>::digest(&array);
println!("{:x}", digest);


// Hash record batches
let schema = Arc::new(Schema::new(vec![
    Field::new("a", DataType::Int32, false),
    Field::new("b", DataType::Utf8, false),
]));

let record_batch = RecordBatch::try_new(Arc::new(schema), vec![
    Arc::new(Int32Array::from(vec![1, 2, 3])), 
    Arc::new(StringArray::from(vec!["a", "b", "c"])),
]).unwrap();

// Or use `.update(&batch)` to hash multiple batches with same schema
let digest = RecordsDigestV0::<Sha3_256>::digest(&record_batch);
println!("{:x}", digest);
```

## Design Goals
- Be reasonably fast
- Same hash no matter how many batches the input was split into
- Same hash no matter if dictionary encoding is used

## Hashing Process
Starting from primitives and building up:

- **Endinanness** - always assume little endian
- `{U}Int{8,16,32,64}, Float{16,32,64}` - hashed using their in-memory binary representation
- `Utf8, LargeUtf8` - hash length (as `u64`) followed by in-memory representation of the string
  - Empty strings affect the hash - `digest(["foo", "bar"]) != hash(["f", "oobar"])`
- **Nullability** - every null value is represented by a `0` (zero) byte
  - Arrays without validity bitmap have same hashes as arrays that do and all items are valid
- **Array** - data in array hashed sequentially using the above rules
  - Arrays without gaps and null items can be hashed as a single memory slice
- **Record Batch** - every column is hashed independently, hashes of individual columns are then combined together into the final digest

## TODO
- Metadata endianness check
- Schema should be part of the hash to exclude manipulation of representation (e.g. changing `decimal` precision)
- Support nested data
- Support lists

## References
- [Arrow memory layout](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout)
