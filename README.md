# S3Path.jl

A work in progress replacement for
[s3path.jl](https://github.com/JuliaCloud/AWSS3.jl/blob/master/src/s3path.jl)
that was available in the old
[AWSS3.jl](https://github.com/JuliaCloud/AWSS3.jl/tree/master)
package that has now been replaced by
[AWS.jl](https://github.com/JuliaCloud/AWS.jl).

This provides many of the common file manipulation utility functions
for S3 paths.

## Example

```julia
fp = S3Path("s3://bucket/tmpfile")

write(fp, "Something")

readdir(dirname(fp)) # tempfile
                     # and any other files/folders

"Something" == read(String, fp) # true

cp(fp, S3Path(bucket, "tmpfile2"))

"Something" == read(String, S3Path(bucket, "tmpfile2")) # true

rm(fp)
```

## TODO

- [ ] missing copy/delete functions for whole folders
- [ ] copy local file to s3 reads the entire file into memory unnecessarily but it could be streamed
