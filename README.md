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
using S3PATH

fp = S3Path("s3://bucket/tmpfile")

write(fp, "Something")

isfile(fp) # true
isdir(fp) # false

readdir(dirname(fp)) # tempfile
                     # and any other files/folders

"Something" == read(String, fp) # true

cp(fp, S3Path(bucket, "tmpfile2"))

"Something" == read(String, S3Path(bucket, "tmpfile2")) # true

rm(fp)

# Copy files to and from a bucket

fp = S3Path("s3://bucket/a_file")
cp("a_file", fp)
cp(fp, "local_copy")
run(`diff a_file local_copy`)

# Writing larger amounts of data
write1 = rand(UInt8, S3PATH.DEFAULTBUFFERSIZE * 2) # 10 MB
write2 = rand(UInt8, S3PATH.DEFAULTBUFFERSIZE * 3) # 15 MB

open(fp, "w") do io
    write(io, write1)
    write(io, write2)
end

read(fp) == vcat(write1, write2)

rm(fp)

# Writing and reading a Parquet File to/from S3
using DataFrames
using Parquet2

fp = S3Path("s3://bucket/data.parq")

df = DataFrame(a = 1:20, b = 0, c = "awesome!")

open(fp, "w") do io
    Parquet2.writefile(io, df)
end

df2 = open(fp, "r") do io
    Parquet2.Dataset(io) |> DataFrame
end

df == df2 # true

rm(fp)

# Writing a Compressed CSV to S3
using DataFrames
using CSV
using CodecZstd

fp = S3Path("s3://bucket/data.csv.zstd")

df = DataFrame(a = 1:20, b = 0, c = "awesome!")

wio = open(fp, "w")
cwio = ZstdCompressorStream(wio)
CSV.write(cwio, df)
close(cwio)
close(wio)

# Reading an XML file
using EzXML

fp = S3Path("s3://bucket/values.xml")

values = open(fp, "r") do io
    nodecontent.(
        findall(
            "//option/string/text()",
            root(readxml(io))
        )
    )
end
```

## TODO

- missing copy/delete functions for whole folders
- ~~copy local file to s3 reads the entire file into memory unnecessarily but it could be streamed~~
- url encoding is missing for some parts of api
- ~~reading large files / streaming files~~
