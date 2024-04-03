module S3PATH

using AWS
using Retry

@service S3 use_response_type = true

const DEFAULTBUFFERSIZE = 5 * 1_048_576 # 5 MB approx

struct S3Path
    bucket::AbstractString
    path::AbstractString
    aws_config

    function S3Path(bucket::U, path::V; aws_config=global_aws_config()) where {U <: AbstractString, V <: AbstractString}
        new(bucket, path, aws_config)
    end

    function S3Path(absolute_path::U; aws_config=global_aws_config()) where {U <: AbstractString}
        @assert startswith(absolute_path, "s3://") && '/' in absolute_path[6:end] "Not Valid URI '$absolute_path'"
        bucket, path = split(absolute_path[6:end], '/', limit=2)
        new(bucket, path, aws_config)
    end
end

Base.tryparse(::Type{<:S3Path}, n::Nothing; aws_config=global_aws_config()) = nothing
Base.tryparse(::Type{<:S3Path}, m::Missing; aws_config=global_aws_config()) = nothing
function Base.tryparse(::Type{<:S3Path}, str::AbstractString; aws_config=global_aws_config())
    try
        return S3Path(str; aws_config=aws_config)
    catch
        return nothing
    end
end

Base.show(io::IO, x::S3Path) = print(io, "s3://$(x.bucket)/$(x.path)")
Base.basename(x::S3Path) = basename("s3://$(x.bucket)/$(x.path)")
Base.dirname(x::S3Path) = S3Path(dirname("s3://$(x.bucket)/$(x.path)") * '/'; aws_config=x.aws_config)
function Base.joinpath(parts::Union{AbstractString, S3Path}...)
    @assert typeof(first(parts)) <: S3Path "First argument should be an S3Path"
    return S3Path(
        parts[1].bucket,
        joinpath(parts[1].path, parts[2:end]...);
        aws_config=parts[1].aws_config
    )
end

################################################################################
# Generic

function exists(s3Path::S3Path)
    try
        S3.head_object(
            s3Path.bucket,
            s3Path.path;
            aws_config=s3Path.aws_config
        )
        return true
    catch e
        if isa(e, AWS.AWSExceptions.AWSException) && e.code == "404"
            return false
        else
            throw(e)
        end
    end
end

function Base.isfile(s3Path::S3Path)
    if last(s3Path.path) == '/'
        return false
    end
    return exists(s3Path)
end

function Base.isdir(s3Path::S3Path)
    if last(s3Path.path) != '/'
        return false
    end
    return exists(s3Path)
end

Base.rm(s3Path::S3Path) =
    S3.delete_object(s3Path.bucket, s3Path.path; aws_config=s3Path.aws_config)

function Base.mkpath(s3Path::S3Path)
    S3.put_object(s3Path.bucket, s3Path.path; aws_config=s3Path.aws_config)
end

function Base.readdir(s3Path::S3Path; join=false, sort=true)
    results = String[]
    token = ""
    keylength = length(s3Path.path)

    removekey(key) = ismissing(key) ? key : key[keylength+1:end]

    function add2result!(result, key, subkey)
        if key in keys(result)
            if typeof(result[key]) <: Vector
                append!(
                    results,
                    skipmissing(removekey.(get.(result[key], subkey, nothing)))
                )
            else
                push!(results, removekey(result[key][subkey]))
            end
        end
    end

    while token !== nothing
        params = Dict("delimiter" => "/", "prefix" => s3Path.path)

        if !isempty(token)
            params["continuation-token"] = token
        end

        result = S3.list_objects_v2(s3Path.bucket, params; aws_config=s3Path.aws_config)
        token = get(result, "NextContinuationToken", nothing)

        add2result!(result, "CommonPrefixes", "Prefix")
        add2result!(result, "Contents", "Key")
    end

    filter!(x -> !(isempty(x) || x == "/"), results)

    sort && sort!(results)

    return join ? joinpath.(Ref(s3Path), results) : results
end

################################################################################
# Reading

Base.read(s3Path::S3Path) = read(Vector{UInt8}, s3Path)

function Base.read(type, s3Path::S3Path)
    type(
        S3.get_object(
            s3Path.bucket,
            s3Path.path,
            aws_config=s3Path.aws_config
        )
    )
end

################################################################################
# Writing

function create_multipart_upload(s3Path::S3Path)
    result = @repeat 4 try
        S3.create_multipart_upload(
            s3Path.bucket,
            s3Path.path;
            aws_config=s3Path.aws_config
        )
    catch e
        @delay_retry if true end
    end

    return result["UploadId"]
end

function upload_part(s3Path::S3Path, uploadid, partnumber, part::V) where V <: AbstractVector{UInt8}
    @repeat 4 try
        _, headers = S3.upload_part(
            s3Path.bucket,
            s3Path.path,
            partnumber,
            uploadid,
            Dict(
                "body" => part,
                "Content-Length" => sizeof(part),
                "return_headers" => true
            );
            aws_config=s3Path.aws_config
        )
        return first(filter(p -> p.first == "ETag", headers)).second
    catch e
        @delay_retry if true end
    end

end

function finalise_multipart_upload(s3Path::S3Path, uploadid, upload_ids)
    body = join(
        vcat(
            "<CompleteMultipartUpload>",
            map(x -> "<Part><PartNumber>$(x[1])</PartNumber><ETag>\"$(x[2])\"</ETag></Part>", enumerate(upload_ids)),
            "</CompleteMultipartUpload>"
        )
    )

    @repeat 4 try
        S3.complete_multipart_upload(
            s3Path.bucket,
            s3Path.path,
            uploadid,
            Dict("body"=>body)
        )
    catch e
        @delay_retry if true end
    end
end

Base.write(s3Path::S3Path, content::AbstractString; blocksize=DEFAULTBUFFERSIZE) =
    Base.write(s3Path::S3Path, Vector{UInt8}(content); blocksize=blocksize)

function Base.write(s3Path::S3Path, content::Vector{UInt8}; blocksize=DEFAULTBUFFERSIZE)

    if sizeof(content) < blocksize
        @repeat 4 try
            S3.put_object(
                s3Path.bucket,
                s3Path.path,
                Dict(
                    "body" => content,
                    "Content-Length" => sizeof(content),
                );
                aws_config=s3Path.aws_config
            )
        catch e
            @delay_retry if true end
        end

    else
        uploadid = create_multipart_upload(s3Path)
        total_parts = Int(ceil(sizeof(content) / blocksize))
        upload_ids = Vector(undef, total_parts) # TODO rename

        Threads.@threads for partnumber in 1:total_parts
            upload_ids[partnumber] = upload_part(
                s3Path,
                uploadid,
                view(content, (partnumber-1)*blocksize+1:min(partnumber*blocksize, sizeof(content))),
                partnumber
            )
        end

        finalise_multipart_upload(s3Path, uploadid, upload_ids)
    end
end

################################################################################
# Copy helper functions to copy between S3 <-> S3 or Filesystem <-> S3

function Base.cp(src::S3Path, dst::S3Path)
    S3.copy_object(
        dst.bucket, dst.path,
        joinpath(src.bucket, src.path);
        aws_config = src.aws_config
    )
end

function Base.cp(src::S3Path, dst::AbstractString)
    open(dst, "w") do f
        write(f, S3.get_object(src.bucket, src.path; aws_config=src.aws_config))
    end
end

function Base.cp(src::AbstractString, dst::S3Path)
    write(dst, read(src)) # TODO make it work on streams as well
end

################################################################################
# Buffered Reading/Writing

mutable struct S3WriteBuffer <: Base.IO
    buffer::IOBuffer
    s3Path::S3Path
    uploadid::Union{AbstractString, Missing}
    partnumber::UInt64
    writtenparts::Vector{AbstractString}
    isopen::Bool
    position
    function S3WriteBuffer(
        s3Path::S3Path;
        buffersize = DEFAULTBUFFERSIZE
    )
        new(
            IOBuffer(;
                maxsize=buffersize
            ),
            s3Path,
            missing,
            UInt64(0),
            Vector{AbstractString}(),
            true,
            0
        )
    end
end

Base.isreadable(io::S3WriteBuffer) = false
Base.iswritable(io::S3WriteBuffer) = io.isopen
Base.isopen(io::S3WriteBuffer) = io.isopen
Base.position(io::S3WriteBuffer) = io.position

mutable struct S3ReadBuffer <: Base.IO
    buffer::IOBuffer
    s3Path::S3Path
    isopen::Bool
    function S3ReadBuffer(
        s3Path::S3Path;
        buffersize = DEFAULTBUFFERSIZE
    )
        new(
            IOBuffer(;
                maxsize=buffersize
            ),
            s3Path,
            true
        )
    end
end

Base.isreadable(io::S3ReadBuffer) = io.isopen
Base.iswritable(io::S3ReadBuffer) = false
Base.isopen(io::S3ReadBuffer) = io.isopen

function Base.open(f::Function, s3Path::S3Path, mode::AbstractString; buffersize=DEFAULTBUFFERSIZE)
    @assert mode ∈ Set(["w"]) "Only write is currently supported"
    if mode == "w"
        io = S3WriteBuffer(s3Path; buffersize=buffersize)
        f(io)
        close(io)
    else
        io = S3ReadBuffer(s3Path; buffersize=buffersize)
        f(io)
        close(io)
    end
end

# TODO might want a flush function

Base.write(io::S3WriteBuffer, content::Union{SubString{String}, String}) =
    Base.write(io::S3WriteBuffer, Vector{UInt8}(content))

# TODO don't want to wrap this in a vector everytime
function Base.write(io::S3WriteBuffer, byte::UInt8)
    write(io, [byte])
end

function Base.write(io::S3WriteBuffer, content::Vector{UInt8})
    io.position += length(content)

    if length(content) + io.buffer.ptr - 1 > io.buffer.maxsize

        if ismissing(io.uploadid)
            io.uploadid = create_multipart_upload(io.s3Path)
        end

        # First finish writing buffer contents
        ptr = io.buffer.maxsize - io.buffer.ptr + 1
        write(io.buffer, content[1:ptr])
        io.partnumber += 1
        push!(
            io.writtenparts,
            upload_part(
                io.s3Path,
                io.uploadid,
                io.partnumber,
                io.buffer.data
            )
        )
        ptr += 1

        # Write any data that won't fit into the buffer
        while length(content) - ptr + 1 > io.buffer.maxsize
            io.partnumber += 1
            push!(
                io.writtenparts,
                upload_part(
                    io.s3Path,
                    io.uploadid,
                    io.partnumber,
                    content[ptr:ptr+io.buffer.maxsize-1]
                )
            )
            ptr += io.buffer.maxsize
        end

        # Add remaining data to buffer
        seekstart(io.buffer)
        write(io.buffer, content[ptr:end])

    else

        write(io.buffer, content)

    end
end

function Base.close(io::S3WriteBuffer)
    io.isopen = false

    if ismissing(io.uploadid)

        write(io.s3Path, io.buffer.data[1:io.buffer.ptr-1])

    else

        if io.buffer.ptr > 1

            # Write contents of buffer if there is any
            io.partnumber += 1
            push!(
                io.writtenparts,
                upload_part(
                    io.s3Path,
                    io.uploadid,
                    io.partnumber,
                    io.buffer.data[1:io.buffer.ptr-1]
                )
            )

        end

        finalise_multipart_upload(
            io.s3Path,
            io.uploadid,
            io.writtenparts
        )

    end

end

# Base.read(p::S3Buffer) = read(Vector{UInt8}, p)
#
# function Base.read(type, p::S3Buffer)
#     throw(ErrorException("Not Implemented Yet"))
#
#     # GET /example-object HTTP/1.1
#     # Host: example-bucket.s3.<Region>.amazonaws.com
#     # x-amz-date: Fri, 28 Jan 2011 21:32:02 GMT
#     # Range: bytes=0-9
#     # Authorization: AWS AKIAIOSFODNN7EXAMPLE:Yxg83MZaEgh3OZ3l0rLo5RTX11o=
#     # Sample Response with Specified Range of the Object Bytes
#     #
#     # aws s3api get-object --bucket DOC-EXAMPLE-BUCKET1 --key folder/my_data --range bytes=0-500 my_data_range
#     #
#     # just use the get object method with ranges
#     #
#     # https://juliacloud.github.io/AWS.jl/stable/services/s3.html#Main.S3.get_object-Tuple{Any,%20Any}
#     #
#     # and I think can find out size of object in advance via
#     #
#     # https://juliacloud.github.io/AWS.jl/stable/services/s3.html#Main.S3.head_object-Tuple{Any,%20Any}
#
# end

export S3Path

end
