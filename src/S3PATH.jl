module S3PATH

using AWS
using Retry
import Base: ==

@service S3

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

==(x::S3Path, y::S3Path) = x.bucket == y.bucket &&
                           x.path == y.path &&
                           x.aws_config == y.aws_config

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

function Base.splitdir(x::S3Path)
    if endswith(x.path, '/')
        (p1, p2) = splitdir(x.path[1:end-1])
        p2 *= '/'
    else
        (p1, p2) = splitdir(x.path)
    end

    if !isempty(p1)
        p1 *= "/"
    end

    return (
        S3Path(x.bucket, p1; aws_config=x.aws_config),
        p2
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
    return s3Path
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
        params = Dict(
            "delimiter" => "/",
            "prefix" => s3Path.path,
        )

        if !isempty(token)
            params["continuation-token"] = token
        end

        # Force parsing as XMl
        # replacing the high-level method
        # result = S3.list_objects_v2(s3Path.bucket, params; aws_config=s3Path.aws_config)
        result = S3.s3(
            "GET",
            "/$(s3Path.bucket)?list-type=2",
            params;
            aws_config=s3Path.aws_config,
            feature_set=AWS.FeatureSet(use_response_type=true),
        )

        expected_result_type = MIME"application/xml"
        resultdict = parse(result, expected_result_type())

        token = get(resultdict, "NextContinuationToken", nothing)

        add2result!(resultdict, "CommonPrefixes", "Prefix")
        add2result!(resultdict, "Contents", "Key")
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
            # Force Binary Format
            Dict("response-content-type"=>"application/octet-stream");
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
        upload_ids = Vector(undef, total_parts)

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
    return nothing
end

function Base.cp(src::S3Path, dst::AbstractString)
    open(dst, "w") do f
        write(
            f,
            S3.get_object(
                src.bucket,
                src.path,
                # Force Binary Format
                Dict("response-content-type"=>"application/octet-stream");
                aws_config=src.aws_config
            )
        )
    end
    return nothing
end

function Base.cp(src::AbstractString, dst::S3Path; buffersize=DEFAULTBUFFERSIZE)
    # Uploads the local file in blocks of at most buffersize.
    open(src, "r") do io_in
        open(dst, "w"; buffersize=buffersize) do io_out
            while !eof(io_in)
                write(io_out, read(io_in, UInt8))
            end
        end
    end
    return nothing
end

################################################################################
# Buffered Reading/Writing

function Base.open(s3Path::S3Path, mode::AbstractString; buffersize=DEFAULTBUFFERSIZE)
    @assert mode ∈ Set(["w", "r"]) "Only read or write is supported"
    if mode == "w"
        S3WriteBuffer(s3Path; buffersize=buffersize)
    else
        S3ReadBuffer(s3Path; buffersize=buffersize)
    end
end

function Base.open(f::Function, s3Path::S3Path, mode::AbstractString; buffersize=DEFAULTBUFFERSIZE)
    io = open(s3Path, mode; buffersize=buffersize)
    f(io)
    close(io)
end

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

Base.isopen(io::S3WriteBuffer) = io.isopen
Base.isreadable(io::S3WriteBuffer) = false
Base.iswritable(io::S3WriteBuffer) = io.isopen
Base.position(io::S3WriteBuffer) = io.position

function Base.write(io::S3WriteBuffer, byte::UInt8)
    @assert iswritable(io) "Buffer isn't writeable"

    io.position += 1

    if io.buffer.ptr > io.buffer.maxsize
        flush(io)
    end

    write(io.buffer, byte)
end

function Base.flush(io::S3WriteBuffer)
    @assert iswritable(io) "Buffer isn't writeable"

    towrite = io.buffer.ptr

    if towrite > 1

        # Writing to S3 is only done via flush (unless on close
        # only a small amount of data was written)
        # Because the user can always call flush, we need to
        # make sure that a multipart upload is started.
        if ismissing(io.uploadid)
            io.uploadid = create_multipart_upload(io.s3Path)
        end

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
        seekstart(io.buffer)
    end

    return towrite
end

function Base.close(io::S3WriteBuffer)
    if io.isopen

        if ismissing(io.uploadid)

            # Don't use multipart upload we only have a small
            # amount of data to write
            write(io.s3Path, io.buffer.data[1:io.buffer.ptr-1])

        else

            flush(io)

            finalise_multipart_upload(
                io.s3Path,
                io.uploadid,
                io.writtenparts
            )

        end

        io.isopen = false

    end

    return Nothing

end

mutable struct S3ReadBuffer <: Base.IO
    buffer::Vector{UInt8}
    ptr::UInt64
    s3Path::S3Path
    isopen::Bool
    size::UInt64
    position
    function S3ReadBuffer(
        s3Path::S3Path;
        buffersize = DEFAULTBUFFERSIZE
    )
        new(
            Vector{UInt8}(undef, buffersize),
            buffersize,
            s3Path,
            true,
            parse(
                UInt64,
                S3.head_object(
                    s3Path.bucket,
                    s3Path.path;
                    aws_config=s3Path.aws_config
                )["Content-Length"]),
            0
        )
    end
end

Base.isopen(io::S3ReadBuffer) = io.isopen
Base.isreadable(io::S3ReadBuffer) = io.isopen
Base.iswritable(io::S3ReadBuffer) = false
Base.position(io::S3WriteBuffer) = io.position

function Base.bytesavailable(io::S3ReadBuffer)
    max(length(io.buffer) - io.ptr, 0)
end

function Base.read(io::S3ReadBuffer, ::Type{UInt8})
    if io.position >= io.size
        throw(EOFError())
    end

    if bytesavailable(io) == 0

        from_byte = io.position
        to_byte   = min(from_byte + length(io.buffer), io.size) - 1

        # zero indexed
        result = S3.get_object(
            io.s3Path.bucket,
            io.s3Path.path,
            # Force Binary Format
            Dict(
                "response-content-type" => "application/octet-stream",
                "range" => "bytes=$(from_byte)-$(to_byte)"
            );
            aws_config=io.s3Path.aws_config
        )# Testing with mock [from_byte+1:to_byte+1] and smaller buffersize

        io.ptr = length(io.buffer) - length(result)
        io.buffer[(io.ptr+1):end] .= result

    end

    io.ptr += 1
    io.position += 1

    return io.buffer[io.ptr]

end

function Base.eof(io::S3ReadBuffer)
    return !(io.isopen && io.position < io.size)
end

function Base.close(io::S3ReadBuffer)
    io.isopen = false

    return Nothing
end

export S3Path

end
