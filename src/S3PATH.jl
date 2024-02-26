module S3PATH

using AWS
using Retry

@service S3

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

Base.read(p::S3Path) = read(Vector{UInt8}, p)

function Base.read(type, p::S3Path)
    type(
        S3.get_object(
            p.bucket,
            p.path,
            aws_config=p.aws_config
        )
    )
end

Base.write(p::S3Path, content::AbstractString) = Base.write(p::S3Path, Vector{UInt8}(content))

function Base.write(p::S3Path, content::Vector{UInt8})
    block_size = 5 * 1_048_576

    if sizeof(content) < block_size
        @repeat 4 try
            S3.put_object(
                p.bucket,
                p.path,
                Dict(
                    "body" => content,
                    "Content-Length" => sizeof(content),
                );
                aws_config=p.aws_config
            )
        catch e
            @delay_retry if true end
        end

    else
        result = @repeat 4 try
            S3.create_multipart_upload(
                p.bucket,
                p.path;
                aws_config=p.aws_config
            )
        catch e
            @delay_retry if true end
        end

        total_parts = Int(ceil(sizeof(content) / block_size))
        upload_ids = Vector(undef, total_parts)

        Threads.@threads for partnumber in 1:total_parts
            @repeat 4 try
                part = content[(partnumber-1)*block_size+1:min(partnumber*block_size, sizeof(content))]

                _, headers = S3.upload_part(
                    result["Bucket"],
                    result["Key"],
                    partnumber,
                    result["UploadId"],
                    Dict(
                        "body" => part,
                        "Content-Length" => sizeof(part),
                        "return_headers" => true
                    );
                    aws_config=p.aws_config
                )

                upload_ids[partnumber] = (partnumber, filter(p -> p.first == "ETag", headers)[1].second)
            catch e
            end
        end

        body = join(
            vcat(
                "<CompleteMultipartUpload>",
                map(x -> "<Part><PartNumber>$(x[1])</PartNumber><ETag>\"$(x[2])\"</ETag></Part>", upload_ids),
                "</CompleteMultipartUpload>"
            )
        )

        @repeat 4 try
            S3.complete_multipart_upload(
                result["Bucket"],
                result["Key"],
                result["UploadId"],
                Dict("body"=>body)
            )
        catch e
            @delay_retry if true end
        end

    end
end

Base.rm(p::S3Path) = S3.delete_object(p.bucket, p.path; aws_config=p.aws_config)

function Base.mkpath(p::S3Path)
    S3.put_object(p.bucket, p.path; aws_config=p.aws_config)
end

# copy helper functions to copy between S3 <-> S3 or Filesystem <-> S3
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

function Base.joinpath(parts::Union{AbstractString, S3Path}...)
    @assert typeof(first(parts)) <: S3Path "First argument should be an S3Path"
    return S3Path(
        parts[1].bucket,
        joinpath(parts[1].path, parts[2:end]...);
        aws_config=parts[1].aws_config
    )
end

function Base.readdir(fp::S3Path; join=false, sort=true)
    results = String[]
    token = ""
    keylength = length(fp.path)

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
        params = Dict("delimiter" => "/", "prefix" => fp.path)

        if !isempty(token)
            params["continuation-token"] = token
        end

        result = S3.list_objects_v2(fp.bucket, params; aws_config=fp.aws_config)
        token = get(result, "NextContinuationToken", nothing)

        add2result!(result, "CommonPrefixes", "Prefix")
        add2result!(result, "Contents", "Key")
    end

    filter!(x -> !(isempty(x) || x == "/"), results)

    sort && sort!(results)

    return join ? joinpath.(Ref(fp), results) : results
end

# Probably better to split into an S3WriteBuffer and S3ReadBuffer
mutable struct S3Buffer <: Base.IO
    data::IOBuffer
    readable::Bool
    writeable::Bool
    seekable::Bool
    append::Bool
    # size::Int
    # maxsize::Int
    # ptr::Int
    # mark::int

    # Writing
    bucket::Union{AbstractString, Missing}
    key::Union{AbstractString, Missing}
    uploadid::Union{AbstractString, Missing}
    partnumber::UInt64
    written::Vector{Tuple{UInt64}, AbstractString}
    function S3Buffer(readable, writeable)
        @assert readable != writeable
        new(
            IOBuffer(;
                maxsize=5 * 1_048_576 # 5 MB approx
            ),
            readable,
            writeable,
            false,
            false,
            missing,
            missing,
            missing,
            UInt64(0),
            Vector{Tuple{UInt64}, AbstractString}
        )
    end
end

Base.isreadable(io::S3Buffer) = io.readable
Base.iswriteable(io::S3Buffer) = io.writeable
Base.isopen(io::S3Buffer) = io.readable || io.writeable
function Base.close(io::S3Buffer)
    io.readable = false
    io.writeable = false

    # TODO needs to write anything left before closing

    body = join(
        vcat(
            "<CompleteMultipartUpload>",
            map(x -> "<Part><PartNumber>$(x[1])</PartNumber><ETag>\"$(x[2])\"</ETag></Part>", io.written),
            "</CompleteMultipartUpload>"
        )
    )

    @repeat 4 try
        S3.complete_multipart_upload(
            io.bucket,
            io.key,
            io.uploadid,
            Dict("body"=>body)
        )
    catch e
        @delay_retry if true end
    end
end

function Base.open(f::Function, path::S3Path, mode::AbstractString)
    @assert mode ∉ Set(["w", "r"]) "Only read or write is currently supported"
    if mode == "w"
        io = S3Buffer(false, true)
        f(io)
        close(io)
    else
        io = S3Buffer(true, false)
        f(io)
        close(io)
    end
end

Base.read(p::S3Buffer) = read(Vector{UInt8}, p)

function Base.read(type, p::S3Buffer)
    throw(ErrorException("Not Implemented Yet"))

    # GET /example-object HTTP/1.1
    # Host: example-bucket.s3.<Region>.amazonaws.com
    # x-amz-date: Fri, 28 Jan 2011 21:32:02 GMT
    # Range: bytes=0-9
    # Authorization: AWS AKIAIOSFODNN7EXAMPLE:Yxg83MZaEgh3OZ3l0rLo5RTX11o=
    # Sample Response with Specified Range of the Object Bytes
    #
    # aws s3api get-object --bucket DOC-EXAMPLE-BUCKET1 --key folder/my_data --range bytes=0-500 my_data_range
    #
    # just use the get object method with ranges
    #
    # https://juliacloud.github.io/AWS.jl/stable/services/s3.html#Main.S3.get_object-Tuple{Any,%20Any}
    #
    # and I think can find out size of object in advance via
    #
    # https://juliacloud.github.io/AWS.jl/stable/services/s3.html#Main.S3.head_object-Tuple{Any,%20Any}

end

Base.write(p::S3Buffer, content::AbstractString) = Base.write(p::S3Buffer, Vector{UInt8}(content))

function Base.write(p::S3Buffer, content::Vector{UInt8})
    throw(ErrorException("Not Implemented Yet"))

    # If content + buffer too large
    # then open upload if not open
    # write parts until fits in buffer again

    if ismissing(bucket)
        result = @repeat 4 try
            S3.create_multipart_upload(
                p.bucket,
                p.path;
                aws_config=p.aws_config
            )
        catch e
            @delay_retry if true end
        end

        io.bucket = result["Bucket"]
        io.key = result["Key"]
        io.uploadid = result["UploadId"]
    end
end

export S3Path

end
