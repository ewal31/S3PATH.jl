@testitem "write bytes smaller than buffer" begin

    const towrite = "some small string"
    const written = Vector{UInt8}()

    # Mock Relevant Methods
    function Base.write(io::S3PATH.S3Path, content::Vector{UInt8}; blocksize=S3PATH.DEFAULTBUFFERSIZE)
        append!(written, content)
    end

    # Run Test
    open(S3Path("s3://bucket/object"; aws_config=missing), "w") do io
        @test isopen(io)

        for (i, c) in enumerate(towrite)
            write(io, c)
            @test position(io) == i
        end

        @test ismissing(io.uploadid) # Hasn't created an upload request as buffer isn't full
        @test position(io) == length(towrite)
    end

    @test towrite == String(written)

end

@testitem "write bytes larger than buffer" begin

    const bytes_to_write = 10000
    const buffer_size = 256
    const towrite = rand(UInt8, bytes_to_write)
    const total_parts_written = Int(ceil(bytes_to_write / 256))
    const uploadid_to_return = "this is an id"
    const written = Vector{UInt8}()

    # Mock Relevant Methods
    function S3PATH.S3.create_multipart_upload(bucket, path; aws_config=nothing)
        return Dict("UploadId" => uploadid_to_return)
    end

    function S3PATH.upload_part(s3Path::S3Path, uploadid, partnumber, part::V) where V <: AbstractVector{UInt8}
        @test uploadid == uploadid_to_return
        append!(written, part)
        return "$(partnumber)" # number and etag
    end

    function S3PATH.finalise_multipart_upload(s3Path::S3Path, uploadid, upload_ids)
        @test uploadid == uploadid_to_return
        @test length(upload_ids) == total_parts_written
    end

    # Run Test
    open(S3Path("s3://bucket/object"; aws_config=missing), "w"; buffersize=buffer_size) do io
        @test isopen(io)

        for (i, c) in enumerate(towrite)
            write(io, c)
            @test position(io) == i
        end

        @test !ismissing(io.uploadid) # Should have written multiple parts
        @test position(io) == length(towrite)
    end

    @test bytes_to_write == length(written)
    @test towrite == written

end

@testitem "write contents smaller than buffer" begin

    const towrite = "some small string"
    const written = Vector{UInt8}()

    # Mock Relevant Methods
    function Base.write(io::S3PATH.S3Path, content::Vector{UInt8}; blocksize=DEFAULTBUFFERSIZE)
        append!(written, content)
    end

    # Run Test
    open(S3Path("s3://bucket/object"; aws_config=missing), "w") do io
        @test isopen(io)

        write(io, towrite)

        @test ismissing(io.uploadid) # Hasn't created an upload request as buffer isn't full
        @test position(io) == length(towrite)
    end

    @test towrite == String(written)

end

@testitem "write contents larger than buffer" begin

    const uploadid_to_return = "this is an id"
    const written = Vector{UInt8}()
    total_parts_written = 2

    # Mock Relevant Methods
    function S3PATH.S3.create_multipart_upload(bucket, path; aws_config=nothing)
        return Dict("UploadId" => uploadid_to_return)
    end

    function S3PATH.upload_part(s3Path::S3Path, uploadid, partnumber, part::V) where V <: AbstractVector{UInt8}
        @test uploadid == uploadid_to_return
        append!(written, part)
        return "$(partnumber)" # number and etag
    end

    function S3PATH.finalise_multipart_upload(s3Path::S3Path, uploadid, upload_ids)
        @test uploadid == uploadid_to_return
        @test length(upload_ids) == total_parts_written
    end

    # Test: single line longer than the buffer

    empty!(written)
    towrite = "a much longer string"
    total_parts_written = 2

    open(S3Path("s3://bucket/object"; aws_config=missing), "w"; buffersize=12) do io
        @test isopen(io)

        write(io, towrite)

        @test io.uploadid == uploadid_to_return
        @test position(io) == length(towrite)
    end

    @test String(written) == towrite

    # Test: single line much longer than the buffer

    empty!(written)
    towrite = "a much much much longer string"
    total_parts_written = 6

    open(S3Path("s3://bucket/object"; aws_config=missing), "w"; buffersize=5) do io
        @test isopen(io)

        write(io, towrite)

        @test io.uploadid == uploadid_to_return
        @test position(io) == length(towrite)
    end

    @test String(written) == towrite

    # Test: multiple lines first shorter than buffer

    empty!(written)
    towrite1 = "word"
    towrite2 = "something else"
    total_parts_written = 4

    open(S3Path("s3://bucket/object"; aws_config=missing), "w"; buffersize=5) do io
        @test isopen(io)

        write(io, towrite1)
        @test position(io) == length(towrite1)
        write(io, towrite2)
        @test position(io) == length(towrite1) + length(towrite2)

        @test io.uploadid == uploadid_to_return
    end

    @test String(written) == towrite1 * towrite2

    # Test: random data

    empty!(written)
    towrite = rand(UInt8, 10000)
    total_parts_written = Int(ceil(10000 / 256))

    open(S3Path("s3://bucket/object"; aws_config=missing), "w"; buffersize=256) do io
        @test isopen(io)

        write(io, towrite)

        @test io.uploadid == uploadid_to_return
        @test position(io) == length(towrite)
    end

    @test written == towrite

end
