@testitem "write contents smaller than buffer" begin

    function S3PATH.S3.create_multipart_upload(bucket, path; aws_config=nothing)
        return Dict("UploadId" => "this is an id")
    end

    open(S3Path("s3://bucket/object"; aws_config=missing), "w") do io
        @test isopen(io)

        write(io, "some small string")
    end

end

@testitem "write contents larger than buffer" begin

    open(S3Path("s3://bucket/object"; aws_config=missing), "w"; buffersize=2) do io
        @test isopen(io)

        write(io, "a much longer string")
    end

end
