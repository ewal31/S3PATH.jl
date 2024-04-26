@testitem "Integration Test" begin

    const test_server_port = 3000
    const test_bucket = "bucket"

    using AWS
    @service S3

    # Modified to use custom test endpoint
    struct TestConfig <: AbstractAWSConfig
        endpoint::String
        region::String
        creds
    end
    AWS.region(c::TestConfig) = c.region
    AWS.credentials(c::TestConfig) = c.creds
    function AWS.generate_service_url(aws::TestConfig, service::String, resource::String)
        return string(aws.endpoint, resource)
    end

    struct TestCredentials
        access_key_id::String
        secret_key::String
        token::String
    end
    AWS.check_credentials(c::TestCredentials) = c

    # Set Test Credentials
    const aws_config = AWS.global_aws_config(TestConfig("http://127.0.0.1:$(test_server_port)", "eu-central-1", TestCredentials("id", "key", "token")))

    # Start Test Server
    # Dashboard while running http://localhost:$(test_server_port)/moto-api/
    server_process = run(`"$(ENV["CONDA_JL_HOME"])/bin/moto_server" -p $(test_server_port)`; wait = false)

    try

        # Create Test Bucket
        S3.create_bucket(
            test_bucket,
            Dict(
                "body" => """
                    <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                    <LocationConstraint>Europe</LocationConstraint>
                    </CreateBucketConfiguration >
                """
        ); aws_config = aws_config)

        ############################################
        # Test reading and writing to a file
        #
        s3file = S3Path(test_bucket, "file")
        to_write = "This is some random string"

        write(s3file, to_write)
        result = read(String, s3file)

        @test result == to_write
        @test isfile(s3file)
        @test !isdir(s3file)

        rm(s3file)
        @test !isfile(s3file)
        @test !isdir(s3file)

        ############################################
        # Test creating a directory
        #
        s3dir = S3Path(test_bucket, "directory/")
        mkpath(s3dir)
        @test !isfile(s3dir)
        @test isdir(s3dir)

        ############################################
        # Test readdir
        #
        paths = readdir(s3dir)
        @test length(paths) == 0

        for i in 1:10
            write(joinpath(s3dir, "file_$(i)"), "This is file $(i)")
        end

        paths = readdir(s3dir)
        @test length(paths) == 10
        @test all(typeof.(paths) .== String)

        s3paths = readdir(s3dir; join=true)
        @test length(s3paths) == 10
        @test all(typeof.(s3paths) .== S3Path)

        @test all(dirname.(s3paths) .== Ref(s3dir))
        @test basename.(s3paths) == paths

        ############################################
        # Test splitdir
        #
        @test splitdir(S3Path("s3://bucket/")) == (S3Path("s3://bucket/"), "")
        @test splitdir(S3Path("s3://bucket/dir/")) == (S3Path("s3://bucket/"), "dir/")
        @test splitdir(S3Path("s3://bucket/dir/file")) == (S3Path("s3://bucket/dir/"), "file")
        @test splitdir(S3Path("s3://bucket/dir/dir2/")) == (S3Path("s3://bucket/dir/"), "dir2/")
        @test splitdir(S3Path("s3://bucket/dir/dir2/file2")) == (S3Path("s3://bucket/dir/dir2/"), "file2")
        @test splitdir(splitdir(S3Path("s3://bucket/dir/dir2/file2"))[1]) == (S3Path("s3://bucket/dir/"), "dir2/")

    finally

        # Stop Test Server
        kill(server_process)
        wait(server_process)

    end

end
