using Conda
using Pkg
using TestItemRunner

const conda_package_path = "$(dirname(@__DIR__))/.testpython"
ENV["CONDA_JL_HOME"] = conda_package_path
if !isfile("$(conda_package_path)/bin/moto_server")
    Pkg.build("Conda")
    Conda.add("moto[server]", conda_package_path; channel="conda-forge")
end

@run_package_tests
