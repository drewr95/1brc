cmake_minimum_required(VERSION 3.30)

project(1brc
    DESCRIPTION "One billion row challenge"
    LANGUAGES CXX
    VERSION 0.1.0
)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Export compiler commands
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set output directory for binaries
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Fetch libraries
include (FetchContent)

FetchContent_Declare(
    rapidcsv
    GIT_REPOSITORY git@github.com:d99kris/rapidcsv.git
    GIT_TAG 03133b59e17ca92ecf886b611d87a03ad2bb2a8f
    SYSTEM
)

FetchContent_MakeAvailable(rapidcsv)

# Add subdirectories
add_subdirectory(src)
