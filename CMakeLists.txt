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
    googletest
    GIT_REPOSITORY git@github.com:google/googletest.git
    GIT_TAG v1.17.0
    SYSTEM
)

FetchContent_MakeAvailable(googletest)
enable_testing() # Enable testing for CTest integration.

# Add subdirectories
add_subdirectory(src)
