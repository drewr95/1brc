
# Add source files
set(SOURCES
    main.cpp
)

# Create executable
add_executable(1brc main.cpp)
target_link_libraries(1brc rapidcsv)
