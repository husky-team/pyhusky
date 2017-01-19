cmake_minimum_required(VERSION 3.0.2)

set(dep_DIR ${PROJECT_SOURCE_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

add_subdirectory(husky)
add_subdirectory(backend)
add_subdirectory(manager)
