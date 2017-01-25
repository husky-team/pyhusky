cmake_minimum_required(VERSION 3.0.2)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})

add_subdirectory(husky)
add_definitions(${HUSKY_EXTERNAL_DEFINITION})

add_subdirectory(backend)
add_subdirectory(manager)
