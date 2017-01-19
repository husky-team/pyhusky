include_directories(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/husky ${HUSKY_EXTERNAL_INCLUDE})

if(DEP_FOUND)
    include_directories(${ZMQ_INCLUDE_DIR})
endif(DEP_FOUND)

add_library(pyhusky-backend-objs OBJECT 
    register.cpp
    pythonconnector.cpp
    threadconnector.cpp
    daemondriver.cpp
    workerdriver.cpp
)
set_property(TARGET pyhusky-backend-objs PROPERTY CXX_STANDARD 14)

add_library(pyhusky-backend-master-objs OBJECT
    splitter_register.cpp
    master_handler.cpp
)
set_property(TARGET pyhusky-backend-master-objs PROPERTY CXX_STANDARD 14)

# add sub directory
add_subdirectory(library)
