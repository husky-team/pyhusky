include_directories(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/husky ${Boost_INCLUDE_DIRS})

if(DEP_FOUND)
    include_directories(${ZMQ_INCLUDE_DIR})
endif(DEP_FOUND)

add_library(pyhusky-backend-library-objs OBJECT
    functional.cpp
    linear_regression.cpp
    logistic_regression.cpp
)
set_property(TARGET pyhusky-backend-library-objs PROPERTY CXX_STANDARD 14)
