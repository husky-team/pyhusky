include_directories(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/husky ${HUSKY_EXTERNAL_INCLUDE})

if(DEP_FOUND)
    include_directories(${ZMQ_INCLUDE_DIR})
endif(DEP_FOUND)

add_library(pyhusky-manager-master-objs OBJECT
  opdag.cpp
  operation.cpp
  job.cpp
  opsplitter.cpp
  optimizer.cpp
  frontend_master_handlers.cpp
)
set_property(TARGET pyhusky-manager-master-objs PROPERTY CXX_STANDARD 14)

add_library(pyhusky-manager-daemon-objs OBJECT
  opdag.cpp
  operation.cpp
  itc.cpp
  task.cpp
)
set_property(TARGET pyhusky-manager-daemon-objs PROPERTY CXX_STANDARD 14)

add_library(pyhusky-master STATIC
    $<TARGET_OBJECTS:pyhusky-manager-master-objs>
    $<TARGET_OBJECTS:pyhusky-backend-master-objs>
)
set_property(TARGET pyhusky-master PROPERTY CXX_STANDARD 14)

add_library(pyhusky-daemon STATIC
    $<TARGET_OBJECTS:pyhusky-manager-daemon-objs>
    $<TARGET_OBJECTS:pyhusky-backend-objs>    
    $<TARGET_OBJECTS:pyhusky-backend-library-objs>
)
set_property(TARGET pyhusky-daemon PROPERTY CXX_STANDARD 14)

### PyHuskyDaemon

add_executable(PyHuskyDaemon daemon.cpp)
target_link_libraries(PyHuskyDaemon pyhusky-daemon)
target_link_libraries(PyHuskyDaemon husky)
target_link_libraries(PyHuskyDaemon ${HUSKY_EXTERNAL_LIB})
set_property(TARGET PyHuskyDaemon PROPERTY CXX_STANDARD 14)

### PyHuskyMaster

add_executable(PyHuskyMaster master.cpp $<TARGET_OBJECTS:husky-master-objs>)
target_link_libraries(PyHuskyMaster pyhusky-master)
target_link_libraries(PyHuskyMaster husky)
target_link_libraries(PyHuskyMaster ${husky-master-dep})
target_link_libraries(PyHuskyMaster ${HUSKY_EXTERNAL_LIB})
if(WIN32)
    set_target_properties(PyHuskyMaster PROPERTIES COMPILER_FLAG "/EHsc")
endif()
set_property(TARGET PyHuskyMaster PROPERTY CXX_STANDARD 14)
