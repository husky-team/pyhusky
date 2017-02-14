# Copyright 2016 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include_directories(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/husky ${HUSKY_EXTERNAL_INCLUDE})

add_library(pyhusky-backend-objs OBJECT 
    itc.cpp
    operation.cpp
    opdag.cpp
    register.cpp
    python_connector.cpp
    thread_connector.cpp
    daemon_driver.cpp
    worker_driver.cpp
)
set_property(TARGET pyhusky-backend-objs PROPERTY CXX_STANDARD 14)

add_subdirectory(library)

add_library(pyhusky-backend STATIC
    $<TARGET_OBJECTS:pyhusky-backend-objs>
    $<TARGET_OBJECTS:pyhusky-backend-library-objs>)
set_property(TARGET pyhusky-backend PROPERTY CXX_STANDARD 14)
