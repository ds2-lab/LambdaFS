project (hops_streaming)

cmake_minimum_required (VERSION 2.8)
include(JNIFlags.cmake NO_POLICY_SCOPE)
find_package(JNI REQUIRED)
INCLUDE_DIRECTORIES(${JNI_INCLUDE_DIRS})

set(HOPS_STREAMING_VERSION_MAJOR 2)
set(HOPS_STREAMING_VERSION_MINOR 7)
set(HOPS_STREAMING_VERSION_BUILD 2)
configure_file(${CMAKE_SOURCE_DIR}/main/native/streaming/Version.h.in ${CMAKE_SOURCE_DIR}/main/native/streaming/include/Version.h)

include_directories (${CMAKE_SOURCE_DIR}/main/native/streaming/include)
include_directories (${CMAKE_SOURCE_DIR}/main/native/streaming/include_ndb)
#if(NOT NDB_DIR)
#  set(NDB_DIR "/usr/local/mysql")
#  message(STATUS "NDB_DIR was not defined. Default: ${NDB_DIR} ")
#endif()

#include_directories(${NDB_DIR}/include ${NDB_DIR}/storage/ndb/include/ndbapi ${NDB_DIR}/storage/ndb/include)
#link_directories(${NDB_DIR}/lib)

SET( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wformat-security  -Woverloaded-virtual -Wno-unused-parameter -Wno-unused-but-set-variable -Wno-strict-aliasing -Wno-switch -O3 -DDBUG_OFF" )

file(GLOB SOURCE ${CMAKE_SOURCE_DIR}/main/native/streaming/src/*.cpp)

add_library(hopsyarn SHARED ${SOURCE})

target_link_libraries(hopsyarn ndbclient pthread)

function(output_directory TGT DIR)
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
    SET_TARGET_PROPERTIES(${TGT} PROPERTIES
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/${DIR}")
endfunction(output_directory TGT DIR)


output_directory(hopsyarn ../classes)
