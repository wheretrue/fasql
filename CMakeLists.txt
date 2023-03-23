cmake_minimum_required(VERSION 3.21)

# Set extension name here
set(TARGET_NAME fasql)

set(CMAKE_CXX_STANDARD 17)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

Include(FetchContent)
FetchContent_Declare(
  kseqpp
  GIT_REPOSITORY https://github.com/cartoonist/kseqpp.git
  GIT_TAG v1.0.0)

list(APPEND available_contents kseqpp)

FetchContent_MakeAvailable(${available_contents})

include_directories(${kseqpp_SOURCE_DIR}/include)

find_package(ZLIB REQUIRED)

set(EXTENSION_SOURCES src/fasql_extension.cpp src/fasta_io.cpp src/fastq_io.cpp)
add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME}
  PUBLIC
  ZLIB::ZLIB)

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
