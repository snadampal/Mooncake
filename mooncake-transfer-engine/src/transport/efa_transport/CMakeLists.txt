file(GLOB EFA_TRANSPORT_SOURCES "*.cpp")
add_library(efa_transport OBJECT ${EFA_TRANSPORT_SOURCES})

# Find libfabric
find_library(FABRIC_LIBRARY NAMES fabric PATHS /opt/amazon/efa/lib /usr/local/lib /usr/lib NO_DEFAULT_PATH)
if(NOT FABRIC_LIBRARY)
  find_library(FABRIC_LIBRARY NAMES fabric)
endif()

if(FABRIC_LIBRARY)
  message(STATUS "Found libfabric: ${FABRIC_LIBRARY}")
else()
  message(FATAL_ERROR "libfabric not found. Please install AWS EFA or libfabric.")
endif()

# Add include directories
target_include_directories(efa_transport PRIVATE /opt/amazon/efa/include)
target_link_libraries(efa_transport PRIVATE ${FABRIC_LIBRARY} glog::glog)
