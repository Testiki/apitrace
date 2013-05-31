add_library (lz4_bundled STATIC EXCLUDE_FROM_ALL
    lz4.c
    lz4hc.c
)

set_target_properties (lz4_bundled PROPERTIES
    COMPILE_FLAGS "${CMAKE_SHARED_LIBRARY_CXX_FLAGS}"
)
