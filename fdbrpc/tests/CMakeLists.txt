if(NOT WIN32)
  add_flow_target(EXECUTABLE NAME authz_tls_unittest SRCS AuthzTlsTest.cpp)
  target_link_libraries(authz_tls_unittest PRIVATE flow fdbrpc fmt::fmt)
  if(NOT OPEN_FOR_IDE)
    add_test(NAME authorization_tls_unittest
      COMMAND $<TARGET_FILE:authz_tls_unittest>)
    set_tests_properties(authorization_tls_unittest PROPERTIES TIMEOUT 120)
  endif()
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_flow_target(EXECUTABLE NAME ktls_unittest SRCS KtlsTest.cpp)
  target_link_libraries(ktls_unittest PRIVATE flow fmt::fmt)
  if(NOT OPEN_FOR_IDE)
    foreach(variant IN ITEMS default main_thread_handshakes tls12 cbc
                              io_uring io_uring_main_thread_handshakes io_uring_tls12 io_uring_cbc
                              io_uring_no_multishot io_uring_few_buffers)
      set(args "")
      string(REGEX REPLACE "^io_uring_?" "" base "${variant}")
      if(NOT base STREQUAL variant)
        list(APPEND args --net-io-uring)
      endif()
      if(base STREQUAL "no_multishot")
        list(APPEND args --no-multishot)
      elseif(base STREQUAL "few_buffers")
        list(APPEND args --recv-buffers=2)
      elseif(base STREQUAL "main_thread_handshakes")
        list(APPEND args --main-thread-handshakes)
      elseif(base STREQUAL "tls12" OR base STREQUAL "cbc")
        list(APPEND args --openssl-conf=${base})
      endif()
      set(dir ${CMAKE_CURRENT_BINARY_DIR}/ktls_unittest_${variant})
      file(MAKE_DIRECTORY ${dir})
      add_test(NAME ktls_unittest/${variant} COMMAND $<TARGET_FILE:ktls_unittest> ${args} WORKING_DIRECTORY ${dir})
      set_tests_properties(ktls_unittest/${variant} PROPERTIES TIMEOUT 120 LABELS "unit;native")
    endforeach()
  endif()
endif()

if(WITH_GRPC)
  generate_grpc_protobuf(fdbrpc.test protos/echo.proto)
endif()
add_flow_target(EXECUTABLE NAME fdbrpc_transport_bench SRCS fdbrpc_bench.cpp)
target_link_libraries(fdbrpc_transport_bench PUBLIC flow fdbrpc boost_target_program_options)

add_flow_target(EXECUTABLE NAME fdbrpc_network_test SRCS networktest_main.cpp networktest.cpp NetworkTest.h)
target_link_libraries(fdbrpc_network_test PRIVATE flow fdbrpc fmt::fmt)

if(FDB_REGISTER_UNIT_TESTS AND Python3_EXECUTABLE)
  add_dependencies(unit_tests fdbrpc_network_test)
  add_test(NAME unit/fdbrpc_network_test/native
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/networktest_smoke.py $<TARGET_FILE:fdbrpc_network_test>)
  set_tests_properties(unit/fdbrpc_network_test/native PROPERTIES
    LABELS "unit;native;fdbrpc_network_test"
    TIMEOUT 90
    ENVIRONMENT "${SANITIZER_OPTIONS}")
endif()
