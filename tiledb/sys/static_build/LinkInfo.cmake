message("Adding Link Info Executable")
add_executable(link_info link_info.cc)
target_link_libraries(link_info tiledb)
