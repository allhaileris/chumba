set(NAME chumba)

cmake_minimum_required(VERSION 3.16)
set(GNUSTEP gnustep-config)
project(${NAME})

find_package(PkgConfig REQUIRED)

add_custom_target(prepare ALL
        ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release .. &&
        ${CMAKE_COMMAND} --build . --target prepare_cross_compiling &&
        cd ../td && php SplitSource.php
        )

# tdlib
add_subdirectory(td)

##
# gnustep
#
execute_process(COMMAND ${GNUSTEP} --objc-flags
        OUTPUT_VARIABLE GNUSTEP_FLAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE)
separate_arguments(GNUSTEP_FLAGS UNIX_COMMAND ${GNUSTEP_FLAGS})

execute_process(COMMAND ${GNUSTEP} --base-libs
        OUTPUT_VARIABLE GNUSTEP_LIBS
        OUTPUT_STRIP_TRAILING_WHITESPACE)

#####################################################################################

set(OBJECTIVE_C_SRCS
        src/main.m
        src/Env.m
        src/Application.m
        src/Telegram.m
        )

add_executable(${NAME} ${OBJECTIVE_C_SRCS})
target_compile_options(${NAME} PUBLIC ${GNUSTEP_FLAGS})
target_link_libraries(${NAME} PUBLIC ${GNUSTEP_LIBS} Td::TdJson)

add_custom_command(
        TARGET ${NAME} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy
                ${CMAKE_SOURCE_DIR}/.env
                ${CMAKE_CURRENT_BINARY_DIR}/.env)
