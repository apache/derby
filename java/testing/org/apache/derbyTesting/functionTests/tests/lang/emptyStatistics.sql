-- test to verify that RTS functionality is stubbed out
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
values 1, 2, 3;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
