drop table t1;
create table t1 (i integer primary key, j integer, c char(200));
insert into t1 values (1, 1, 'a');
insert into t1 (select t1.i + 2,    t1.j + 2,    t1.c from t1);
insert into t1 (select t1.i + 4,    t1.j + 4,    t1.c from t1);
insert into t1 (select t1.i + 8,    t1.j + 8,    t1.c from t1);
insert into t1 (select t1.i + 16,   t1.j + 16,   t1.c from t1);
insert into t1 (select t1.i + 32,   t1.j + 32,   t1.c from t1);
insert into t1 (select t1.i + 64,   t1.j + 64,   t1.c from t1);
insert into t1 (select t1.i + 128,  t1.j + 128,  t1.c from t1);
insert into t1 (select t1.i + 256,  t1.j + 256,  t1.c from t1);
insert into t1 (select t1.i + 512,  t1.j + 512,  t1.c from t1);
insert into t1 (select t1.i + 1024, t1.j + 1024, t1.c from t1);

delete from t1 where j=1;

CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'T1', 1, 1, 1);

delete from t1 where j=2;

CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'T1', 1, 1, 1);

delete from t1 where i > 1024;

CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'T1', 1, 1, 1);

delete from t1 where i < 512;

-- prior to the fix the following compress would result in a deadlock
CALL SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE('APP', 'T1', 1, 1, 1);
