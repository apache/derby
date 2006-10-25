--
--   Licensed to the Apache Software Foundation (ASF) under one or more
--   contributor license agreements.  See the NOTICE file distributed with
--   this work for additional information regarding copyright ownership.
--   The ASF licenses this file to You under the Apache License, Version 2.0
--   (the "License"); you may not use this file except in compliance with
--   the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
--   Unless required by applicable law or agreed to in writing, software
--   distributed under the License is distributed on an "AS IS" BASIS,
--   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--   See the License for the specific language governing permissions and
--   limitations under the License.
--
-- Very basic single user testing of update row locking.

run resource 'createTestProcedures.subsql';
autocommit off;
run resource 'LockTableQuery.subsql';
commit;

-- READ COMMITTED TEST
set isolation read committed;
commit;

-- run each test with rows on one page in the interesting conglomerate (heap in
-- the non-index tests, and in the index in the index based tests).

-- cursor, no index run
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    
    create table a(a int, b int);
    alter table a add column c varchar(1900);
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    commit;
    run resource 'updateholdcursorlocksJDBC30.subsql';


-- cursor, unique index run
    drop table a;
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    
    create table a(a int, b int);
    alter table a add column c varchar(1900); 
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    create unique index a_idx on a (a);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';

-- cursor, non-unique index run
    drop table a;
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    
    create table a(a int, b int);
    alter table a add column c varchar(1900);
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    create index a_idx on a (a);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';


-- run each test with rows across multiple pages in the interesting 
-- conglomerate (heap in the non-index tests, and in the index in the index 
-- based tests).

-- cursor, no index run
    drop table a;
    call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
    create table a(a int, b int, c varchar(1900));
    insert into a values (1, 10, PADSTRING('one',1900));
    insert into a values (2, 20, PADSTRING('two',1900));
    insert into a values (3, 30, PADSTRING('three',1900));
    insert into a values (4, 40, PADSTRING('four',1900));
    insert into a values (5, 50, PADSTRING('five',1900));
    insert into a values (6, 60, PADSTRING('six',1900));
    insert into a values (7, 70, PADSTRING('seven',1900));
    commit;
    run resource 'updateholdcursorlocksJDBC30.subsql';

-- cursor, unique index run
    drop table a;
    create table a (a int, b int, c varchar(1900), index_pad varchar(600) );
    insert into a values (1, 10, PADSTRING('one',1900), PADSTRING('index pad 1',600));
    insert into a values (2, 20, PADSTRING('two',1900), PADSTRING('index pad 2',600));
    insert into a values (3, 30, PADSTRING('three',1900), PADSTRING('index pad 3',600));
    insert into a values (4, 40, PADSTRING('four',1900), PADSTRING('index pad 4',600));
    insert into a values (5, 50, PADSTRING('five',1900), PADSTRING('index pad 5',600));
    insert into a values (6, 60, PADSTRING('six',1900), PADSTRING('index pad 6',600));
    insert into a values (7, 70, PADSTRING('seven',1900), PADSTRING('index pad 7',600));
    create unique index a_idx on a (a, index_pad);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';

-- cursor, non-unique index run
    drop table a;
    create table a (a int, b int, c varchar(1900), index_pad varchar(700) );
    insert into a values (1, 10, PADSTRING('one',1900), PADSTRING('index pad 1',700));
    insert into a values (2, 20, PADSTRING('two',1900), PADSTRING('index pad 2',700));
    insert into a values (3, 30, PADSTRING('three',1900), PADSTRING('index pad 3',700));
    insert into a values (4, 40, PADSTRING('four',1900), PADSTRING('index pad 4',700));
    insert into a values (5, 50, PADSTRING('five',1900), PADSTRING('index pad 5',700));
    insert into a values (6, 60, PADSTRING('six',1900), PADSTRING('index pad 6',700));
    insert into a values (7, 70, PADSTRING('seven',1900), PADSTRING('index pad 7',700));
    create index a_idx on a (a, index_pad);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';


commit;

-- REPEATABLE READ TEST
--   should be the same as SERIALIZABLE results except no previous key locks.

set isolation RS;
commit;

-- run each test with rows on one page in the interesting conglomerate (heap in
-- the non-index tests, and in the index in the index based tests).

-- cursor, no index run
    drop table a;
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
    create table a(a int, b int);
    alter table a add column  c varchar(1900);
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    commit;
    run resource 'updateholdcursorlocksJDBC30.subsql';

-- cursor, unique index run
    drop table a;
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    
    create table a(a int, b int);
    alter table a add column c varchar(1900);
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    create unique index a_idx on a (a);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';

-- cursor, non-unique index run
    drop table a;
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    
    create table a(a int, b int);
    alter table a add column c varchar(1900);
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    create index a_idx on a (a);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';


-- run each test with rows across multiple pages in the interesting 
-- conglomerate (heap in the non-index tests, and in the index in the index 
-- based tests).

-- cursor, no index run
    drop table a;
    call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
    create table a (a int, b int, c varchar(1900));
    insert into a values (1, 10, PADSTRING('one',1900));
    insert into a values (2, 20, PADSTRING('two',1900));
    insert into a values (3, 30, PADSTRING('three',1900));
    insert into a values (4, 40, PADSTRING('four',1900));
    insert into a values (5, 50, PADSTRING('five',1900));
    insert into a values (6, 60, PADSTRING('six',1900));
    insert into a values (7, 70, PADSTRING('seven',1900));
    commit;
    run resource 'updateholdcursorlocksJDBC30.subsql';


-- cursor, unique index run
    drop table a;
    create table a (a int, b int, c varchar(1900), index_pad varchar(600) );
    insert into a values (1, 10, PADSTRING('one',1900), PADSTRING('index pad 1',600));
    insert into a values (2, 20, PADSTRING('two',1900), PADSTRING('index pad 2',600));
    insert into a values (3, 30, PADSTRING('three',1900), PADSTRING('index pad 3',600));
    insert into a values (4, 40, PADSTRING('four',1900), PADSTRING('index pad 4',600));
    insert into a values (5, 50, PADSTRING('five',1900), PADSTRING('index pad 5',600));
    insert into a values (6, 60, PADSTRING('six',1900), PADSTRING('index pad 6',600));
    insert into a values (7, 70, PADSTRING('seven',1900), PADSTRING('index pad 7',600));
    create unique index a_idx on a (a, index_pad);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';

-- cursor, non-unique index run
    drop table a;
    create table a (a int, b int, c varchar(1900), index_pad varchar(700) );
    insert into a values (1, 10, PADSTRING('one',1900), PADSTRING('index pad 1',700));
    insert into a values (2, 20, PADSTRING('two',1900), PADSTRING('index pad 2',700));
    insert into a values (3, 30, PADSTRING('three',1900), PADSTRING('index pad 3',700));
    insert into a values (4, 40, PADSTRING('four',1900), PADSTRING('index pad 4',700));
    insert into a values (5, 50, PADSTRING('five',1900), PADSTRING('index pad 5',700));
    insert into a values (6, 60, PADSTRING('six',1900), PADSTRING('index pad 6',700));
    insert into a values (7, 70, PADSTRING('seven',1900), PADSTRING('index pad 7',700));
    create index a_idx on a (a, index_pad);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';

commit;

-- SERIALIZABLE TEST
set isolation serializable;
commit;

-- run each test with rows on one page in the interesting conglomerate (heap in
-- the non-index tests, and in the index in the index based tests).

-- cursor, no index run
    drop table a;
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', NULL);
    create table a(a int, b int);
    alter table a add column c varchar(1900);
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    commit;
    run resource 'updateholdcursorlocksJDBC30.subsql';


-- cursor, unique index run
    drop table a;
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    
    create table a(a int, b int);
    alter table a add column c varchar(1900);
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    create unique index a_idx on a (a);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';

-- cursor, non-unique index run
    drop table a;
    -- to create tables of page size 4k and still keep the following tbl 
    -- create table a (a int, b int, c varchar(1900));
    
    create table a(a int, b int);
    alter table a add column c varchar(1900);
    insert into a values (1, 10, 'one');
    insert into a values (2, 20, 'two');
    insert into a values (3, 30, 'three');
    insert into a values (4, 40, 'four');
    insert into a values (5, 50, 'five');
    insert into a values (6, 60, 'six');
    insert into a values (7, 70, 'seven');
    create index a_idx on a (a);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';

-- run each test with rows across multiple pages in the interesting 
-- conglomerate (heap in the non-index tests, and in the index in the index 
-- based tests).

-- cursor, no index run
    drop table a;
    call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '4096');
    create table a (a int, b int, c varchar(1900));
    insert into a values (1, 10, PADSTRING('one',1900));
    insert into a values (2, 20, PADSTRING('two',1900));
    insert into a values (3, 30, PADSTRING('three',1900));
    insert into a values (4, 40, PADSTRING('four',1900));
    insert into a values (5, 50, PADSTRING('five',1900));
    insert into a values (6, 60, PADSTRING('six',1900));
    insert into a values (7, 70, PADSTRING('seven',1900));
    commit;
    run resource 'updateholdcursorlocksJDBC30.subsql';

-- cursor, unique index run
    drop table a;
    create table a (a int, b int, c varchar(1900), index_pad varchar(600) );
    insert into a values (1, 10, PADSTRING('one',1900), PADSTRING('index pad 1',600));
    insert into a values (2, 20, PADSTRING('two',1900), PADSTRING('index pad 2',600));
    insert into a values (3, 30, PADSTRING('three',1900), PADSTRING('index pad 3',600));
    insert into a values (4, 40, PADSTRING('four',1900), PADSTRING('index pad 4',600));
    insert into a values (5, 50, PADSTRING('five',1900), PADSTRING('index pad 5',600));
    insert into a values (6, 60, PADSTRING('six',1900), PADSTRING('index pad 6',600));
    insert into a values (7, 70, PADSTRING('seven',1900), PADSTRING('index pad 7',600));
    create unique index a_idx on a (a, index_pad);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';

-- cursor, non-unique index run
    drop table a;
    create table a (a int, b int, c varchar(1900), index_pad varchar(700) );
    insert into a values (1, 10, PADSTRING('one',1900), PADSTRING('index pad 1',700));
    insert into a values (2, 20, PADSTRING('two',1900), PADSTRING('index pad 2',700));
    insert into a values (3, 30, PADSTRING('three',1900), PADSTRING('index pad 3',700));
    insert into a values (4, 40, PADSTRING('four',1900), PADSTRING('index pad 4',700));
    insert into a values (5, 50, PADSTRING('five',1900), PADSTRING('index pad 5',700));
    insert into a values (6, 60, PADSTRING('six',1900), PADSTRING('index pad 6',700));
    insert into a values (7, 70, PADSTRING('seven',1900), PADSTRING('index pad 7',700));
    create index a_idx on a (a, index_pad);
    commit;
    run resource 'updateBtreeHoldCursorLocksJDBC30.subsql';


commit;
exit;
