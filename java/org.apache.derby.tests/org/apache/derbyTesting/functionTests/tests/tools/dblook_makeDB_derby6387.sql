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

--
-- Script for creating the test database that will be used for testing
-- that dblook returns triggers in the correct order (which is
-- important because creation order decides execution order).
-- Regression test case for DERBY-6387.
--

create table t1(x int);

-- Create enough triggers to fill at least one page in
-- SYS.SYSTRIGGERS.

create trigger tr01 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr02 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr03 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr04 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr05 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr06 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr07 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr08 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr09 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr10 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr11 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr12 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr13 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);

-- Drop some of the triggers to create a hole on the first page of
-- SYS.SYSTRIGGERS.

drop trigger tr01;
drop trigger tr02;
drop trigger tr03;
drop trigger tr04;
drop trigger tr05;
drop trigger tr06;
drop trigger tr07;
drop trigger tr08;
drop trigger tr09;
drop trigger tr10;
drop trigger tr11;
drop trigger tr12;
call syscs_util.syscs_checkpoint_database();

-- Now fill up the second page of SYS.SYSTRIGGERS. When it's full,
-- it'll start inserting into the first page, and then the rows in the
-- table are not in creation order. That's what we need in order to
-- test that the bug is fixed.

create trigger tr14 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr15 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr16 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr17 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr18 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr19 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr20 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr21 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr22 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr23 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
create trigger tr24 after update on t1 referencing new table as new for each statement select * from sys.systables natural join sys.sysschemas where exists(select * from new where x > 100);
