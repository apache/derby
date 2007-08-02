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
-- this test is for miscellaneous errors
--

-- lexical error
select @#^%*&! from swearwords;

--
-- try to create duplicate table
--


create table a (one int);

create table a (one int, two int);

create table a (one int);

drop table a ;

create table a (one int, two int, three int);

insert into a values (1,2,3);

select * from a;

drop table a;

-- set isolation to repeatable read
set isolation serializable;

-- see that statements that fail at parse or bind time
-- are not put in the statment cache;
values 1;
select SQL_TEXT from syscs_diag.statement_cache where CAST(SQL_TEXT AS LONG VARCHAR) LIKE '%932432%';

VALUES FRED932432;
SELECT * FROM BILL932432;
SELECT 932432;

select SQL_TEXT from syscs_diag.statement_cache where CAST(SQL_TEXT AS LONG VARCHAR) LIKE '%932432%';
