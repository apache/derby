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
drop table tab1;
create table tab1( c1 decimal(5,3), c2 date, c3 char(20) );
insert into tab1 values(12.345, date('2000-05-25'),  'test row 1');
insert into tab1 values(32.432, date('2000-01-14'),  'test row 2');
insert into tab1 values(54.846, date('2000-08-21'),  'test row 3');
insert into tab1 values(98.214, date('2000-12-08'),  'test row 4');
insert into tab1 values(77.406, date('2000-10-19'),  'test row 5');
insert into tab1 values(50.395, date('2000-11-29'),  'test row 6');

call SYSCS_UTIL.SYSCS_EXPORT_TABLE (null, 'TAB1' , 'extinout/tab1_fr.unl' , 
                                    null, null, 'UTF8') ;

-- localized display is off
select * from tab1;

LOCALIZEDDISPLAY ON;
select * from tab1;

drop table tab1;
create table tab1( c1 decimal(5,3), c2 date, c3 char(20) );

call SYSCS_UTIL.SYSCS_IMPORT_TABLE (null, 'TAB1' , 'extinout/tab1_fr.unl' , 
                                    null, null, 'UTF8', 0) ;


-- localized display is off
LOCALIZEDDISPLAY OFF;
select * from tab1;

LOCALIZEDDISPLAY ON;
select * from tab1;

