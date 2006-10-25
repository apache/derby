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

drop function PADSTRING;
drop function RANDOM;
CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Formatters.padString' LANGUAGE JAVA PARAMETER STYLE JAVA;

CREATE FUNCTION RANDOM() RETURNS DOUBLE EXTERNAL NAME 'java.lang.Math.random' LANGUAGE JAVA PARAMETER STYLE JAVA;


drop table main;
drop table main2;

create table main(x int not null primary key, y varchar(2000));
insert into main values(1, PADSTRING('aaaa',2000));
insert into main values(2, PADSTRING('aaaa',2000));
insert into main values(3, PADSTRING('aaaa',2000));
insert into main values(4, PADSTRING('aaaa',2000));
insert into main values(5, PADSTRING('aaaa',2000));
insert into main values(6, PADSTRING('aaaa',2000));
insert into main values(7, PADSTRING('aaaa',2000));
insert into main values(8, PADSTRING('aaaa',2000));
insert into main values(9, PADSTRING('aaaa',2000));
insert into main values(10, PADSTRING('aaaa',2000));
insert into main values(12, PADSTRING('aaaa',2000));
insert into main values(13, PADSTRING('aaaa',2000));

create table main2(x int not null primary key, y varchar(2000));
insert into main2 values(1, PADSTRING('aaaa',2000));
insert into main2 values(2, PADSTRING('aaaa',2000));
insert into main2 values(3, PADSTRING('aaaa',2000));
insert into main2 values(4, PADSTRING('aaaa',2000));
insert into main2 values(5, PADSTRING('aaaa',2000));
insert into main2 values(6, PADSTRING('aaaa',2000));
insert into main2 values(7, PADSTRING('aaaa',2000));
insert into main2 values(8, PADSTRING('aaaa',2000));
insert into main2 values(9, PADSTRING('aaaa',2000));
insert into main2 values(10, PADSTRING('aaaa',2000));
insert into main2 values(12, PADSTRING('aaaa',2000));
insert into main2 values(13, PADSTRING('aaaa',2000));
disconnect;
