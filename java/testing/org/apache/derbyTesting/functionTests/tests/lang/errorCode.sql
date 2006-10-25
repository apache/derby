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
-- this test shows the error code functionality
--

-- specify an invalid driver
driver 'java.lang.Integer';

-- now a valid driver
driver 'org.apache.derby.jdbc.EmbeddedDriver';

-- specify an invalid database
connect 'asdfasdf';

-- now a valid database, but no create
connect 'jdbc:derby:wombat';

-- now a valid database
connect 'jdbc:derby:wombat;create=true';


-- create the table
create table t(i int, s smallint);

-- populate the table
insert into t values (1,2);
insert into t values (null,2);

-- parser error
-- bug 5701
create table t(i nt, s smallint);

-- non-boolean where clause
select * from t where i;

-- invalid correlation name for "*"
select asdf.* from t; 

-- execution time error
select i/0 from t;

-- test ErrorMessages VTI
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = '07000';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = '22012';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = '42X74';

-- test ErrorMessages VTI for severe errors
-- session_severity
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = '04501';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = '28502';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XJ004';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XJ028';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XJ040';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XJ041';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XJ049';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XJ081';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XJ05B';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XXXXX';

-- database_severity
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM01';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM02';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM05';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM06';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM07';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM08';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0G';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0H';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0I';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0J';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0K';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0L';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0M';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0N';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0P';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0Q';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0R';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0S';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0T';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0X';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0Y';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XBM0Z';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XCW00';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA0';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA1';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA2';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA3';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA4';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA5';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA6';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA7';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLA8';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAA';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAB';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAC';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAD';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAE';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAF';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAH';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAI';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAJ';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAK';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAL';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAM';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAN';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAO';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAP';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAQ';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAR';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAS';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSLAT';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB0';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB1';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB2';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB3';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB4';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB5';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB6';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB7';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB8';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDB9';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDBA';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDG0';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDG1';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDG2';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDG3';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDG5';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDG6';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDG7';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSDG8';

-- system_severity
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSTB0';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSTB2';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSTB3';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSTB5';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XSTB6';
select * from new org.apache.derby.diag.ErrorMessages() c where sql_state = 'XJ015';

-- cleanup
drop table t;
