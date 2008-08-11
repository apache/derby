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

----------------------------------------------------------------------------------------
--
-- This script demonstrates how to declare and use several sample
-- table functions.
--
-- Several of the function calls in this script assume that your
-- Derby code client can be found at:
--
--          /opt/DerbyTrunk
--
--  If that is not the case, then you will need to adjust this
--  script accordingly.
--
----------------------------------------------------------------------------------------

connect 'jdbc:derby:vtitest;create=true';

----------------------------------------------------------------------------------------
--
-- Drop and recreate the database procedures and tables needed
-- by this demonstration script.
--
----------------------------------------------------------------------------------------

--
-- Drop procedures and tables
--
drop procedure registerXMLRowVTIs;

--
-- Drop miscellaneous table functions
--
drop function svnLogReader;
drop function propertyFileVTI;


--
-- Recreate procedures
--
create procedure registerXMLRowVTIs( className varchar( 32672 ) )
language java
parameter style java
modifies sql data
external name 'org.apache.derbyDemo.vtis.core.XmlVTI.registerXMLRowVTIs'
;

----------------------------------------------------------------------------------------
--
-- Declare the table functions.
--
----------------------------------------------------------------------------------------

--
-- Register the table functions in the VTIs class
--
call registerXMLRowVTIs( 'org.apache.derbyDemo.vtis.example.VTIs' );

--
-- Register a table function which reads the output of an 'svn log' command
--
create function svnLogReader( logFileName varchar( 32672 ) )
returns TABLE
  (
     XID varchar( 15 ),
     committer    varchar( 20 ),
     commit_time  timestamp,
     line_count   varchar( 10 ),
     description  varchar( 32672 )
  )
language java
parameter style DERBY_JDBC_RESULT_SET
no sql
external name 'org.apache.derbyDemo.vtis.example.SubversionLogVTI.subversionLogVTI'
;

--
-- Register a table function to read a Derby message file
--
create function propertyFileVTI( fileName varchar( 32672 ) )
returns TABLE
  (
     messageID  varchar( 10 ),
     messageText varchar( 1000 )
  )
language java
parameter style DERBY_JDBC_RESULT_SET
no sql
external name 'org.apache.derbyDemo.vtis.example.PropertyFileVTI.propertyFileVTI'
;

----------------------------------------------------------------------------------------
--
-- Read a log file dumped as a flat file
--
----------------------------------------------------------------------------------------

-- how active were the committers in 2006?
select committer, count(*) as commits
from table( svnLogReader( '/opt/DerbyTrunk/java/demo/vtis/data/svn_log.txt' ) ) s
where commit_time between timestamp( '2006-01-01 00:00:00' ) and timestamp( '2007-01-01 00:00:00' )
group by committer
;

----------------------------------------------------------------------------------------
--
-- Read a property file of Derby messages
--
----------------------------------------------------------------------------------------

-- find the messages which have not been translated into french
select *
from table( propertyFileVTI( '/opt/DerbyTrunk/java/engine/org/apache/derby/loc/messages_en.properties' ) ) m_english
where m_english.messageID not in
(
    select m_french.messageID
    from table( propertyFileVTI( '/opt/DerbyTrunk/java/engine/org/apache/derby/loc/messages_fr.properties' ) ) m_french
);


----------------------------------------------------------------------------------------
--
-- XML VTIs
--
----------------------------------------------------------------------------------------

--
-- Read from the XML log file produced by an Apache web server
--

-- this vti treats the oddly formatted accessDate and fileSize fields as varchars
select s.*
from table( "apacheVanillaLogFile"( 'file:///opt/DerbyTrunk/java/demo/vtis/data/ApacheServerLog.xml' ) ) s
;

-- this vti treats accessDate as a timestamp and fileSize as an int
select s.*
from table( "apacheNaturalLogFile"( 'file:///opt/DerbyTrunk/java/demo/vtis/data/ApacheServerLog.xml' ) ) s
;

-- look for relevant status codes
select s.*
from table( "apacheNaturalLogFile"( 'file:///opt/DerbyTrunk/java/demo/vtis/data/ApacheServerLog.xml' ) ) s
where s."statusCode" = 206
;

-- look for relevant IP addresses
select s.*
from table( "apacheNaturalLogFile"( 'file:///opt/DerbyTrunk/java/demo/vtis/data/ApacheServerLog.xml' ) ) s
where "IP" like '208%'
;

-- look for log records in a time range
select s.*
from table( "apacheNaturalLogFile"( 'file:///opt/DerbyTrunk/java/demo/vtis/data/ApacheServerLog.xml' ) ) s
where "accessDate" between timestamp( '2002-07-01 08:40:56.0' ) and timestamp( '2002-07-01 08:42:56.0' )
;

--
-- Read from the XML log file produced by a JIRA report
--

-- report on Derby JIRAs
select s.*
from table( "apacheVanillaJiraReport"( 'file:///opt/DerbyTrunk/java/demo/vtis/data/DerbyJiraReport.xml' ) ) s
where s."key" between 'DERBY-2800' and 'DERBY-2950'
;

-- treat keys as ints and sort Derby JIRAs by key
select s.*
from table( "apacheNaturalJiraReport"( 'file:///opt/DerbyTrunk/java/demo/vtis/data/DerbyJiraReport.xml' ) ) s
where s."key" between 2800 and 2950
order by "key"
;

-- eliminate uninteresting Derby JIRAs
select s.*
from table( "apacheNaturalJiraReport"( 'file:///opt/DerbyTrunk/java/demo/vtis/data/DerbyJiraReport.xml' ) ) s
where "type" != 'Sub-task'
order by "key"
;
