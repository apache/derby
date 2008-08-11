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
-- This script demonstrates how to use VTIs to access data in foreign
-- RDBMSes.
--
-- Several of the function calls in this script assume that MySQL
-- is running on your machine, loaded with MySQL's sample "world" database.
-- You will need to change the hard-coded connection URL which occurs
-- throughout this script and which is used to connect to the "world"
-- database:
--
--          jdbc:mysql://localhost/world?user=root&password=mysql-passwd
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
drop procedure registerQueryRowVTIs;

drop procedure closeConnection;

drop procedure createSubscription;
drop procedure dropSubscription;

drop table countryLanguage;

--
-- Recreate procedures
--
create procedure registerQueryRowVTIs
( className varchar( 32672 ), connectionURL varchar( 32672 ) )
language java
parameter style java
modifies sql data
external name 'org.apache.derbyDemo.vtis.core.QueryVTIHelper.registerQueryRowVTIs'
;
create procedure closeConnection( connectionURL varchar( 32672 ) )
language java
parameter style java
modifies sql data
external name 'org.apache.derbyDemo.vtis.core.QueryVTIHelper.closeConnection'
;
create procedure createSubscription
( subscriptionClassName varchar( 32672 ), connectionURL varchar( 32672 ) )
language java
parameter style java
modifies sql data
external name 'org.apache.derbyDemo.vtis.snapshot.Subscription.createSubscription'
;
create procedure dropSubscription
( subscriptionClassName varchar( 32672 ) )
language java
parameter style java
modifies sql data
external name 'org.apache.derbyDemo.vtis.snapshot.Subscription.dropSubscription'
;

----------------------------------------------------------------------------------------
--
-- Declare the table functions.
--
----------------------------------------------------------------------------------------

--
-- Register the table functions in the VTIs class
--
call registerQueryRowVTIs( 'org.apache.derbyDemo.vtis.example.VTIs', 'jdbc:mysql://localhost/world?user=root&password=mysql-passwd' );

----------------------------------------------------------------------------------------
--
-- External Database VTIs
--
----------------------------------------------------------------------------------------

select s.*
from table( "countryLanguage"( 'jdbc:mysql://localhost/world?user=root&password=mysql-passwd' ) ) s
where "CountryCode" between 'E' and 'F'
order by "CountryCode"
;

create table countryLanguage
as select s.*
from table( "countryLanguage"( 'jdbc:mysql://localhost/world?user=root&password=mysql-passwd' ) ) s
with no data
;

insert into countryLanguage
select s.*
from table( "countryLanguage"( 'jdbc:mysql://localhost/world?user=root&password=mysql-passwd' ) ) s
;

select * from countryLanguage
where "Percentage" > 75.0
and "CountryCode" between 'E' and 'F'
order by "CountryCode"
;

--
-- Don't forget to clean up.
--
call closeConnection( 'jdbc:mysql://localhost/world?user=root&password=mysql-passwd' );

----------------------------------------------------------------------------------------
--
-- Parameterized Subscription to Foreign Data
--
----------------------------------------------------------------------------------------

call dropSubscription( 'org.apache.derbyDemo.vtis.example.WorldDBSnapshot' );

call createSubscription
(
  'org.apache.derbyDemo.vtis.example.WorldDBSnapshot',
  'jdbc:mysql://localhost/world?user=root&password=mysql-passwd'
);

-- now tear off a parameterized subscription:
--
-- all data related to cities with more than 9M people
call refreshWorldDB
(
  'org.apache.derbyDemo.vtis.example.WorldDBSnapshot',
  'jdbc:mysql://localhost/world?user=root&password=mysql-passwd',
  '9000000',  -- populationMin
  '30000000'  -- populationMax
);

--inspect the tear-off
select * from "City";
select * from "Country";
select * from "CountryLanguage";

-- now recalculate the subscription
--
-- all data related to cities in a narrower population range: 9-10M people
call refreshWorldDB
(
  'org.apache.derbyDemo.vtis.example.WorldDBSnapshot',
  'jdbc:mysql://localhost/world?user=root&password=mysql-passwd',
  '9000000',  -- populationMin
  '10000000'  -- populationMax
);

--inspect the tear-off
select * from "City";
select * from "Country";
select * from "CountryLanguage";
