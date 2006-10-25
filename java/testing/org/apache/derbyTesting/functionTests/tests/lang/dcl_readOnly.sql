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
-- common tests for read-only jarred database

select * from EMC.CONTACTS;
select e_mail, "emcAddOn".VALIDCONTACT(e_mail) from EMC.CONTACTS;
insert into EMC.CONTACTS values(3, 'no@is_read_only.gov', NULL);
CALL EMC.ADDCONTACT(3, 'really@is_read_only.gov');

-- same set as dcl.sql for reading resources
-- VALUES EMC.GETARTICLE('graduate.txt');
-- VALUES EMC.GETARTICLE('/article/release.txt');
-- VALUES EMC.GETARTICLE('/article/fred.txt');
-- VALUES EMC.GETARTICLE('barney.txt');
-- VALUES EMC.GETARTICLE('emc.class');
-- VALUES EMC.GETARTICLE('/org/apache/derbyTesting/databaseclassloader/emc.class');

-- signed
VALUES EMC.GETSIGNERS('org.apache.derbyTesting.databaseclassloader.emc');
-- not signed
VALUES EMC.GETSIGNERS('org.apache.derbyTesting.databaseclassloader.addon.vendor.util');

-- ensure that a read-only database automatically gets table locking
autocommit off;
select * from EMC.CONTACTS WITH RR;
select TYPE, MODE, TABLENAME from syscs_diag.lock_table ORDER BY 1,2,3;