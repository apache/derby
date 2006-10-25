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

-- note: weird is spelled wrong here.  So you too don't spend 15 minutes figuring that out.
values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wierdlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),8);

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('logDevice', null);
call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('logDevice', 'foobar');

values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wierdlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),8);

call SYSCS_UTIL.SYSCS_BACKUP_DATABASE('ext/mybackup');

disconnect;
connect 'wombat;shutdown=true';

connect 'wombat';

call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('logDevice', 'foobar');

values SUBSTR(SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice'), LOCATE('wierdlog',SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('logDevice')),8);

