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
-- assumes the connection connOne is set up already
-- and that connThree, connFour failed to be setup correctly (bad URLs)

-- expect connOne to be active
show connections;
connect 'jdbc:derby:lemming;create=true' as connTwo;
set connection connOne;
values 1;
set connection connTwo;
values 1;
-- connThree doesn't exist, it failed at boot time
set connection connThree;
-- connFour doesn't exist, it failed at boot time
set connection connFour;
-- connTwo is still active
show connections;
-- no such connection to disconnect
disconnect noName;
disconnect connOne;
-- connOne no longer exists
set connection connOne;

disconnect current;

-- see no more connections to use
show connections;
