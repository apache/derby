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
-- this test is for keyword case insensitivity
--

-- Try some of the keywords with mixed case. Don't do all of the keywords, as
-- that would be overkill (either that, or I'm too lazy).

cReAtE tAbLe T (x InT);

CrEaTe TaBlE s (X iNt);

iNsErT iNtO t VaLuEs (1);

InSeRt InTo S vAlUeS (2);

sElEcT * fRoM t;

SeLeCt * FrOm s;

drop table s;

drop table t;
