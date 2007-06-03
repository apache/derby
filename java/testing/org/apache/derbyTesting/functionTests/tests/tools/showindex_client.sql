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
-- This test will cover SHOW INDEXES. The test is duplicated in 
-- showindex_embed because client driver currently (10.3) does not support 
-- boolean. 
--
-- NOTE: If this test is modified, showindex_embed.sql should probably also 
-- be modified.

-- create some tables and indexes
CREATE TABLE t1 (i int, d DECIMAL(5,2), test VARCHAR(20));
CREATE TABLE t2 (pk int primary key, v VARCHAR(20));
CREATE INDEX idx1 ON APP.t1 (test ASC);
CREATE INDEX idx2 ON APP.t2 (v);

-- show all the indexes in the schema 
SHOW INDEXES IN APP;
-- show only indexes in table t1
SHOW INDEXES FROM APP.t1;
