-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
;

-- Indexes for the oe transactions when foreign key
-- constraints are not created or the creation of
-- foreign key constraints do not automatically create
-- backing indexes (e.g. not Derby).

-- Required to quickly find the most recent order for a customer
CREATE INDEX ORDERS_CUSTOMER_STATUS ON ORDERS(O_W_ID, O_D_ID, O_C_ID);
