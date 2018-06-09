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

-- Primary key constraint definitions from TPC-C specification, version 5.7.
-- Section 1.3 Table Layout
;
ALTER TABLE WAREHOUSE ADD CONSTRAINT
    WAREHOUSE_PK PRIMARY KEY (W_ID);

ALTER TABLE DISTRICT ADD CONSTRAINT
    DISTRICT_PK PRIMARY KEY (D_W_ID, D_ID);

ALTER TABLE CUSTOMER ADD CONSTRAINT
    CUSTOMER_PK PRIMARY KEY(C_W_ID, C_D_ID, C_ID);

ALTER TABLE ITEM ADD CONSTRAINT
    ITEM_PK PRIMARY KEY (I_ID);

ALTER TABLE STOCK ADD CONSTRAINT
    STOCK_PK PRIMARY KEY (S_W_ID, S_I_ID);

ALTER TABLE ORDERS ADD CONSTRAINT
    ORDERS_PK PRIMARY KEY(O_W_ID, O_D_ID, O_ID);

ALTER TABLE NEWORDERS ADD CONSTRAINT
    NEWORDERS_PK PRIMARY KEY(NO_W_ID, NO_D_ID, NO_O_ID);

ALTER TABLE ORDERLINE ADD CONSTRAINT
    ORDERLINE_PK PRIMARY KEY(OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER);
    