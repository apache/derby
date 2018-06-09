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

-- Routines related to the delivery transaction
-- Section 2.7
--
-- OrderEntry (OE) uses the database as the queueing
-- mechanism and the "results file"
-- described by the TPC-C specification in section 2.7.
-- This exceeds the requirements of the specification
-- (no ACID required) but since the focus of OE is to
-- test Derby, it seems additional work against the
-- database is a good thing. It also provides a consistent
-- and standard way to access the data, including from clients.
;

-- Requests are queued in this table.
-- DR_STATE Q-queued, Pin progress, C-complete, E-error
;
CREATE TABLE DELIVERY_REQUEST
(
   DR_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
   DR_W_ID SMALLINT NOT NULL,
   DR_CARRIER_ID SMALLINT NOT NULL,
   DR_QUEUED TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
   DR_COMPLETED TIMESTAMP,
   DR_STATE  CHAR(1)
     CONSTRAINT DR_STATE_CHECK CHECK (DR_STATE IN ('Q', 'I', 'C', 'E'))
);

-- Index to allow lookup of the oldest queued order.
;
CREATE INDEX DR_LOOKUP ON DELIVERY_REQUEST(DR_STATE, DR_QUEUED);

-- Record of orders delivered.
-- A NULL DL_O_ID means that no orders for that district could be delivered.
;
CREATE TABLE DELIVERY_ORDERS
(
   DO_DR_ID INTEGER NOT NULL CONSTRAINT DO_FK REFERENCES DELIVERY_REQUEST(DR_ID),
   DO_D_ID SMALLINT NOT NULL,
   DO_O_ID INTEGER
);

    