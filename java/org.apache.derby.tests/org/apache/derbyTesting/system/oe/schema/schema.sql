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

-- Table definitions from TPC-C specification, version 5.7.
-- Section 1.3 Table Layout
--
-- Constraints are defined in a separate SQL scripts to
-- allow intial data load with or without contstraints.
;

CREATE TABLE WAREHOUSE (
 W_ID       SMALLINT       NOT NULL,

 W_NAME     VARCHAR(10)    NOT NULL,
 W_STREET_1 VARCHAR(20)    NOT NULL,
 W_STREET_2 VARCHAR(20)    NOT NULL,
 W_CITY     VARCHAR(20)    NOT NULL,
 W_STATE    CHAR(2)        NOT NULL,
 W_ZIP      CHAR(9)        NOT NULL,
 W_TAX      DECIMAL (4,4)  NOT NULL,
 W_YTD      DECIMAL (12,2) NOT NULL
);

CREATE TABLE DISTRICT (
 D_ID        SMALLINT       NOT NULL,
 D_W_ID      SMALLINT       NOT NULL,

 D_NAME      VARCHAR(10)    NOT NULL,
 D_STREET_1  VARCHAR(20)    NOT NULL,
 D_STREET_2  VARCHAR(20)    NOT NULL,
 D_CITY      VARCHAR(20)    NOT NULL,
 D_STATE     CHAR(2)        NOT NULL,
 D_ZIP       CHAR(9)        NOT NULL,
 D_TAX       DECIMAL (4,4)  NOT NULL,
 D_YTD       DECIMAL (12,2) NOT NULL,
 D_NEXT_O_ID INTEGER        NOT NULL
 );

CREATE TABLE CUSTOMER (
 C_ID           INTEGER       NOT NULL,
 C_D_ID         SMALLINT      NOT NULL,
 C_W_ID         SMALLINT      NOT NULL,

 C_FIRST        VARCHAR(16)   NOT NULL,
 C_MIDDLE       CHAR(2)       NOT NULL,
 C_LAST         VARCHAR(16)   NOT NULL,
 C_STREET_1     VARCHAR(20)   NOT NULL,
 C_STREET_2     VARCHAR(20)   NOT NULL,
 C_CITY         VARCHAR(20)   NOT NULL,
 C_STATE        CHAR(2)       NOT NULL,
 C_ZIP          CHAR(9)       NOT NULL,
 C_PHONE        CHAR(16)      NOT NULL,
 C_SINCE        TIMESTAMP     NOT NULL,
 C_CREDIT       CHAR(2)       NOT NULL,
 C_CREDIT_LIM   DECIMAL(12,2) NOT NULL,
 C_DISCOUNT     DECIMAL(4,4)  NOT NULL,
 C_BALANCE      DECIMAL(12,2) NOT NULL,
 C_YTD_PAYMENT  DECIMAL(12,2) NOT NULL,
 C_PAYMENT_CNT  INTEGER       NOT NULL,
 C_DELIVERY_CNT INTEGER       NOT NULL,
 C_DATA         VARCHAR(500)  NOT NULL
);

CREATE TABLE HISTORY (
 H_C_ID   INTEGER      NOT NULL,
 H_C_D_ID SMALLINT     NOT NULL,
 H_C_W_ID SMALLINT     NOT NULL,
 H_D_ID   SMALLINT     NOT NULL,
 H_W_ID   SMALLINT     NOT NULL ,
 H_DATE   TIMESTAMP    NOT NULL,
 H_AMOUNT DECIMAL(6,2) NOT NULL,
 H_DATA   VARCHAR(24)  NOT NULL
);

CREATE TABLE NEWORDERS (
 NO_O_ID  INTEGER  NOT NULL,
 NO_D_ID  SMALLINT NOT NULL,
 NO_W_ID  SMALLINT NOT NULL
);

CREATE TABLE ORDERS (
 O_ID         INTEGER NOT NULL,
 O_D_ID       SMALLINT NOT NULL,
 O_W_ID       SMALLINT NOT NULL,
 O_C_ID       INTEGER NOT NULL,

 O_ENTRY_D    TIMESTAMP NOT NULL,
 O_CARRIER_ID SMALLINT,
 O_OL_CNT     SMALLINT NOT NULL,
 O_ALL_LOCAL  SMALLINT NOT NULL
);

CREATE TABLE ORDERLINE (
 OL_O_ID        INTEGER      NOT NULL,
 OL_D_ID        SMALLINT     NOT NULL,
 OL_W_ID        SMALLINT     NOT NULL,
 OL_NUMBER      SMALLINT     NOT NULL,

 OL_I_ID        INTEGER      NOT NULL,
 OL_SUPPLY_W_ID SMALLINT     NOT NULL,
 OL_DELIVERY_D  TIMESTAMP,
 OL_QUANTITY    SMALLINT     NOT NULL,
 OL_AMOUNT      DECIMAL(6,2) NOT NULL,
 OL_DIST_INFO   CHAR(24)     NOT NULL
);

CREATE TABLE ITEM (
 I_ID     INTEGER      NOT NULL,

 I_IM_ID  INTEGER      NOT NULL,
 I_NAME   VARCHAR(24)  NOT NULL,
 I_PRICE  DECIMAL(5,2) NOT NULL,
 I_DATA   VARCHAR(50)  NOT NULL
);

CREATE TABLE STOCK (
 S_I_ID       INTEGER     NOT NULL,
 S_W_ID       SMALLINT    NOT NULL,
 S_QUANTITY   INTEGER     NOT NULL,

 S_DIST_01    CHAR(24)    NOT NULL,
 S_DIST_02    CHAR(24)    NOT NULL,
 S_DIST_03    CHAR(24)    NOT NULL,
 S_DIST_04    CHAR(24)    NOT NULL,
 S_DIST_05    CHAR(24)    NOT NULL,
 S_DIST_06    CHAR(24)    NOT NULL,
 S_DIST_07    CHAR(24)    NOT NULL,
 S_DIST_08    CHAR(24)    NOT NULL,
 S_DIST_09    CHAR(24)    NOT NULL,
 S_DIST_10    CHAR(24)    NOT NULL,

 S_YTD        DECIMAL(8)  NOT NULL,
 S_ORDER_CNT  INTEGER     NOT NULL,
 S_REMOTE_CNT INTEGER     NOT NULL,
 S_DATA       VARCHAR(50) NOT NULL
);