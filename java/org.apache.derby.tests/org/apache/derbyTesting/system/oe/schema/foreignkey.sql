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

-- Foreign key constraint definitions from TPC-C specification, version 5.7.
-- Section 1.3 Table Layout
;

ALTER TABLE DISTRICT ADD CONSTRAINT
    D_W_FK FOREIGN KEY (D_W_ID) REFERENCES WAREHOUSE;

ALTER TABLE CUSTOMER ADD CONSTRAINT
    C_D_FK_DISTRICT FOREIGN KEY (C_W_ID,C_D_ID) REFERENCES DISTRICT;

ALTER TABLE STOCK ADD CONSTRAINT
    S_W_FK FOREIGN KEY (S_W_ID) REFERENCES WAREHOUSE;
ALTER TABLE STOCK ADD CONSTRAINT
    S_I_FK FOREIGN KEY (S_I_ID) REFERENCES ITEM;

ALTER TABLE HISTORY ADD CONSTRAINT
    H_C_FK FOREIGN KEY (H_C_W_ID, H_C_D_ID, H_C_ID) REFERENCES CUSTOMER;
ALTER TABLE HISTORY ADD CONSTRAINT
    H_D_FK FOREIGN KEY (H_W_ID, H_D_ID) REFERENCES DISTRICT;

ALTER TABLE ORDERS ADD CONSTRAINT
    O_C_FK FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES CUSTOMER;

ALTER TABLE NEWORDERS ADD CONSTRAINT
    NO_O_FK FOREIGN KEY (NO_W_ID, NO_D_ID, NO_O_ID) REFERENCES ORDERS;

ALTER TABLE ORDERLINE ADD CONSTRAINT
    OL_O_FK FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES ORDERS;
ALTER TABLE ORDERLINE ADD CONSTRAINT
    OL_S_FK FOREIGN KEY (OL_SUPPLY_W_ID, OL_I_ID) REFERENCES STOCK;


