-- Test prints query plans for SpecjAppServer2001 benchmark queries.
-- Specifically this test checks for plans generated for tables with no rows to 
-- ensure that 
-- Index scan must be chosen over table scan for searched update/delete even when there are 
-- 0 rows in table
-- Also see Beetle task id : 5006


-- Test does the following
-- 1. First creates the necessary schema (tables, indexes)
-- 2. Executes and prints the query plan for all the queries in specjappserver2001 benchmark
--    Makes sure that the insert stmts are in the end to ensure that there are no
--    rows in the tables
-- 3. Drops the tables

-- Let's start with something light...

-- by default, holdability of ResultSet objects created using this Connection object is true. Following will set it to false for this connection.
NoHoldForConnection;

drop table t1;
create table t1(c1 int, c2 int);
-- create non-unique index first, then unique, to make sure non-unique is looked at first, and so
-- in case of tie, the first (nonunique) would be chosen by the cost-based optimizer.  But we need
-- to make sure such tie never happens, and unique index is always chosen (if the only difference
-- between the two is "uniqueness").  Well the beetle bug 5006 itself is about, first of all,
-- table scan should never be chosen, no matter the index is covering ot not.
create index i11 on t1(c1);
create unique index i12 on t1(c1);
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 3500;

-- In the following statement, optimizer thinks we have a covering index (only referenced column is
-- c1), make sure we are using unique index (I12), not table scan, not I11.
delete from t1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- In the following statement, optimizer thinks we have a non-covering index (referenced columns are
-- c1 and c2), make sure we are still using unique index (I12), not table scan, not I11.
update t1 set c2 = 1 where c1 = 1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- The following select should use TABLE SCAN, no predicate at all, and index not covering, no reason
-- to use index!!!
select * from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- This one should use index, since it is covered, really doesn't matter which one, since no predicate,
-- It will choose the first one -- I11.
select c1 from t1;
values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

drop table t1;
-- end of something light...

DROP TABLE C_customer;
CREATE TABLE C_customer
(
	c_id		integer not null,
	c_first		char(16),
	c_last		char(16),
	c_street1	char(20),	
	c_street2	char(20),	
	c_city		char(20),	
	c_state		char(2),	
	c_country	char(10),	
	c_zip		char(9),	
	c_phone		char(16),
	c_contact	char(25),	
	c_since		date,
	c_balance	numeric(9,2),
	c_credit	char(2),
	c_credit_limit	numeric(9,2),
	c_ytd_payment	numeric(9,2)
);

CREATE UNIQUE INDEX C_c_idx ON C_customer (c_id);

DROP TABLE C_supplier;
CREATE TABLE C_supplier
(
	supp_id			integer not null,
	supp_name		char(16),
	supp_street1	char(20),	
	supp_street2	char(20),	
	supp_city		char(20),	
	supp_state		char(2),	
	supp_country	char(10),	
	supp_zip		char(9),	
	supp_phone		char(16),
	supp_contact	char(25)
);

CREATE UNIQUE INDEX C_supp_idx ON C_supplier (supp_id);

DROP TABLE C_site;
CREATE TABLE C_site
(
	site_id			integer not null,
	site_name		char(16),
	site_street1	char(20),	
	site_street2	char(20),	
	site_city		char(20),	
	site_state		char(2),	
	site_country	char(10),	
	site_zip		char(9)
);

CREATE UNIQUE INDEX C_site_idx ON C_site (site_id);

DROP TABLE C_parts;
CREATE TABLE C_parts
(
	p_id			char(15) not null,
	p_name			char(10),
	p_desc			varchar(100),
	p_rev			char(6),
	p_unit			char(10),
	p_cost			numeric(9,2),
	p_price			numeric(9,2),
	p_planner		integer,
	p_type			integer,
	p_ind			integer,
        p_lomark                integer,
        p_himark                integer
);

CREATE UNIQUE INDEX C_p_idx ON C_parts (p_id);

DROP TABLE C_rule;
CREATE TABLE C_rule
(
	r_id		varchar(20) not null,
	r_text		long varchar
);

CREATE UNIQUE INDEX C_r_idx on C_rule (r_id);

DROP TABLE C_discount;
CREATE TABLE C_discount
(	
	d_id		varchar(64) not null,
	d_percent	integer
);



CREATE UNIQUE INDEX C_d_idx on C_discount (d_id);
DROP TABLE M_parts;
CREATE TABLE M_parts
(
	p_id			char(15) not null,
	p_name			char(10),
	p_desc			varchar(100),
	p_rev			char(6),
	p_planner		integer,
	p_type			integer,
	p_ind			integer,
	p_lomark		integer,
	p_himark		integer
);

CREATE UNIQUE INDEX M_parts_idx ON M_parts (p_id);

DROP TABLE M_bom;
CREATE TABLE M_bom
(
	b_comp_id		char(15) not null,
	b_assembly_id		char(15) not null,
	b_line_no		integer,
	b_qty			integer,
	b_ops			integer,	
	b_eng_change		char(10),	
	b_ops_desc		varchar(100)
);

CREATE UNIQUE INDEX M_bom_idx ON M_bom (b_assembly_id, b_comp_id, b_line_no);

DROP TABLE M_workorder;
CREATE TABLE M_workorder
(
	wo_number		integer not null,
	wo_o_id			integer,
	wo_ol_id		integer,
	wo_status		integer,
	wo_assembly_id	char(15),
	wo_orig_qty		integer,
	wo_comp_qty		integer,
	wo_due_date		date,
	wo_start_date		timestamp
);

CREATE UNIQUE INDEX M_wo_idx ON M_workorder (wo_number);

DROP TABLE M_largeorder;
CREATE TABLE M_largeorder
(
	lo_id			integer not null,
	lo_o_id			integer,
	lo_ol_id		integer,
	lo_assembly_id	char(15),
	lo_qty			integer,
	lo_due_date		date
);

CREATE UNIQUE INDEX M_lo_idx ON M_largeorder (lo_id);
CREATE UNIQUE INDEX M_OL_O_idx ON M_largeorder (lo_o_id, lo_ol_id);

DROP TABLE M_inventory;
CREATE TABLE M_inventory
(
	in_p_id			char(15) not null,
	in_qty			integer,
	in_ordered		integer,
	in_location		char(20),	
	in_acc_code		integer,
	in_act_date		date
);

CREATE UNIQUE INDEX M_inv_idx ON M_inventory (in_p_id);
DROP TABLE O_customer;
CREATE TABLE O_customer
(
	c_id		integer not null,
	c_first		char(16),
	c_last		char(16),
	c_street1	char(20),	
	c_street2	char(20),	
	c_city		char(20),	
	c_state		char(2),	
	c_country	char(10),	
	c_zip		char(9),	
	c_phone		char(16),
	c_contact	char(25),
	c_since		date
);

CREATE UNIQUE INDEX O_c_idx ON O_customer (c_id);

DROP TABLE O_orders;
CREATE TABLE O_orders
(
	o_id		integer not null,
	o_c_id		integer,
	o_ol_cnt	integer,
	o_discount	numeric(4,2),
	o_total		numeric(9,2),
	o_status	integer,
	o_entry_date	timestamp,
	o_ship_date	date
);

CREATE UNIQUE INDEX O_ords_idx ON O_orders (o_id);

CREATE INDEX O_oc_idx ON O_orders (o_c_id);

DROP TABLE O_orderline;
CREATE TABLE O_orderline
(
	ol_id		integer not null,
	ol_o_id		integer not null,
	ol_i_id		char(15),
	ol_qty		integer,
	ol_status	integer,
	ol_ship_date	date
);

CREATE UNIQUE INDEX O_ordl_idx ON O_orderline (ol_o_id, ol_id);
CREATE INDEX O_ordl_idx2 ON O_orderline (ol_o_id, ol_i_id);
CREATE INDEX O_ordl_idx3 ON O_orderline (ol_o_id);

DROP TABLE O_item;
CREATE TABLE O_item
(
	i_id			char(15) not null,
	i_name			char(20),
	i_desc			varchar(100),
	i_price			numeric(9,2),
	i_discount		numeric(6,4)
);

CREATE UNIQUE INDEX O_i_idx ON O_item (i_id);

DROP TABLE S_component;
CREATE TABLE S_component
(
	comp_id			char(15) not null,
	comp_name		char(10),
	comp_desc		varchar(100),
	comp_unit		char(10),
	comp_cost		numeric(9,2),
	qty_on_order		integer,
	qty_demanded		integer,
	lead_time		integer,
	container_size		integer
);

CREATE UNIQUE INDEX S_comp_idx ON S_component (comp_id);

DROP TABLE S_supp_component;
CREATE TABLE S_supp_component
(
	sc_p_id			char(15) not null,
	sc_supp_id		integer not null,
	sc_price		numeric(9,2),
	sc_qty			integer,
	sc_discount		numeric(6,4),
	sc_del_date		integer
);

CREATE UNIQUE INDEX S_sc_idx ON S_supp_component (sc_p_id, sc_supp_id);

DROP TABLE S_supplier;
CREATE TABLE S_supplier
(
	supp_id			integer not null,
	supp_name		char(16),
	supp_street1	char(20),	
	supp_street2	char(20),	
	supp_city		char(20),	
	supp_state		char(2),	
	supp_country	char(10),	
	supp_zip		char(9),	
	supp_phone		char(16),
	supp_contact	char(25)
);

CREATE UNIQUE INDEX S_supp_idx ON S_supplier (supp_id);

DROP TABLE S_site;
CREATE TABLE S_site
(
	site_id			integer not null,
	site_name		char(16),
	site_street1	char(20),	
	site_street2	char(20),	
	site_city		char(20),	
	site_state		char(2),	
	site_country	char(10),	
	site_zip		char(9)
);

CREATE UNIQUE INDEX S_site_idx ON S_site (site_id);

DROP TABLE S_purchase_order;
CREATE TABLE S_purchase_order
(
	po_number		integer not null,
	po_supp_id		integer,
	po_site_id		integer
);

CREATE UNIQUE INDEX S_po_idx ON S_purchase_order (po_number);

DROP TABLE S_purchase_orderline;
CREATE TABLE S_purchase_orderline
(
	pol_number		integer not null,
	pol_po_id		integer not null,
	pol_p_id		char(15),
	pol_qty			integer,
	pol_balance		numeric(9,2),
	pol_deldate		date,
	pol_message		varchar(100)
);

CREATE UNIQUE INDEX S_pol_idx ON S_purchase_orderline (pol_po_id, pol_number);

DROP TABLE U_sequences;
CREATE TABLE U_sequences
(
	s_id		varchar(50) not null,
	s_nextnum	integer,
	s_blocksize	integer
);

CREATE UNIQUE INDEX U_s_idx ON U_sequences (s_id);



-- set the runtimestatistics to check the query plans generated
call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1);
maximumdisplaywidth 3500;

 SELECT T1.O_STATUS, T1.O_SHIP_DATE, T1.O_ENTRY_DATE, T1.O_TOTAL, T1.O_DISCOUNT, T1.O_OL_CNT, T1.O_C_ID, T1.O_ID 
 FROM O_ORDERS  T1 WHERE o_c_id = 0 FOR UPDATE ;
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- MAKE SURE THE FOLLOWING USE TABLE SCAN, NO REASON TO USE INDEX AT ALL, NOT USEFUL PREDICATES!!! 
 SELECT COUNT (*) FROM O_orders WHERE o_entry_date >= '01/10/2003' AND o_entry_date <= '01/09/2003' ;
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 
 SELECT T1.WO_NUMBER, T1.WO_O_ID, T1.WO_OL_ID, T1.WO_STATUS, T1.WO_ORIG_QTY, T1.WO_COMP_QTY, T1.WO_ASSEMBLY_ID, 
 T1.WO_DUE_DATE, T1.WO_START_DATE FROM M_WORKORDER  T1 WHERE T1.WO_NUMBER = 1 FOR UPDATE;
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- MAKE SURE THE FOLLOWING USE TABLE SCAN, NO REASON TO USE INDEX AT ALL, NOT USEFUL PREDICATES!!! 
 SELECT T1.LO_ID, T1.LO_O_ID, T1.LO_OL_ID, T1.LO_ASSEMBLY_ID, T1.LO_QTY, T1.LO_DUE_DATE FROM M_LARGEORDER  T1 WHERE 1=1;
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

 UPDATE M_INVENTORY  SET IN_QTY = 1, IN_LOCATION = 'sanfrancisco', IN_ACC_CODE = 1, IN_ACT_DATE = '01/01/2003', IN_ORDERED = 1 WHERE IN_P_ID = 'abcdefghijklm'; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM S_component; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.R_ID, T1.R_TEXT FROM C_RULE  T1 WHERE T1.R_ID = 'abcdefghijlkmijklmnopqrstuvwxyz'; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM C_site; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.IN_P_ID, T1.IN_QTY, T1.IN_LOCATION, T1.IN_ACC_CODE, T1.IN_ACT_DATE, T1.IN_ORDERED FROM M_INVENTORY  T1 WHERE T1.IN_P_ID = 'abcdefghijkl' FOR UPDATE; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.OL_SHIP_DATE, T1.OL_QTY, T1.OL_I_ID, T1.OL_O_ID, T1.OL_ID FROM O_ORDERLINE  T1 WHERE ol_o_id = 1 FOR UPDATE ;
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 DELETE FROM M_LARGEORDER  WHERE LO_ID = 1;
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- MAKE SURE THE FOLLOWING USE TABLE SCAN, NO REASON TO USE INDEX AT ALL, NOT USEFUL PREDICATES!!! 
 SELECT COUNT (*) FROM M_workorder WHERE wo_start_date >= '01/10/2003' AND wo_start_date <= '01/10/2003';
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

 SELECT T1.I_DISCOUNT, T1.I_DESC, T1.I_NAME, T1.I_PRICE, T1.I_ID FROM O_ITEM  T1 WHERE T1.I_ID = 'abcdefghijk'; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.P_ID, T1.P_NAME, T1.P_DESC, T1.P_REV, T1.P_PLANNER, T1.P_TYPE, T1.P_IND, T1.P_LOMARK, T1.P_HIMARK FROM M_PARTS  T1 WHERE T1.P_ID = 'abcdefghijl'; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM S_purchase_orderline; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.S_ID, T1.S_NEXTNUM, T1.S_BLOCKSIZE FROM U_SEQUENCES  T1 WHERE T1.S_ID = 'abcdefghijklmnopqrstuvwxyz' FOR UPDATE; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.SC_P_ID, T1.SC_SUPP_ID, T1.SC_PRICE, T1.SC_QTY, T1.SC_DISCOUNT, T1.SC_DEL_DATE FROM S_SUPP_COMPONENT  T1 WHERE T1.SC_P_ID = 'abcdefgjikl' AND T1.SC_SUPP_ID = 1; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.O_STATUS, T1.O_SHIP_DATE, T1.O_ENTRY_DATE, T1.O_TOTAL, T1.O_DISCOUNT, T1.O_OL_CNT, T1.O_C_ID, T1.O_ID FROM O_ORDERS  T1 WHERE T1.O_ID = 1 FOR UPDATE; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM M_workorder; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM S_purchase_order; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM M_bom; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.PO_NUMBER, T1.PO_SUPP_ID, T1.PO_SITE_ID FROM S_PURCHASE_ORDER  T1 WHERE T1.PO_NUMBER = 1; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM O_orderline; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.D_ID, T1.D_PERCENT FROM C_DISCOUNT  T1 WHERE T1.D_ID = 'abcdefghijklmnopqrstuvwz'; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 UPDATE O_ORDERLINE  SET OL_SHIP_DATE = '2/28/2000', OL_QTY = 10, OL_I_ID = 'abcdefghijkl' WHERE OL_O_ID = 1 AND OL_ID = 1; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.COMP_ID, T1.COMP_NAME, T1.COMP_DESC, T1.COMP_UNIT, T1.COMP_COST, T1.QTY_ON_ORDER, T1.QTY_DEMANDED, T1.LEAD_TIME, T1.CONTAINER_SIZE FROM S_COMPONENT  T1 WHERE T1.COMP_ID = 'abcdefghijk' FOR UPDATE; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM O_customer; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.LO_ID, T1.LO_O_ID, T1.LO_OL_ID, T1.LO_ASSEMBLY_ID, T1.LO_QTY, T1.LO_DUE_DATE FROM M_LARGEORDER  T1 WHERE lo_o_id = 1 AND lo_ol_id = 1; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 UPDATE O_ORDERS  SET O_STATUS = 1, O_SHIP_DATE = '01/01/9004', O_ENTRY_DATE = NULL, O_TOTAL = 1000, O_DISCOUNT =100, O_OL_CNT = 1, O_C_ID = 1 WHERE O_ID = 2; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM C_customer; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM M_inventory; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- MAKE SURE THE FOLLOWING USE TABLE SCAN, NO REASON TO USE INDEX AT ALL, NOT USEFUL PREDICATES!!! 
 SELECT T1.SUPP_ID, T1.SUPP_NAME, T1.SUPP_STREET1, T1.SUPP_STREET2, T1.SUPP_CITY, T1.SUPP_STATE, T1.SUPP_COUNTRY, T1.SUPP_ZIP, T1.SUPP_PHONE, T1.SUPP_CONTACT FROM S_SUPPLIER  T1 WHERE 1=1; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 UPDATE U_SEQUENCES  SET S_NEXTNUM = 1	, S_BLOCKSIZE = 1000 WHERE S_ID = 'abcdefghijklmnopqrstuvwxyz'; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM M_parts ;
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM O_item; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 UPDATE M_WORKORDER  SET WO_O_ID = 11, WO_OL_ID = 11, WO_STATUS = 11, WO_ORIG_QTY = 11, WO_COMP_QTY = 11, WO_ASSEMBLY_ID = 'abcdefghijk', WO_DUE_DATE = '01/01/2000', WO_START_DATE = '01/01/00' WHERE WO_NUMBER = 1; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.POL_NUMBER, T1.POL_PO_ID, T1.POL_P_ID, T1.POL_QTY, T1.POL_BALANCE, T1.POL_DELDATE, T1.POL_MESSAGE FROM S_PURCHASE_ORDERLINE  T1 WHERE T1.POL_NUMBER = 100 AND T1.POL_PO_ID = 200 FOR UPDATE; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.OL_SHIP_DATE, T1.OL_QTY, T1.OL_I_ID, T1.OL_O_ID, T1.OL_ID FROM O_ORDERLINE  T1 WHERE ol_o_id = 100 AND ol_i_id = 'abcdefgh'  FOR UPDATE; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.POL_NUMBER, T1.POL_PO_ID, T1.POL_P_ID, T1.POL_QTY, T1.POL_BALANCE, T1.POL_DELDATE, T1.POL_MESSAGE FROM S_PURCHASE_ORDERLINE  T1 WHERE pol_po_id = 11 FOR UPDATE; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 DELETE FROM O_ORDERS  WHERE O_ID = 1; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM O_orders; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM S_supplier; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.C_ID, T1.C_SINCE, T1.C_BALANCE, T1.C_CREDIT, T1.C_CREDIT_LIMIT, T1.C_YTD_PAYMENT FROM C_CUSTOMER  T1 WHERE T1.C_ID = 1111 FOR UPDATE; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();

-- MAKE SURE THE FOLLOWING DELETE STATEMENT USES UNIQUE INDEX "O_ORDL_IDX", NOT NON-UNIQUE INDEX "O_ORDL_IDX2",
-- EVEN THOUGH WE ARE COMPILING WITH EMPTY TABLE!!! beetle 5006.
 DELETE FROM O_ORDERLINE  WHERE OL_O_ID = 11111 AND OL_ID = 111111; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM C_supplier; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.B_ASSEMBLY_ID, T1.B_COMP_ID, T1.B_LINE_NO, T1.B_QTY, T1.B_ENG_CHANGE, T1.B_OPS, T1.B_OPS_DESC FROM M_BOM  T1 WHERE b_assembly_id = 'specjstuff'; 
 
 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 UPDATE S_COMPONENT  SET COMP_NAME = 'abc', COMP_DESC = 'book', COMP_UNIT = '100', COMP_COST = 1000, QTY_ON_ORDER = 1000, QTY_DEMANDED = 111, LEAD_TIME = 11, CONTAINER_SIZE = 11 WHERE COMP_ID = 'rudyardkipling'; 

 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.PO_NUMBER, T1.PO_SUPP_ID, T1.PO_SITE_ID FROM S_PURCHASE_ORDER  T1 WHERE T1.PO_NUMBER = 100 FOR UPDATE; 

 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 UPDATE S_PURCHASE_ORDERLINE  SET POL_P_ID = 'teacup', POL_QTY = 2, POL_BALANCE = 2, POL_DELDATE = '01/01/2000', POL_MESSAGE = 'tintin shooting star' WHERE POL_NUMBER = 1 AND POL_PO_ID = 1111; 

 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT COUNT (*) FROM S_site ;

 values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
 SELECT T1.C_SINCE, T1.C_STREET1, T1.C_STREET2, T1.C_CITY, T1.C_STATE, T1.C_COUNTRY, T1.C_ZIP, T1.C_PHONE, T1.C_CONTACT, T1.C_LAST, T1.C_FIRST, T1.C_ID FROM O_CUSTOMER  T1 WHERE T1.C_ID = 23456; 

values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS();
SELECT COUNT (*) FROM C_parts; 

INSERT INTO O_ORDERLINE (OL_O_ID, OL_ID, OL_SHIP_DATE, OL_QTY, OL_I_ID) VALUES (1, 1,NULL, 1,'abcdefghijklmn') ;


INSERT INTO M_LARGEORDER (LO_ID, LO_O_ID, LO_OL_ID, LO_ASSEMBLY_ID, LO_QTY, LO_DUE_DATE) VALUES (2, 2, 2,'id', 2, date('2000-02-29')); 


INSERT INTO O_ORDERS (O_ID, O_STATUS, O_SHIP_DATE, O_ENTRY_DATE, O_TOTAL, O_DISCOUNT, O_OL_CNT, O_C_ID) VALUES (111, 11, date('1999-01-01'),timestamp('1997-06-30 01:01:01'), 10, 10, 10, 10);

INSERT INTO M_WORKORDER (WO_NUMBER, WO_O_ID, WO_OL_ID, WO_STATUS, WO_ORIG_QTY, WO_COMP_QTY, WO_ASSEMBLY_ID, WO_DUE_DATE, WO_START_DATE) VALUES (10,10 ,10, 10,10, 10, 'abcd', date('2099-10-10'), timestamp('1997-06-30 01:01:01')); 

INSERT INTO O_CUSTOMER (C_ID, C_SINCE, C_STREET1, C_STREET2, C_CITY, C_STATE, C_COUNTRY, C_ZIP, C_PHONE, C_CONTACT, C_LAST, C_FIRST) VALUES (1, date('2000-01-01'), 'berkeley', 'berkeley','berkeley','ca', 'usofa', '94703', '01191797897', 'calvinandhobbes', 'watterson','bill'); 

INSERT INTO S_PURCHASE_ORDER (PO_NUMBER, PO_SUPP_ID, PO_SITE_ID) VALUES (100, 100, 100); 

INSERT INTO S_PURCHASE_ORDERLINE (POL_NUMBER, POL_PO_ID, POL_P_ID, POL_QTY, POL_BALANCE, POL_DELDATE, POL_MESSAGE) VALUES (121,987 ,'snowsnowsnow',11 , 999, date('2003-1-01'),'wow, it really snowed last night isnt it wonderful  last calvin and hobbes'); 

INSERT INTO C_CUSTOMER (C_ID, C_SINCE, C_BALANCE, C_CREDIT, C_CREDIT_LIMIT, C_YTD_PAYMENT) VALUES (11, date('2000-10-01'), 1000, 'ab', 10000,1000.20); 

-- Cleanup : Drop all the tables created as part of this test

DROP TABLE C_customer;
DROP TABLE C_supplier;
DROP TABLE C_site;
DROP TABLE C_parts;
DROP TABLE C_rule;
DROP TABLE C_discount;
DROP TABLE M_parts;
DROP TABLE M_bom;
DROP TABLE M_workorder;
DROP TABLE M_largeorder;
DROP TABLE M_inventory;
DROP TABLE O_customer;
DROP TABLE O_orders;
DROP TABLE O_orderline;
DROP TABLE O_item;
DROP TABLE S_component;
DROP TABLE S_supp_component;
DROP TABLE S_supplier;
DROP TABLE S_site;
DROP TABLE S_purchase_order;
DROP TABLE S_purchase_orderline;
DROP TABLE U_sequences;
