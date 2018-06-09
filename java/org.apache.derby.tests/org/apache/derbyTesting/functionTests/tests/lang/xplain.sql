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
create table derby6216( a int, status varchar(10));
insert into derby6216 values (1, 'ACTIVE'), (2, 'IDLE');
call syscs_util.syscs_set_runtimestatistics(1); 
call syscs_util.syscs_set_xplain_schema('STATS'); 
call syscs_util.syscs_set_xplain_mode(1); 
select a from derby6216 where status != 'IDLE'; 
call syscs_util.syscs_set_xplain_mode(0); 
call syscs_util.syscs_set_runtimestatistics(0); 
call syscs_util.syscs_set_xplain_schema(''); 
select stmt_text from stats.sysxplain_statements;
select st.stmt_text, rs.op_identifier
       from stats.sysxplain_statements st
       join stats.sysxplain_resultsets rs
         on st.stmt_id = rs.stmt_id
       order by st.stmt_text,rs.op_identifier;
select st.stmt_text, sp.no_visited_pages, sp.no_visited_rows 
    from stats.sysxplain_scan_props sp, 
         stats.sysxplain_resultsets rs, 
         stats.sysxplain_statements st 
    where st.stmt_id = rs.stmt_id and 
          rs.scan_rs_id = sp.scan_rs_id and 
          rs.op_identifier = 'TABLESCAN' and 
          sp.scan_object_name = 'DERBY6216';

drop table stats.sysxplain_statements;
drop table stats.sysxplain_resultsets;
drop table stats.sysxplain_scan_props;
drop table stats.sysxplain_sort_props;

call syscs_util.syscs_set_runtimestatistics(1); 
call syscs_util.syscs_set_xplain_schema('STATS'); 
call syscs_util.syscs_set_xplain_mode(1); 
select sql_text from syscs_diag.transaction_table where status != 'IDLE'; 
call syscs_util.syscs_set_xplain_mode(0); 
call syscs_util.syscs_set_runtimestatistics(0); 
call syscs_util.syscs_set_xplain_schema(''); 
select stmt_text from stats.sysxplain_statements;
select st.stmt_text, rs.op_identifier, rs.op_details, rs.returned_rows
       from stats.sysxplain_statements st
       join stats.sysxplain_resultsets rs
         on st.stmt_id = rs.stmt_id
       order by st.stmt_text,rs.op_identifier;
select count(*) from stats.sysxplain_scan_props;
select count(*) from stats.sysxplain_sort_props;
