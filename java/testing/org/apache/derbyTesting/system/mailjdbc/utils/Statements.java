/*
 * 
 * Derby - Class org.apache.derbyTesting.system.mailjdbc.utils.Statements
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *  
 */
package org.apache.derbyTesting.system.mailjdbc.utils;
//This class has all the SQL statements use for the test
public class Statements {
	public static String readStr = "select * from REFRESH.INBOX";

	public static String insertStr = "insert into REFRESH.INBOX(from_name,to_name,date,Message) values (?,?,?,?)";
	
	public static String insertStrAttach = "insert into REFRESH.ATTACH (id,attachment) values (?,?)";

	public static String deleteStr = "delete from REFRESH.INBOX where to_delete = 1";
	
	public static String updateStr = "update REFRESH.INBOX set to_delete = ? where id = ?";
	
	public static String getRowCount = "select count(*) from REFRESH.INBOX";

	public static String getRowCountAttach = "select count(*) from REFRESH.ATTACH";

	public static String getRowCountin = "select count(*)from REFRESH.INBOX where to_name = ?";

	public static String getRowCountdel = "select count(*)from REFRESH.INBOX where to_delete = 1";

	public static String moveStr = "insert into folders (foldername,message_id) (select cast(? as varchar(16)),id from REFRESH.INBOX where date=?)";

	public static String movefolder = "update REFRESH.INBOX set folder_id = ? where id = ?";

	public static String selExp = "select id,date from REFRESH.INBOX ";

	public static String delExp = "delete from REFRESH.INBOX where id = ?";

	public static String del_jdbc_exp = "delete from REFRESH.INBOX where (values {fn TIMESTAMPDIFF(SQL_TSI_DAY,  date,CURRENT_TIMESTAMP)})>1";

	public static String getTableCount = "select count(*) from sys.systables";

	public static String grantSel1 = "grant select on REFRESH.INBOX to BROWSE";

	public static String grantSel2 = "grant select on REFRESH.ATTACH to BROWSE";

	public static String grantSel3 = "grant select on REFRESH.INBOX to BACKUPVER";

	public static String grantSel4 = "grant select on REFRESH.ATTACH to BACKUPVER";

	public static String grantSel5 = "grant select on REFRESH.ATTACH to PURGE";

	public static String grantSel6 = "grant select on REFRESH.INBOX to PURGE";

	public static String grantSel7 = "grant select on REFRESH.FOLDERS to BROWSE";

	public static String grantIns1 = "grant insert on REFRESH.INBOX to BROWSE";

	public static String grantIns2 = "grant insert on REFRESH.ATTACH to BROWSE";

	public static String grantIns3 = "grant insert on REFRESH.FOLDERS to BROWSE";

	public static String grantUp1 = "grant update on REFRESH.INBOX to BROWSE";

	public static String grantUp2 = "grant update on REFRESH.ATTACH to BROWSE";

	public static String grantUp3 = "grant update on REFRESH.INBOX to BROWSE";

	public static String grantDel1 = "grant delete on REFRESH.INBOX to PURGE";

	public static String grantDel2 = "grant delete on REFRESH.ATTACH to PURGE";

	public static String grantDel3 = "grant delete on REFRESH.FOLDERS to BROWSE";
	
	public static String grantExe1 = "grant execute on procedure SYSCS_UTIL.SYSCS_BACKUP_DATABASE to BACKUP";

	public static String grantExe2 = "grant execute on procedure SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE  to BACKUP";

	public static String grantExe3 = "grant execute on procedure SYSCS_UTIL.SYSCS_BACKUP_DATABASE_AND_ENABLE_LOG_ARCHIVE_MODE_NOWAIT to BACKUP";

	public static String grantExe4 = "grant execute on procedure SYSCS_UTIL.SYSCS_DISABLE_LOG_ARCHIVE_MODE to BACKUP";

	public static String grantExe5 = "grant execute on procedure SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE to BACKUP";
}
