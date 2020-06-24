/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.userDefMethods

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.lang;
import java.sql.*;
import java.util.Vector;


//This class defines miscelanious test java methods to be called from sql.
//These are not generic methods, typically  used by a particular tests.
public class userDefMethods
{
 
	//method that does a delete of rows on table t1 based on values from triggers.
    public static void deleteFromATable() throws SQLException
    {
		Connection con = DriverManager.getConnection("jdbc:default:connection");
        PreparedStatement statement = null;
        Statement s = con.createStatement();
        ResultSet rs = s.executeQuery("SELECT c1 from new org.apache.derby.catalog.TriggerOldTransitionRows() AS EQ");
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        Vector<Long> keys = new Vector<Long>();
        while(rs.next()){
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            keys.addElement(rs.getLong(1));
        }
        rs.close();

        statement = 
        con.prepareStatement("delete from t1  where c1  = ?");
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (long key : keys) {
           statement.setLong(1, key);
           statement.executeUpdate();
		}
        statement.close();
    }


	public static void deleteFromParent() throws SQLException
    {
		Connection con = DriverManager.getConnection("jdbc:default:connection");
        String sqlstmt;
		Statement stmt = con.createStatement();
		sqlstmt = "SELECT a FROM new org.apache.derby.catalog.TriggerOldTransitionRows() AS EQ";
		ResultSet rs = stmt.executeQuery(sqlstmt);
		sqlstmt = "delete from parent where a = ? ";
		PreparedStatement pstmt = con.prepareStatement(sqlstmt);
		while(rs.next()){
			long value = rs.getLong(1);
			if(value == 1 || value == 3)
				value = 4;
			else
				value = 5;
			pstmt.setLong(1,value);
			pstmt.executeUpdate();
		}
		rs.close();
		stmt.close();
		pstmt.close();
	}

}
