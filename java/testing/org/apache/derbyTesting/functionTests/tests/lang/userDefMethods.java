/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.userDefMethods

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

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
        String delStr = null;
        Statement s = con.createStatement();
        ResultSet rs = s.executeQuery("SELECT c1 from new org.apache.derby.catalog.TriggerOldTransitionRows() AS EQ");
        Vector keys = new Vector();
        while(rs.next()){
            keys.addElement(new Long(rs.getLong(1)));
        }
        rs.close();

        statement = 
        con.prepareStatement("delete from t1  where c1  = ?");
        for(int i = 0; i < keys.size() ; i++){ 
           long key = ((Long)keys.elementAt(i)).longValue();
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
