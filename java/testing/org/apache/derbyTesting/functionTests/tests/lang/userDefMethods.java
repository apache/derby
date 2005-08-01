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

	/* ****
	 * Derby-388: When a set of inserts & updates is performed on a table
	 * and each update fires a trigger that in turn performs other updates,
	 * Derby will sometimes try to recompile the trigger in the middle
	 * of the update process and will throw an NPE when doing so.
	 */
	public static void derby388() throws SQLException
	{
		System.out.println("Running DERBY-388 Test.");
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		boolean needCommit = !conn.getAutoCommit();
		Statement s = conn.createStatement();

		// Create our objects.
		s.execute("CREATE TABLE D388_T1 (ID INT)");
		s.execute("CREATE TABLE D388_T2 (ID_2 INT)");
		s.execute(
			"CREATE TRIGGER D388_TRIG1 AFTER UPDATE OF ID ON D388_T1" +
			"	REFERENCING NEW AS N_ROW FOR EACH ROW MODE DB2SQL" +
			"	UPDATE D388_T2" +
			"	SET ID_2 = " +
			"	  CASE WHEN (N_ROW.ID <= 0) THEN N_ROW.ID" +
			"	  ELSE 6 END " +
			"   WHERE N_ROW.ID < ID_2"
		);

		if (needCommit)
			conn.commit();

		// Statement to insert into D388_T1.
		PreparedStatement ps1 = conn.prepareStatement(
			"INSERT INTO D388_T1 VALUES (?)");

		// Statement to insert into D388_T2.
		PreparedStatement ps2 = conn.prepareStatement(
			"INSERT INTO D388_T2(ID_2) VALUES (?)");

		// Statement that will cause the trigger to fire.
		Statement st = conn.createStatement();
		for (int i = 0; i < 20; i++) {

			for (int id = 0; id < 10; id++) {

				ps2.setInt(1, id);
				ps2.executeUpdate();
				ps1.setInt(1, 2*id);
				ps1.executeUpdate();

				if (needCommit)
					conn.commit();

			}

			// Execute an update, which will fire the trigger.
			// Note that having the update here is important
			// for the reproduction.  If we try to remove the
			// outer loop and just insert lots of rows followed
			// by a single UPDATE, the problem won't reproduce.
			st.execute("UPDATE D388_T1 SET ID=5");
			if (needCommit)
				conn.commit();
				
		}

		// Clean up.
		s.execute("DROP TABLE D388_T1");
		s.execute("DROP TABLE D388_T2");

		if (needCommit)
			conn.commit();
				
		st.close();
		ps1.close();
		ps2.close();

		System.out.println("DERBY-388 Test Passed.");
	}

}
