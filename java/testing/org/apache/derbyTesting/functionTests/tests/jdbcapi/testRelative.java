package org.apache.derbyTesting.functionTests.tests.jdbcapi;


import java.sql.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

public class testRelative {
   public static void main(String[] args) {
        test1(args);        
    }
    
    public static void test1(String []args) {   
                Connection con;
                ResultSet rs;
                PreparedStatement stmt = null;
                PreparedStatement pStmt = null;
                Statement stmt1 = null;
                String returnValue = null;

                System.out.println("Test testRelative starting");

                try
                {
                        // use the ij utility to read the property file and
                        // make the initial connection.
                        ij.getPropertyArg(args);
                        con = ij.startJBMS();
					
			con.setAutoCommit(false);                        			              
                        
                        stmt = con.prepareStatement("create table testRelative(name varchar(10), i int)");
   			stmt.executeUpdate();
   			con.commit();
   			
   			pStmt = con.prepareStatement("insert into testRelative values (?,?)");
   			   			
   			pStmt.setString(1,"work1");
			pStmt.setNull(2,1);
			pStmt.addBatch();
			
			pStmt.setString(1,"work2");
			pStmt.setNull(2,2);
			pStmt.addBatch();
			
			pStmt.setString(1,"work3");
			pStmt.setNull(2,3);
			pStmt.addBatch();
			
			pStmt.setString(1,"work4");
			pStmt.setNull(2,4);
			pStmt.addBatch();

		
			pStmt.executeBatch();
			con.commit();

			stmt1 = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
		        rs = stmt1.executeQuery("select * from testRelative");						

   			rs.next(); // First Record
   			returnValue = rs.getString("name");
   			System.out.println("Value="+returnValue);

   			rs.relative(2);
   			System.out.println("isFirst=" + rs.isFirst() + " isLast=" + rs.isLast() + " isAfterLast=" + rs.isAfterLast());
   			returnValue = rs.getString("name");
   			System.out.println("Value="+returnValue);

   			rs.relative(-2);
   			returnValue = rs.getString("name");
   			System.out.println("Value="+returnValue);

   			rs.relative(10);
   			System.out.println("isFirst=" + rs.isFirst() + " isLast=" + rs.isLast() + " isAfterLast=" + rs.isAfterLast());

   			returnValue = rs.getString("name");
   			System.out.println("Value="+returnValue);

 		} catch(SQLException sqle) {
 		   dumpSQLExceptions(sqle);
 		   sqle.printStackTrace();
 		} catch(Throwable e) {
 		   System.out.println("FAIL -- unexpected exception: "+e);
                   e.printStackTrace();

 		}
      }
      
      static private void dumpSQLExceptions (SQLException se) {
                System.out.println("FAIL -- unexpected exception");
                while (se != null) {
                        System.out.println("SQLSTATE("+se.getSQLState()+"): "+se);
                        se = se.getNextException();
                }
        }
}