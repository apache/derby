package org.apache.derbyTesting.functionTests.tests.jdbcapi;


import java.sql.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

import org.apache.derbyTesting.functionTests.util.TestUtil;

public class prepStmtNull {

    public static void main(String[] args) {
        test1(args);
        test2(args);
        test3(args);
    }
    
        public static void test1(String []args) {   
                Connection con;
                ResultSet rs;
                PreparedStatement stmt = null;
                PreparedStatement pStmt = null;
                Statement stmt1 = null;

                System.out.println("Test prepStmtNull starting");

                try
                {
                        // use the ij utility to read the property file and
                        // make the initial connection.
                        ij.getPropertyArg(args);
                        con = ij.startJBMS();
					
			con.setAutoCommit(false);                        			              
                        
                        stmt = con.prepareStatement("create table nullTS(name varchar(10), ts timestamp)");
   			stmt.executeUpdate();
   			con.commit();
   			
   			pStmt = con.prepareStatement("insert into nullTS values (?,?)");
   			   			
   			pStmt.setString(1,"work");
			pStmt.setNull(2,java.sql.Types.TIMESTAMP);
			pStmt.addBatch();
			pStmt.setString(1,"work1");
			pStmt.setNull(2,java.sql.Types.TIMESTAMP,"");
			pStmt.addBatch();

		
			pStmt.executeBatch();
			con.commit();

			stmt1 = con.createStatement();
		        rs = stmt1.executeQuery("select * from nullTS");
			while(rs.next()) {
			   System.out.println("ResultSet is: "+rs.getObject(1));
			   System.out.println("ResultSet is: "+rs.getObject(2));
			}
			String[] testObjects = {"table nullTS"};
                        TestUtil.cleanUpTest(stmt1, testObjects);
   			con.commit();
 		} catch(SQLException sqle) {
 		   dumpSQLExceptions(sqle);
 		   sqle.printStackTrace();
 		} catch(Throwable e) {
 		   System.out.println("FAIL -- unexpected exception: "+e);
                   e.printStackTrace();

 		}
     }
     
     public static void test2(String []args) {   
                Connection con;
                ResultSet rs;
                PreparedStatement stmt = null;
                PreparedStatement pStmt = null;
                Statement stmt1 = null;

                System.out.println("Test prepStmtNull starting");

                try
                {
                        // use the ij utility to read the property file and
                        // make the initial connection.
                        ij.getPropertyArg(args);
                        con = ij.startJBMS();
					
			con.setAutoCommit(false);                        			              
                        
                        stmt = con.prepareStatement("create table nullBlob(name varchar(10), bval blob(16K))");
   			stmt.executeUpdate();
   			con.commit();
   			
   			pStmt = con.prepareStatement("insert into nullBlob values (?,?)");
   			   			
   			pStmt.setString(1,"blob");
			pStmt.setNull(2,java.sql.Types.BLOB);
			pStmt.addBatch();
			pStmt.setString(1,"blob1");
			pStmt.setNull(2,java.sql.Types.BLOB,"");
			pStmt.addBatch();

		
			pStmt.executeBatch();
			con.commit();

			stmt1 = con.createStatement();
		        rs = stmt1.executeQuery("select * from nullBlob");
			while(rs.next()) {
			   System.out.println("ResultSet is: "+rs.getObject(1));
			   System.out.println("ResultSet is: "+rs.getObject(2));
			}
			String[] testObjects = {"table nullBlob"};
                        TestUtil.cleanUpTest(stmt1, testObjects);
   			con.commit();
 		} catch(SQLException sqle) {
 		   dumpSQLExceptions(sqle);
 		   sqle.printStackTrace();
 		} catch(Throwable e) {
 		   System.out.println("FAIL -- unexpected exception: "+e);
                   e.printStackTrace();

 		}
     }
     
     /* Test setNull() on Clob/Blob using Varchar/binary types */
     public static void test3(String []args) {
          Connection con;
          ResultSet rs;
          PreparedStatement stmt = null;
          PreparedStatement pStmt = null;
          Statement stmt1 = null;
          byte[] b2 = new byte[1];
          b2[0] = (byte)64;

          System.out.println("Test3 prepStmtNull starting");

          try
          {
               // use the ij utility to read the property file and
               // make the initial connection.
               ij.getPropertyArg(args);
               con = ij.startJBMS();
					
               stmt = con.prepareStatement("create table ClobBlob(cval clob, bval blob(16K))");
               stmt.executeUpdate();
   			
               pStmt = con.prepareStatement("insert into ClobBlob values (?,?)");
   			   			
               pStmt.setNull(1, Types.VARCHAR);
               pStmt.setBytes(2, b2);
               pStmt.execute();
               pStmt.setNull(1, Types.VARCHAR,"");
               pStmt.setBytes(2, b2);
               pStmt.execute();

               stmt1 = con.createStatement();
               rs = stmt1.executeQuery("select * from ClobBlob");
               while(rs.next()) {
                    System.out.println("ResultSet is: "+rs.getObject(1));
               }
               String[] testObjects = {"table ClobBlob"};
               TestUtil.cleanUpTest(stmt1, testObjects);
          } catch(SQLException sqle) {
               dumpSQLExceptions(sqle);
          } catch(Throwable e) {
               System.out.println("FAIL -- unexpected exception: ");
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
