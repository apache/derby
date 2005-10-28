/*

   Derby - Class org.apache.derbyTesting.functionTests.store.LogChecksumSetup

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.store;
import java.sql.*;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import org.apache.derbyTesting.functionTests.util.corruptio.CorruptibleIo;
import org.apache.derby.tools.ij;

/*
 * Purpose of this class is to simulate out of order incomplete 
 * log write corruption (see derby-96 for details) using the proxy storage
 * factory (org.apache.derbyTesting.functionTests.util.corruptio.
 * CorruptDiskStorageFactory) instead of the default storage factory.
 * By defailt all io is delegated to the default database storage factory,
 * except when corruption is enabled through CorruptibleIo class.
 * Proxy storage factory is loaded using the following properties in 
 * the test properties file:
 * derby.subSubProtocol.csf=org.apache.derbyTesting.functionTests.
 *             util.corruptio.CorruptDiskStorageFactory
 *  database=jdbc:derby:csf:wombat
 *
 * @author <a href="mailto:suresh.thalamati@gmail.com">Suresh Thalamati</a>
 * @version 1.0
 * @see CorruptibleIo
 */

public class LogChecksumSetup{

	private CorruptibleIo cbio;

	LogChecksumSetup()
	{
		cbio = CorruptibleIo.getInstance();
	}
	
	/**
	 * Insert some rows into the table and corrupt the log for the last row,
	 * so when we recover , there should be one row less even though we committed.
	 */
	void insertAndCorrupt(Connection conn, int rowCount) throws SQLException {

		PreparedStatement ps = conn.prepareStatement("INSERT INTO " + 
													 "T1" + 
													 " VALUES(?,?,?)");

		java.util.Random r = new java.util.Random();
		CRC32 checksum = new CRC32(); // holder for the checksum
		boolean corrupt = false;
		for (int i = 0; i < rowCount; i++) {
			
			//setup last row for log corruption
			if (i == (rowCount -1 ))
			{
				// Note: offset/len for corruption  here refers to 
				// the actual log write request
				// that is being done for this insert. 
				setupLogCorruption(50, 10);
				corrupt = true;
			}
			ps.setInt(1, i); // ID
			byte[] dataBytes  = generateBinaryData(r, 90000 , 1000 * i);
			ps.setBytes(2, dataBytes); 
			//calculate checksum for blob data 
			checksum.update(dataBytes, 0, dataBytes.length);
			checksum.reset();
			checksum.update(dataBytes, 0, dataBytes.length);
			ps.setLong(3, checksum.getValue());
			ps.executeUpdate();
			conn.commit();
		}
	}

		
	/**
	 * update some rows in the table and corrupt the log for the last row,
	 * so when we recover , All checsum should be correct because corrupted 
	 * log transaction should been rolled back.
	 */

	void updateAndCorrupt(Connection conn, int rowCount) throws SQLException{

		PreparedStatement ps = conn.prepareStatement("update " + "T1" + 
													 " SET " +
													 "DATA=?, DATACHECKSUM=? where ID=?");
		
		java.util.Random r = new java.util.Random();
		CRC32 checksum = new CRC32(); // holder for the checksum
		int updateCount = 0;
		boolean corrupt = false;
		for (int i = 0; i < rowCount; i++) {
			
			//setup last row for log corruption
			if (i == (rowCount -1 ))
			{
				// Note: offset/len for corruption  here refers to 
				// the actual log write request
				// that is being done for this insert. 
				setupLogCorruption(50, 10);
				corrupt = true;
			}
			byte[] dataBytes  = generateBinaryData(r, 1234 , 5000 * i);
			ps.setBytes(1, dataBytes); 

			// calculate checksum for blob data 
			checksum.update(dataBytes, 0, dataBytes.length);
			checksum.reset();
			checksum.update(dataBytes, 0, dataBytes.length);

			ps.setLong(2, checksum.getValue());
			ps.setInt(3, i); // ID
			updateCount +=  ps.executeUpdate();
			conn.commit();
		}
	}


	/*
	 * read the data from the table and verify the blob data using the 
	 * checksum and make sure that expected number rows exist in the table. 
	 * 
	 */
	void verifyData(Connection conn, int expectedRowCount) throws SQLException {
		
		Statement s = conn.createStatement();
		CRC32 checksum = new CRC32(); // holder for the checksum
		
		ResultSet rs = s.executeQuery("SELECT DATA , DATACHECKSUM, ID FROM "
									  + "T1" );
		int count = 0;
		while(rs.next())
		{
			byte[] dataBytes = rs.getBytes(1);
			long ckmRead = rs.getLong(2);
			int id = rs.getInt(3);

			checksum.reset();
			checksum.update(dataBytes, 0, dataBytes.length);

			if(checksum.getValue() != ckmRead )
			{
				logMessage("CHECKSUMs ARE NOT MATCHING");
				logMessage("ID=" + id + " Checksum From DB:" + ckmRead);
				logMessage("Recalcaulted sum :" + checksum.getValue());
				logMessage("Length of Data:" +  dataBytes.length);
			}
			
			count++;
		}
		conn.commit();

		if(count != expectedRowCount)
		{
			logMessage("Expected Number Of Rows (" + 
					   expectedRowCount + ")" +  "!="  + 
					   "No Of rows in the Table(" + 
					   count + ")");
		}
	}

	/* 
	 * create the tables that are used by this test.
	 */
	private  void createTable(Connection conn) throws SQLException {

		Statement s = conn.createStatement();
		s.executeUpdate("CREATE TABLE " + "T1" + "(ID INT," +
						"DATA BLOB(300000),"+ 
						"DATACHECKSUM BIGINT)");
		conn.commit();
		s.close();
	}

	/*
	 * Log is corrupted using the corrupt storage factory. 
	 * setup offset/length where we want the transaction 
	 * log to be corrupted. Transaction tat the corruption 
	 * is simulated  on should be rolled back because the log check
	 * should identify that the writes were incomplete.  
	 */
	private void setupLogCorruption(int off , int len)
	{
		cbio.setLogCorruption(true);
		cbio.setOffset(off); 
		cbio.setLength(len); 
	}


	/*
	 * utility routine to generate random byte array of data.
	 */
	private  byte[] generateBinaryData(java.util.Random r, 
											 int factor,
											 int size)	{
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
		try{
			DataOutputStream daos = new DataOutputStream(baos);
			for(int i = 0 ; i < size ; i++)
			{
				int p = r.nextInt() % factor;
				if (p < 0)
					p = p * -1;
				daos.writeInt(p);
			}
			
		}catch(IOException ie) 
		{
			logMessage(ie.getMessage()) ;
		}
		return baos.toByteArray();
	}


	private void runTest(Connection conn) throws SQLException
	{
		logMessage("Begin LogCheckum Setup Test");
		createTable(conn);
		insertAndCorrupt(conn, 11);
		logMessage("End LogChecksum Setup Test");
	}
	
    void logMessage(String   str)
    {
        System.out.println(str);
    }

	public static void main(String[] argv) throws Throwable {
        LogChecksumSetup lctest = new LogChecksumSetup();
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);

        try {
            lctest.runTest(conn);
        }
        catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}
