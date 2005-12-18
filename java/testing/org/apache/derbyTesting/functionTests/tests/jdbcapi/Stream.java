/*

Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.Stream

Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.derby.tools.ij;


public class Stream {
    
    public static void main(String[] args){
	
	Connection conn = null;

	try{
	    ij.getPropertyArg(args);
	    conn = ij.startJBMS();
	    
	    createTestTables(conn);
	    executeTests(conn);
	    dropTestTables(conn);
	    
	}catch(Throwable t){
	    t.printStackTrace();
	    
	}finally{
	    if(conn != null){
		try{
		    conn.close();
		    
		}catch(SQLException e){
		    e.printStackTrace();
		}
		
	    }
	}
	
    }

    
    private static void createTestTables(Connection conn) 
	throws SQLException,IOException {
	
	createTable(conn);
	createTestData(conn);
	
    }
    
    
    private static void createTable(Connection conn) throws SQLException {
	
	Statement st = null;
	
	try{
	    
	    st = conn.createStatement();
	    st.execute("create table SMALL_BLOB_TABLE( SMALL_BLOB blob( 512 ))");
	    st.execute("create table LARGE_BLOB_TABLE( LARGE_BLOB blob( 512k ))");
	    st.execute("create table SMALL_CLOB_TABLE( SMALL_CLOB clob( 512 ))");
	    st.execute("create table LARGE_CLOB_TABLE( LARGE_CLOB clob( 512k ))");

	}finally{
	    if(st != null)
		st.close();
	}
	
    }

    
    private static void createTestData(Connection conn) 
	throws SQLException,IOException {

	createSmallBlobTestData( conn );
	createLargeBlobTestData( conn );
	createSmallClobTestData( conn );
	createLargeClobTestData( conn );
	
    }
    
    
    private static void createSmallBlobTestData(Connection conn) 
	throws SQLException,IOException {
	
	PreparedStatement st = null;
	TestDataStream stream = null;

	try{
	    st = conn.prepareStatement("insert into SMALL_BLOB_TABLE(SMALL_BLOB) values(?)");
	    stream = new TestDataStream(512);
	    st.setBinaryStream(1, stream, 512);
	    st.executeUpdate();
	    
	}finally{
	    if(st != null){
		st.close();
	    }

	    if(stream != null){
		stream.close();
	    }
	    
	}

    }
    
    
    private static void createLargeBlobTestData(Connection conn) 
	throws SQLException,IOException {
	
	PreparedStatement st = null;
	TestDataStream stream = null;
	
	try{
	    st = conn.prepareStatement("insert into LARGE_BLOB_TABLE(LARGE_BLOB) values(?)");
	    stream = new TestDataStream( 512 * 1024);
	    st.setBinaryStream(1,stream, 512 * 1024);

	    st.executeUpdate();
	    
	}finally{
	    if(st != null){
		st.close();
	    }

	    if(stream != null){
		stream.close();
	    }
	}
    }
    
    
    private static void createSmallClobTestData(Connection conn)
	throws SQLException,IOException {
	
	PreparedStatement st = null;
	TestDataReader reader = null;
	
	try{
	    st = conn.prepareStatement("insert into SMALL_CLOB_TABLE( SMALL_CLOB ) values(?)");

	    reader = new TestDataReader( 512 );
	    st.setCharacterStream(1,
				  reader, 
				  512);
	    
	    st.executeUpdate();
	    
	    
	}finally{
	    if(st != null)
		st.close();
	    
	    if(reader != null)
		reader.close();
	    
	}
	
    }   
    

    private static void createLargeClobTestData(Connection conn)
	throws SQLException, IOException {
	
	PreparedStatement st = null;
	TestDataReader reader = null;

	try{
	    st = conn.prepareStatement("insert into LARGE_CLOB_TABLE( LARGE_CLOB ) values(?)");
	    
	    reader = new TestDataReader( 512 * 1024 );
	    st.setCharacterStream(1,
				  reader,
				  512 * 1024 );
	    
	    st.executeUpdate();
	    
	    
	} finally {
	    if(st != null)
		st.close();
	    
	    if(reader != null)
		reader.close();
	}
    }

    private static void executeTests(Connection conn) 
	throws SQLException, IOException {
	
	executeTestOnSmallBlob( conn );
	executeTestOnLargeBlob( conn );
	executeTestOnSmallClob( conn );
	executeTestOnLargeClob( conn );
	
    }
    
    
    private static void executeTestOnSmallBlob( Connection conn ) 
	throws SQLException, IOException {
	
	BlobTester tester = new BlobTester( "SMALL_BLOB_TABLE", 
					    "SMALL_BLOB" );
	tester.testGetStreamTwice( conn );
	
    }


    private static void executeTestOnLargeBlob( Connection conn ) 
	throws SQLException, IOException {
	
	BlobTester tester = new BlobTester( "LARGE_BLOB_TABLE", 
					    "LARGE_BLOB" );
	tester.testGetStreamTwice( conn );
	
    }
    
    
    private static void executeTestOnSmallClob( Connection conn ) 
	throws SQLException, IOException {
	
	ClobTester tester = new ClobTester( "SMALL_CLOB_TABLE",
					    "SMALL_CLOB" );
	tester.testGetReaderTwice( conn );

    }


    private static void executeTestOnLargeClob( Connection conn ) 
	throws SQLException, IOException {
	
	ClobTester tester = new ClobTester( "LARGE_CLOB_TABLE",
					    "LARGE_CLOB" );
	tester.testGetReaderTwice( conn );

    }
    

    private static void dropTestTables( Connection conn ) throws SQLException {
	
	Statement st = null;
	
	try{
	    st = conn.createStatement();
	    st.execute("drop table SMALL_BLOB_TABLE");
	    st.execute("drop table LARGE_BLOB_TABLE");

	}finally{
	    if(st != null)
		st.close();
	}
	
    }
    
    
    static class TestDataStream extends InputStream {
	
	private long streamedLength = 0;
	private final long total;
	
	
	public TestDataStream(long length){
	    total = length;
	}
	
	
	public int read(){
	    
	    if(streamedLength >= total){
		return -1;
	    }

	    return (int) ((streamedLength ++) % 256L);
	    
	}
	
	
	public void close(){
	    streamedLength = total;
	}
	
    }
    

    static class TestDataReader extends Reader {
	
	private long wroteLength = 0;
	private final long total;

	
	public TestDataReader(long length){
	    total = length;
	}

	
	public void close(){
	    wroteLength = total;
	}

	
	public int read( char[] cbuf,
			 int off,
			 int len ){
	    
	    if(wroteLength >= total)
		return -1;
	    
	    int i;
	    for(i = off ;
		i < off + len &&
		    wroteLength <= total ;
		i++, wroteLength ++){
		
		cbuf[i] = (char) (wroteLength % 0x10000L);
		
	    }

	    return i - off;
	}
	
    }
    
    
    static class BlobTester {
	
	final String tableName;
	final String colName;
	
	
	BlobTester(String tableName,
		   String colName){
	    
	    this.tableName  = tableName;
	    this.colName = colName;
	    
	}
	
	
	public void testGetStreamTwice(Connection conn) 
	    throws SQLException, IOException {
	    
	    Statement st = null;
	    ResultSet rs = null;
	    InputStream is = null;

	    try{
		st = conn.createStatement();
		
		rs = st.executeQuery("select " + 
				     colName + " "+ 
				     "from " + 
				     tableName);
		rs.next();
		
		System.out.println("get stream from " + tableName + "." + colName + " ...");
		is = rs.getBinaryStream(1);
		is.close();
		
		System.out.println("get stream from " + tableName + "." + colName + " again ...");
		is = rs.getBinaryStream(1);
		
		System.out.println("Expected exception did not happen.");
		
	    }catch(SQLException e){
		System.out.println("Expected exception may happen.");
		e.printStackTrace(System.out);
		
	    }finally{
		if( st != null )
		    st.close();
		
		if( rs != null )
		    rs.close();
		
		if( is != null )
		    is.close();
		
	    }
	}
    }


    static class ClobTester {
	
	final String tableName;
	final String colName;

	public ClobTester( String tableName ,
			   String colName ){
	    
	    this.tableName = tableName;
	    this.colName = colName;
	    
	}
	
	
	public void testGetReaderTwice( Connection conn ) 
	    throws SQLException, IOException {
	    
	    Statement st = null;
	    ResultSet rs = null;
	    Reader reader = null;

	    try{
		st = conn.createStatement();
		
		rs = st.executeQuery( "select " + 
				      colName + " " + 
				      "from " + 
				      tableName );
		rs.next();
		
		System.out.println("get reader from " + tableName + "." + colName + " ...");
		reader = rs.getCharacterStream(1);
		reader.close();
		
		System.out.println("get reader from " + tableName + "." + colName + "again ...");
		reader = rs.getCharacterStream(1);
		
		System.out.println("Expected exception did not happen.");
		
	    }catch(SQLException e){
		System.out.println("Expected exception may happen.");
		e.printStackTrace(System.out);

	    }finally{
		if(st != null)
		    st.close();
		
		if(rs != null)
		    rs.close();

		if(reader != null)
		    reader.close();

	    }
	}
    }
}
