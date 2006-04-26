/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.LOBTest

   Copyright 2003, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Array;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.io.Reader;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Ref;
import java.net.URL;
import java.sql.PreparedStatement;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * @author Jonas S Karlsson
 */

public class LOBTest {
	/* the default framework is embedded*/
	public static final String driver = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String protocol = "jdbc:derby:";
        
        
	public static Connection connectAndCreate(String dbname) throws Exception {
		// connect and create db
		Class.forName(driver).newInstance(); // load driver
		Connection conn = DriverManager.getConnection(protocol+dbname
													  +";create=true");

		conn.setAutoCommit(false);
		return conn;
	}
	public static void disconnect(Connection conn) throws Exception {
		conn.commit();
		conn.close();
	}
    public static void printSQLError(SQLException e) {
        while (e != null) {
            System.out.print("\t");
            JDBCDisplayUtil.ShowSQLException(System.out, e);
            e = e.getNextException();
        }
    }
	//////////////////////////////////////////////////////////////////////
	public static void largeTest(String[] args) throws Exception{
		System.out.println("connecting");
		Connection conn = connectAndCreate("LOBdb");
		Statement s = conn.createStatement();

		try {
			System.out.println("dropping");
			s.executeUpdate("DROP TABLE atable");
		} catch (Exception e) {
		}

		System.out.println("creating");
		s.executeUpdate("CREATE TABLE atable (a INT, b LONG VARCHAR FOR BIT DATA)");
		conn.commit();
		java.io.File file = new java.io.File("short.utf");
		int fileLength = (int) file.length();

		// first, create an input stream
		java.io.InputStream fin = new java.io.FileInputStream(file);
		PreparedStatement ps = conn.prepareStatement("INSERT INTO atable VALUES (?, ?)");
		ps.setInt(1, 1);

		// set the value of the input parameter to the input stream
//              ps.setBinaryStream(2, fin, fileLength);
		ps.setBinaryStream(2, fin, -1);
		System.out.println("inserting");
		ps.execute();
		conn.commit();

		// reading the columns
		System.out.println("reading");
		ResultSet rs = s.executeQuery("SELECT b, octet_length(b) FROM atable WHERE a = 1");
		while (rs.next()) {
			java.sql.Clob aclob = rs.getClob(1);
			java.io.InputStream ip = rs.getAsciiStream(1);
			System.out.println("octet_length = "+rs.getInt(2));
		}

		System.out.println("disconnecting");
		disconnect(conn);
	}

    public static void typeTest(String[] args) throws Exception {

		// use the ij utility to read the property file and
			// make the initial connection.
		ij.getPropertyArg(args);
		Connection conn = ij.startJBMS();
		
		// old bit datatype, converted later to char () for bit data
		new LOBTester(conn, "bit", "(8 )").test();
		new LOBTester(conn, "bit", "(8 )").test();

		new LOBTester(conn, "blob", "(2 M)").test();
		new LOBTester(conn, "blob", "(2 K)").test();
		new LOBTester(conn, "blob", "(64  )").test();

		new LOBTester(conn, "clob", "(2 K)").test();
		new LOBTester(conn, "clob", "(64  )").test();


    	new LOBTester(conn, "blob", "(2 M)").testBlobInsert();
		disconnect(conn);
	}
    public static void main(String[] args) {
        try {
//			if (args.length > 0) {
//				largeTest(args);
//			} else {
				typeTest(args);
//			}
		}
		catch (Throwable e) {
			LOBTest.printException(e);
		}
	}

	public static void printException(Throwable e) {
		//System.out.println("\t Exception thrown:");
		if (e instanceof SQLException) 
			printSQLError((SQLException)e);
		else
			e.printStackTrace();
    }
}


class LOBTester {
    String typeName;
    String typeSpec;
    String table;
    String[] colNames;
    String[] colTypes;
	int columns;
	String[] colData;

    Connection conn;
    Statement st;

    String[] typeNames = { "int", "char(10)", "varchar(80)", "long varchar", "char(10) for bit data", "long varchar for bit data", "blob(80)" };

	static int BIT_OFFSET = 4;
	static int LONG_VARBINARY_OFFSET = 5;
	static int BLOB_OFFSET = 6;
	static int TYPE_COL_OFFSET= 7;

    public LOBTester(Connection c, String typeName, String typeSpec) throws SQLException {

        this.conn = c;
        this.typeName = typeName;
        this.typeSpec = typeSpec;
        this.table = typeName+"_table";
        this.st = this.conn.createStatement();

        columns = typeNames.length+1;
        this.colNames = new String[columns];
        this.colTypes = new String[columns];
        for(int i=0; i<columns-1; i++) {
            String colName = "col_"+i;
            colNames[i] = colName;
            colTypes[i] = typeNames[i];
        }
        colNames[columns-1] = "typecol";
	String tmpTypeNameSpec;
		if (typeName.equals("bit"))
			tmpTypeNameSpec="char" +" "+typeSpec + " for bit data";
		else
			tmpTypeNameSpec=typeName+" "+typeSpec;
		colTypes[columns-1] = tmpTypeNameSpec;
		colData = new String[] { "100","'101'","'102'", "'103'",
								 TestUtil.stringToHexLiteral("104"),
								 TestUtil.stringToHexLiteral("105"),
								 "CAST (" +TestUtil.stringToHexLiteral("106") +" AS " +
								 colTypes[BLOB_OFFSET] +")",
								 "CAST (" +TestUtil.stringToHexLiteral("107") +" AS " +
								 tmpTypeNameSpec + ")" };
		
    }
    public static void printResultSet(ResultSet rs) throws SQLException {
        if (rs==null) return;
        ResultSetMetaData md = rs.getMetaData();
        int cols = md.getColumnCount();

        boolean hasNext = true;
        // according to javadoc, rs already points to first
        // row, but it won't work if we don't call next()!

        // print some metadata
        for(int col=1; col<=cols; col++) {
            System.out.println("\t---- "+col);
            System.out.println("\tColumn    : "+md.getColumnName(col));
            System.out.println("\tType      : "+md.getColumnType(col));
            System.out.println("\tTypeName  : "+md.getColumnTypeName(col));
            System.out.println("\tClassName : "+md.getColumnClassName(col));
            System.out.println("\tLabel     : "+md.getColumnLabel(col));
            System.out.println("\tDisplaySz : "+md.getColumnDisplaySize(col));
            System.out.println("\tPrecision : "+md.getPrecision(col));
            System.out.println("\tScale     : "+md.getScale(col));
            System.out.println("\tisCurrency: "+md.isCurrency(col));
            System.out.println("\tisCaseSens: "+md.isCaseSensitive(col));
            System.out.println("\tisDefWrite: "+md.isDefinitelyWritable(col));
            System.out.println("\tisWrite   : "+md.isWritable(col));
            System.out.println("\tisSearchab: "+md.isSearchable(col));
//			System.out.println("\tSchemaName: "+md.getSchemaName(col));
            System.out.print("\n");
        }

        // print actual data
        while (rs.next()) { // for each row
            for(int col=1; col<=cols; col++) {
                Object c = rs.getObject(col);
                if (c==null)
                    System.out.println("\tOUT = NULL");
                else {
                    // fixup if it contains classname (remove "random" part after @)
                    String v = c.toString();
                    if (v.indexOf('@') != -1) {
                        v = v.substring(0, v.indexOf('@')+1);
                        System.out.println("\tOUT = Object : "+prettyType(c));
                    } else 
                        System.out.println("\tOUT = '"+v+"' : "+c.getClass().getName());
                }
            }
        }
    }
    public ResultSet X(String sql) throws SQLException {
        try {
            System.out.println("\n"+sql);
            // cercumwait stupid executeQuery which can't take non-selects...
            boolean result = ( (sql.charAt(0) == 'S') || (sql.charAt(0) == 's')); // detect "select" which returns result..
            if (!result) {
                st.execute(sql);
            } else {
                return st.executeQuery(sql);
            }
        } catch (Throwable e) {
            LOBTest.printException(e);
        }
        return null;
    }
    public void Xprint(String sql) {
        try {
            ResultSet rs = X(sql);
            printResultSet(rs);
        } catch (Throwable e) {
            LOBTest.printException(e);
        }
    }
    static String[] getterName = {
        "getObject", "getArray", "getAsciiStream", // 2
        "getBigDecimal", "getBinaryStream", "getBlob", // 5
        "getBoolean", "getByte", "getBytes", // 8
        "getCharacterStream", "getClob", "getDate", // 11
        "getDouble", "getFloat", "getInt", "getLong", // 15
        "getRef", "getShort", "getString", "getTime", // 19
        "getTimeStamp", "getURL" // 21
    };

    // getter takes a RESULTSET and uses GETTER on COLumn
    // getters numbered 0..N-1, for N-1 null is returned
    // otherwise descriptive string is returned
    // if the getter throws exception the string says so
    public static String getter(ResultSet rs, int getter, int col) {
        Object o = "-NO VALUE-";
        String s = "";
        try {
            if (getter < getterName.length) { // avoid array exception
                s = getterName[getter];
                for(int i=s.length(); i<20; i++) s+=' ';
                s += " ->";
            }

            switch(getter) {
                case 0: {o = rs.getObject(col); break;}
                case 1: {Array v=rs.getArray(col);o=v;break;}
                case 2: {InputStream v=rs.getAsciiStream(col);o=v;break;}
                case 3: {BigDecimal v=rs.getBigDecimal(col);o=v;break;}
                case 4: {InputStream v=rs.getBinaryStream(col);o=v;break;}
                case 5: {Blob v=rs.getBlob(col);o=v;break;}
                case 6: {boolean v=rs.getBoolean(col);o=new Boolean(v);break;}
                case 7: {byte v=rs.getByte(col);o=new Byte(v);break;}
                case 8: {byte[] v=rs.getBytes(col);o=v;break;}
                case 9: {Reader v=rs.getCharacterStream(col);o=v;break;}
                case 10:{Clob v=rs.getClob(col);o=v;break;}
                case 11:{Date v=rs.getDate(col);o=v; break;}
                case 12:{double v=rs.getDouble(col);o=new Double(v);break;}
                case 13:{float v=rs.getFloat(col);o=new Float(v);break;}
                case 14:{int v=rs.getInt(col);o=new Integer(v);break;}
                case 15:{long v=rs.getLong(col);o=new Long(v);break;}
                case 16:{Ref v=rs.getRef(col);o=v;break;}
                case 17:{short v=rs.getShort(col);o=new Short(v);break;}
                case 18:{String v=rs.getString(col);o=v;break;}
                case 19:{Time v=rs.getTime(col);o=v;break;}
                case 20:{Timestamp v=rs.getTimestamp(col);o=v;break;}
//				case 21:{URL v=rs.getURL(col);o=v;break;}
                default: return null;
            }
            // fixup if it contains classname (remove "random" part after @)
            String v = o.toString();
            if (v.indexOf('@') != -1) { // non standard java object.
                s += "Object'   \t: "+prettyType(o);
            } else {
                // default stringifier...
                s += "'"+v+"'    \t: "+o.getClass().getName();
            }
        } catch (Throwable e) {
            s += "\t\tEXCEPTION ("+e.getMessage()+")";
        }
        return s;
    }
    static public String prettyType(Object o) {
        if (o instanceof java.sql.Blob) return "java.sql.Blob";
        if (o instanceof java.sql.Clob) return "java.sql.Clob";
        if (o instanceof java.io.InputStream) return "java.io.InputStream";
        if (o instanceof java.io.Reader) return "java.io.Reader";
        if (o instanceof byte[]) return "byte[]";
        return "Unknown type - "+o.getClass().getName();
    }
    public void testGetters() throws SQLException {
        for(int i=0; i<columns; i++) {
            System.out.println("\n\n=== Columntype "+colTypes[i]);
	    
            String s = 
		"select "+
		colNames[i] + " as " + colNames[i] + "_1, " +
		colNames[i] + " as " + colNames[i] + "_2, " +
		colNames[i] + " as " + colNames[i] + "_3, " +
		colNames[i] + " as " + colNames[i] + "_4, " +
		colNames[i] + " as " + colNames[i] + "_5, " +
		colNames[i] + " as " + colNames[i] + "_6, " +
		colNames[i] + " as " + colNames[i] + "_7, " +
		colNames[i] + " as " + colNames[i] + "_8, " +
		colNames[i] + " as " + colNames[i] + "_9, " +
		colNames[i] + " as " + colNames[i] + "_10, " +
		colNames[i] + " as " + colNames[i] + "_11, " +
		colNames[i] + " as " + colNames[i] + "_12, " +
		colNames[i] + " as " + colNames[i] + "_13, " +
		colNames[i] + " as " + colNames[i] + "_14, " +
		colNames[i] + " as " + colNames[i] + "_15, " +
		colNames[i] + " as " + colNames[i] + "_16, " +
		colNames[i] + " as " + colNames[i] + "_17, " +
		colNames[i] + " as " + colNames[i] + "_18, " +
		colNames[i] + " as " + colNames[i] + "_19, " +
		colNames[i] + " as " + colNames[i] + "_20, " +
		colNames[i] + " as " + colNames[i] + "_21 " +
		"from "+
		table;
	    
            ResultSet rs = X(s);
            rs.next(); // goto first
            int getno = 0;
            String r;
            while(null!=(r = getter(rs, getno, getno + 1 ))) {
                System.out.println("\t"+i+" "+r);
                getno++;
            }
        }
    } 
    public void testMetaData() {
        System.out.println("\n\n---< METADATA TESTS\n");
        // plain select
        for(int i=0; i<columns; i++) {
            String s = "select "+colNames[i]+" from "+table;
            Xprint(s);
        }
    }
    public void testCastTo() {
        System.out.println("\n\n---< type CAST TO types: METADATA TESTS\n");
        // CAST ( column TO types )
        for(int i=0; i<columns; i++) {
            String s;
            if (colTypes[i].startsWith("bit"))
                s = "select cast( typecol as char (8) for bit data) from "+table;
            else
                s = "select cast( typecol as "+colTypes[i]+" ) from "+table;
            Xprint(s);
        }
    }
    public void testCastFrom() {
        System.out.println("\n\n---< columns CAST TO type: METADATA TESTS\n");
        // CAST ( coltypes TO type )
        for(int i=0; i<columns; i++) {
            String s;
            if (typeName.startsWith("bit"))
		{
	        s = "select cast( "+colNames[i]+" as char (8) for bit data ) from "+table;
		}
            else
                s = "select cast( "+colNames[i]+" as "+typeName+typeSpec+" ) from "+table;
            Xprint(s);
        }
    }
	public void testBlobInsert() {

		System.out.println("\n\n---< BLOB Insertion Tests\n");
        // create table for testing
        {
            Xprint("create table blobCheck (bl blob(80)) ");
        }

		// test insertion of literals.
		for (int i=0; i < columns; i++) {

			if (colTypes[i].indexOf("blob") == -1)
				continue;

			// Check char literals.
			// (fail)
            String insert = "insert into blobCheck (bl" +
				" ) values ('string' )";
			Xprint(insert);
			// (succeed)
            insert = "insert into blobCheck (bl" +
				" ) values (cast (" +
				TestUtil.stringToHexLiteral("string") +
				" as blob(80)) )";
			Xprint(insert);
			// Check bit literals.
			// (fail)
            insert = "insert into blobCheck (bl" +
				" ) values (X'48' )";
			Xprint(insert);
			// old CS compatible value:  ( b'01001' )
			// (succeed)
            insert = "insert into blobCheck (bl" +
				" ) values (cast (X'C8' as blob(80)) )";
			Xprint(insert);
			// Check hex literals.
			// (fail)
            insert = "insert into blobCheck (bl" +
				" ) values ( X'a78a' )";
			Xprint(insert);
			// (succeed)
            insert = "insert into blobCheck (bl" +
				" ) values (cast (X'a78a' as blob(80)) )";
			Xprint(insert);
		}
            Xprint("drop table blobCheck");
	}
    public void test() throws SQLException {
        // create table for testing
        {
            String create = "create table "+table+" ( dummy int ";
            for(int i=0; i<columns; i++) {
                create += ", "+colNames[i]+" "+colTypes[i];
            }
            create += " )";
            Xprint(create);              //st.execute(create);
        }
        // insert one row of numbers in string format if possible.
        {
            String insert = "insert into "+table+" values ( 45 ";
				for(int i=0; i<columns; i++) {
					insert += "," + colData[i] ;
				}
            insert += " )";
            Xprint(insert);
        }

        // insert various data in various columns, some will fail (int)
        {
            for(int i=0; i<columns; i++) {
                String insert = "insert into "+table+" ( "+colNames[i];

				if (isBitColumn(i))
				// have to cast for blob columns.
	{
					insert += " ) values cast ( " +
						TestUtil.stringToHexLiteral("true") +
						"  AS " + colTypes[i] + ")";
	}
				else
                    insert += " ) values ( 'true' )";
                Xprint(insert);
            }
        }

        // run tests
        testGetters();
        testMetaData();
        testCastFrom();
        testCastTo();

        // cleanup
        Xprint("drop table "+table); //st.execute("drop table "+table);
    }

	private boolean isBitColumn(int offset)
	{
		return  ((offset == BLOB_OFFSET) ||
				 (offset == BIT_OFFSET) ||
				 (offset == LONG_VARBINARY_OFFSET) ||
				 (offset == TYPE_COL_OFFSET)
				 );
	}
}
