/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.XplainStatisticsTest

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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder; 
import javax.xml.parsers.DocumentBuilderFactory; 

import org.xml.sax.InputSource;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.impl.tools.planexporter.AccessDatabase;
import org.apache.derby.impl.tools.planexporter.CreateXMLFile;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.XML;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

/**
 * This suite contains a set of tests for the new XPLAIN style of
 * runtime statistics capturing which was added as part of DERBY-2487
 * and also a set of tests within tests generated under DERBY-2487
 * are added as part of DERBY-4587-PlanExporter tool.
 *
 * There are a large number of tests which follow the pattern of:
 * - enable capture
 * - execute a single statement
 * - stop capture
 * - verify that the right stuff was captured
 *
 * FIXME -- some general to-do items that I don't want to forget:
 * - should resultSetNumber be its own column in sysxplain_resultsets?
 * - need MORE tests of xplain-only mode
 * - need a test of external sorting/merging
 * - need to cross-check the result set types, and verify that they're
 *   all tested at least once
 */
public class XplainStatisticsTest extends BaseJDBCTestCase {
	
	public XplainStatisticsTest(String name) {
		super(name);
	}
	
	public static Test suite() {
            timeSuiteStarted = (new Date()).getTime();
            TestSuite allTests = new TestSuite(XplainStatisticsTest.class,
                                    "XplainStatisticsTest");
            
            Test test = allTests;
            test = new SupportFilesSetup(test); //added by DERBY-4587
            
		return new CleanDatabaseTestSetup(test) {
			protected void decorateSQL(Statement s)
				throws SQLException
			{
				createSchemaObjects(s);
			}
		};
	}
        private static long timeSuiteStarted;

    @Override
    protected void setUp() throws SQLException {
        // DERBY-6306: Some of the statistics, especially those that give
        // information about compilation timing, are sensitive to whether
        // preparing a statement in fact compiles the statement or if it
        // just fetches it from the statement cache. Empty the statement
        // cache between each test case, so that they don't need to worry
        // about statements lying around from earlier test cases affecting
        // the results.
        emptyStatementCache();
    }

	/**
	 * Creates a variety of tables used by the various tests.
         * The tests use the same basic schema as the 'toursdb' sample
         * database, with a much smaller set of data. We really only
         * populate the COUNTRIES table; the other tables have few or no rows.
         *
	 * @throws SQLException
	 */
	private static void createSchemaObjects(Statement st)
		throws SQLException
	{
		st.executeUpdate(
"CREATE TABLE AIRLINES (" +
"      AIRLINE CHAR(2) NOT NULL ," +
"      AIRLINE_FULL VARCHAR(24)," +
"      BASIC_RATE DOUBLE PRECISION," +
"      DISTANCE_DISCOUNT DOUBLE PRECISION," +
"      BUSINESS_LEVEL_FACTOR DOUBLE PRECISION," +
"      FIRSTCLASS_LEVEL_FACTOR DOUBLE PRECISION," +
"      ECONOMY_SEATS INTEGER," +
"      BUSINESS_SEATS INTEGER," +
"      FIRSTCLASS_SEATS INTEGER)");
		st.executeUpdate(
"ALTER TABLE AIRLINES" +
"   ADD CONSTRAINT AIRLINES_PK Primary Key (AIRLINE)");
		st.executeUpdate(
"CREATE TABLE COUNTRIES (" +
"      COUNTRY VARCHAR(26) NOT NULL," +
"      COUNTRY_ISO_CODE CHAR(2) NOT NULL ," +
"      REGION VARCHAR(26))");
		st.executeUpdate(
"ALTER TABLE COUNTRIES" +
"   ADD CONSTRAINT COUNTRIES_PK Primary Key (COUNTRY_ISO_CODE)");
		st.executeUpdate(
"ALTER TABLE COUNTRIES" +
"   ADD CONSTRAINT COUNTRIES_UNQ_NM Unique (COUNTRY)");
		st.executeUpdate(
"CREATE TABLE CITIES (" +
"      CITY_ID INTEGER NOT NULL ," +
"      CITY_NAME VARCHAR(24) NOT NULL," +
"	COUNTRY VARCHAR(26) NOT NULL," +
"	AIRPORT VARCHAR(3)," +
"	LANGUAGE  VARCHAR(16)," +
"      COUNTRY_ISO_CODE CHAR(2) )");
		st.executeUpdate(
"ALTER TABLE CITIES" +
"   ADD CONSTRAINT CITIES_PK Primary Key (CITY_ID)");
		st.executeUpdate(
"ALTER TABLE CITIES" +
"   ADD CONSTRAINT COUNTRIES_FK Foreign Key (COUNTRY_ISO_CODE)" +
"   REFERENCES COUNTRIES (COUNTRY_ISO_CODE)");
		st.executeUpdate(
"CREATE TABLE FLIGHTS (" +
"      FLIGHT_ID CHAR(6) NOT NULL ," +
"      SEGMENT_NUMBER INTEGER NOT NULL ," +
"      ORIG_AIRPORT CHAR(3)," +
"      DEPART_TIME TIME," +
"      DEST_AIRPORT CHAR(3)," +
"      ARRIVE_TIME TIME," +
"      MEAL CHAR(1)," +
"      FLYING_TIME DOUBLE PRECISION," +
"      MILES INTEGER," +
"      AIRCRAFT VARCHAR(6))");
		st.executeUpdate(
"CREATE INDEX DESTINDEX ON FLIGHTS (DEST_AIRPORT) ");
		st.executeUpdate(
"CREATE INDEX ORIGINDEX ON FLIGHTS (ORIG_AIRPORT) ");
		st.executeUpdate(
"ALTER TABLE FLIGHTS" +
"   ADD CONSTRAINT FLIGHTS_PK Primary Key (FLIGHT_ID, SEGMENT_NUMBER)");
		st.executeUpdate(
"CREATE TABLE FLIGHTAVAILABILITY (" +
"      FLIGHT_ID CHAR(6) NOT NULL ," +
"      SEGMENT_NUMBER INTEGER NOT NULL ," +
"      FLIGHT_DATE DATE NOT NULL ," +
"      ECONOMY_SEATS_TAKEN INTEGER DEFAULT 0," +
"      BUSINESS_SEATS_TAKEN INTEGER DEFAULT 0," +
"      FIRSTCLASS_SEATS_TAKEN INTEGER DEFAULT 0)");
		st.executeUpdate(
"ALTER TABLE FLIGHTAVAILABILITY" +
"   ADD CONSTRAINT FLIGHTAVAIL_PK Primary Key " +
"       (FLIGHT_ID, SEGMENT_NUMBER, FLIGHT_DATE)");
		st.executeUpdate(
"ALTER TABLE FLIGHTAVAILABILITY" +
"   ADD CONSTRAINT FLIGHTS_FK2 Foreign Key (FLIGHT_ID, SEGMENT_NUMBER)" +
"   REFERENCES FLIGHTS (FLIGHT_ID, SEGMENT_NUMBER)");
		st.executeUpdate(
"CREATE TABLE MAPS (" +
"      MAP_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY " +
"             (START WITH 1, INCREMENT BY 1)," +
"      MAP_NAME VARCHAR(24) NOT NULL," +
"      REGION VARCHAR(26)," +
"      AREA DECIMAL(8,4) NOT NULL," +
"      PHOTO_FORMAT VARCHAR(26) NOT NULL," +
"      PICTURE BLOB(102400)," +
"      UNIQUE (MAP_ID, MAP_NAME))");
		st.executeUpdate(
"CREATE TABLE FLIGHTS_HISTORY (" +
"      FLIGHT_ID CHAR(6)," +
"      SEGMENT_NUMBER INTEGER," +
"      ORIG_AIRPORT CHAR(3)," +
"      DEPART_TIME TIME," +
"      DEST_AIRPORT CHAR(3)," +
"      ARRIVE_TIME TIME," +
"      MEAL CHAR(1)," +
"      FLYING_TIME DOUBLE PRECISION," +
"      MILES INTEGER," +
"      AIRCRAFT VARCHAR(6), " +
"      STATUS VARCHAR (20))");

		st.executeUpdate(
"insert into FLIGHTS values ('AA1111',1,'ABQ','09:00:00','LAX','09:19:00','S',1.328,664,'B747')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1112',1,'LAX','09:00:00','ABQ','11:19:00','S',1.328,664,'B747')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1113',1,'ABQ','09:00:00','PHX','09:39:00','S',0.658,329,'B747')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1114',1,'PHX','09:00:00','ABQ','09:39:00','S',0.658,329,'B747')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1115',1,'ABQ','09:00:00','OKC','11:02:00','B',1.034,517,'B747')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1116',1,'OKC','09:00:00','ABQ','09:02:00','B',1.034,517,'B747')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1117',1,'AKL','09:00:00','HNL','18:48:00','L',8.804,4402,'B747')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1118',1,'HNL','13:30:00','AKL','21:18:00','D',8.804,4402,'DC10')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1119',1,'AKL','09:00:00','NRT','15:59:00','L',10.996,5498,'B747')"); 
		st.executeUpdate(
"insert into FLIGHTS values ('AA1120',1,'NRT','09:00:00','AKL','23:59:00','L',10.996,5498,'B747')"); 

		st.executeUpdate(
"insert into COUNTRIES values ( 'Afghanistan','AF','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ( 'Albania','AL','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Algeria','DZ','North Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('American Samoa','AS','Pacific Islands')");
		st.executeUpdate(
"insert into COUNTRIES values ('Angola','AO','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Argentina','AR','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Armenia','AM','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Australia','AU','Australia and New Zealand')");
		st.executeUpdate(
"insert into COUNTRIES values ('Austria','AT','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Azerbaijan','AZ','Central Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Bahamas','BS','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Bangladesh','BD','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Barbados','BB','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Belgium','BE','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Belize','BZ','Central America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Bermuda','BM','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Bolivia','BO','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Botswana','BW','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Brazil','BR','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Bulgaria','BG','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Cambodia','KH','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Cameroon','CM','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Canada','CA','North America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Cape Verde','CV','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Chile','CL','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('China','CN','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Colombia','CO','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Congo','CG','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Costa Rica','CR','Central America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Cote d''Ivoire','CI','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Cuba','CU','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Czech Republic','CZ','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Denmark','DK','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Dominical Republic','DO','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Ecuador','EC','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Egypt','EG','North Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('El Salvador','SV','Central America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Ethiopia','ET','North Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Falkland Islands','FK','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Fiji','FJ','Pacific Islands')");
		st.executeUpdate(
"insert into COUNTRIES values ('Finland','FI','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('France','FR','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Georgia','GE','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Germany','DE','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Ghana','GH','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Greece','GR','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Guadeloupe','GP','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Guatemala','GT','Central America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Honduras','HN','Central America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Hungary','HU','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Iceland','IS','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('India','IN','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Indonesia','ID','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Iran','IR','Middle East')");
		st.executeUpdate(
"insert into COUNTRIES values ('Iraq','IQ','Middle East')");
		st.executeUpdate(
"insert into COUNTRIES values ('Ireland','IE','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Israel','IL','Middle East')");
		st.executeUpdate(
"insert into COUNTRIES values ('Italy','IT','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Jamaica','JM','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Japan','JP','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Jordan','JO','Middle East')");
		st.executeUpdate(
"insert into COUNTRIES values ('Kenya','KE','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Korea, Republic of','KR','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Lebanon','LB','Middle East')");
		st.executeUpdate(
"insert into COUNTRIES values ('Lithuania','LT','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Madagascar','MG','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Malaysia','MY','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Mali','ML','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Mexico','MX','North America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Morocco','MA','North Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Mozambique','MZ','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Nepal','NP','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Netherlands','NL','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('New Zealand','NZ','Australia and New Zealand')");
		st.executeUpdate(
"insert into COUNTRIES values ('Nicaragua','NI','Central America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Nigeria','NG','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Norway','NO','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Pakistan','PK','Central Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Paraguay','PY','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Peru','PE','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Philippines','PH','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Poland','PL','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Portugal','PT','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Russia','RU','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Samoa','WS','Pacific Islands')");
		st.executeUpdate(
"insert into COUNTRIES values ('Senegal','SN','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Sierra Leone','SL','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Singapore','SG','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Slovakia','SK','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('South Africa','ZA','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Spain','ES','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Sri Lanka','LK','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Sudan','SD','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Sweden','SE','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Switzerland','CH','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Syrian Arab Republic','SY','Middle East')");
		st.executeUpdate(
"insert into COUNTRIES values ('Tajikistan','TJ','Central Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Tanzania','TZ','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Thailand','TH','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Trinidad and Tobago','TT','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Tunisia','TN','North Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Turkey','TR','Middle East')");
		st.executeUpdate(
"insert into COUNTRIES values ('Ukraine','UA','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('United Kingdom','GB','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('United States','US','North America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Uruguay','UY','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Uzbekistan','UZ','Central Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Venezuela','VE','South America')");
		st.executeUpdate(
"insert into COUNTRIES values ('Viet Nam','VN','Asia')");
		st.executeUpdate(
"insert into COUNTRIES values ('Virgin Islands (British)','VG','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Virgin Islands (U.S.)','VI','Caribbean')");
		st.executeUpdate(
"insert into COUNTRIES values ('Yugoslavia','YU','Europe')");
		st.executeUpdate(
"insert into COUNTRIES values ('Zaire','ZR','Africa')");
		st.executeUpdate(
"insert into COUNTRIES values ('Zimbabwe','ZW','Africa')");

                // This table 't' sets up a case where you can visit a
                // deleted row via 'select x from t'
		st.executeUpdate(
"create table t (x int not null primary key, y char(250))");
		st.executeUpdate(
"insert into t values (1, 'a'), (2,'b'), (3,'c'), (4,'d')");
		st.executeUpdate(
"delete from t where x = 3");
	}
	
    private static boolean hasTable(Statement s,
                                    String schemaName, String tableName)
        throws SQLException
    {
        ResultSet rs = s.getConnection().getMetaData().getTables((String)null,
                schemaName, tableName,  new String[] {"TABLE"});
        boolean tableFound = rs.next();
        rs.close();
        return tableFound;
    }
    
    private static String []tableNames = {
        "SYSXPLAIN_STATEMENTS",
        "SYSXPLAIN_STATEMENT_TIMINGS",
        "SYSXPLAIN_RESULTSETS",
        "SYSXPLAIN_RESULTSET_TIMINGS",
        "SYSXPLAIN_SORT_PROPS",
        "SYSXPLAIN_SCAN_PROPS",
    };
    
    private static void enableXplainStyle(Statement s)
            throws SQLException
    {
        verifyXplainUnset(s);
        for (int i = 0; i < tableNames.length; i++)
            if (hasTable(s, "XPLTEST", tableNames[i]))
                s.execute("delete from XPLTEST." + tableNames[i]);
        s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        s.execute("call syscs_util.syscs_set_xplain_schema('XPLTEST')");
        s.execute("call syscs_util.syscs_set_xplain_mode(0)");
    }
    
    private static void enableXplainStyleWithTiming(Statement s)
            throws SQLException
    {
        enableXplainStyle(s);
        s.execute("call syscs_util.syscs_set_statistics_timing(1)");
    }
    
    private static void enableXplainOnlyMode(Statement s)
            throws SQLException
    {
        s.execute("call syscs_util.syscs_set_xplain_mode(1)");
    }
    
    private static void clearXplainOnlyMode(Statement s)
            throws SQLException
    {
        s.execute("call syscs_util.syscs_set_xplain_mode(0)");
    }
    
    /**
     * 
     * @param s
     * @throws Exception
     */
    private static void disableXplainStyle(Statement s)
    throws Exception
    {
    	s.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");

    	/*
    	 * Added by DERBY-4587 to test the generation of XML files
    	 * from PlanExporter tool.
    	 */
    	String stmt_id="";
    	ResultSet rs;
    	AccessDatabase access;

    	rs = s.executeQuery( 
    			"select stmt_id from XPLTEST.sysxplain_statements"); 
    	while (rs.next()) 
    	{ 
    		stmt_id = rs.getString(1); 
    		access = 
    			new AccessDatabase(s.getConnection(), "XPLTEST", stmt_id);
    		if(access.initializeDataArray()){ 
    			access.createXMLFragment();
    			access.markTheDepth();

    			CreateXMLFile xml_file = new CreateXMLFile(access); 
    			xml_file.writeTheXMLFile(
    					access.statement(),
    					access.time(),
    					access.getData(), 
    					SupportFilesSetup.getReadWriteURL(stmt_id + ".xml")
    					.getPath(),
    					null);
    		}
    	} 
        s.execute("call syscs_util.syscs_set_xplain_schema('')");
    }

    private static void verifyXplainUnset(Statement s)
        throws SQLException
    {
    	JDBC.assertFullResultSet(
            s.executeQuery("values SYSCS_UTIL.syscs_get_xplain_schema()"),
            new String[][]{{""}});
    	JDBC.assertFullResultSet(
            s.executeQuery("values SYSCS_UTIL.syscs_get_xplain_mode()"),
            new String[][]{{"0"}});
    }
    
    private void verifyNonNullDRDA_ID(Statement s)
        throws SQLException
    {
        ResultSet rs;
        rs = s.executeQuery("select drda_id from xpltest.sysxplain_statements");
        while (rs.next())
        {
            String drda_id = rs.getString("DRDA_ID");
            if (rs.wasNull() || drda_id == null || drda_id.trim().length() == 0)
                fail("While running in a network-client configuration, " +
                        "DRDA_ID was null or blank.");
        }
        rs.close();
    }

    // Can be used internally when diagnosing failed tests.
    //
    private void dumpResultSets(Statement s)
        throws SQLException
    {
        ResultSet rs;
        rs = s.executeQuery("select * from xpltest.sysxplain_resultsets");
        while (rs.next())
        {
            System.out.println(
                    rs.getString("rs_id")+","+
                    rs.getString("op_identifier")+","+
                    rs.getString("op_details")+","+
                    rs.getString("no_opens")+","+
                    rs.getString("no_index_updates")+","+
                    rs.getString("lock_mode")+","+
                    rs.getString("lock_granularity")+","+
                    rs.getString("parent_rs_id")+","+
                    rs.getString("est_row_count")+","+
                    rs.getString("est_cost")+","+
                    rs.getString("affected_rows")+","+
                    rs.getString("deferred_rows")+","+
                    rs.getString("input_rows")+","+
                    rs.getString("seen_rows")+","+
                    rs.getString("seen_rows_right")+","+
                    rs.getString("filtered_rows")+","+
                    rs.getString("returned_rows")+","+
                    rs.getString("empty_right_rows")+","+
                    rs.getString("index_key_opt")+","+
                    rs.getString("scan_rs_id")+","+
                    rs.getString("sort_rs_id")+","+
                    rs.getString("stmt_id")+","+
                    rs.getString("timing_id"));
        }
        rs.close();
    }
    
    private void dumpStatements(Statement s)
        throws SQLException
    {
        ResultSet rs;
        rs = s.executeQuery("select * from xpltest.sysxplain_statements");
        while (rs.next())
        {
            System.out.println(
                    rs.getString("stmt_id")+","+
                    rs.getString("stmt_name")+","+
                    rs.getString("stmt_type")+","+
                    rs.getString("stmt_text")+","+
                    rs.getString("jvm_id")+","+
                    rs.getString("os_identifier")+","+
                    rs.getString("xplain_mode")+","+
                    rs.getString("xplain_time")+","+
                    rs.getString("xplain_thread_id")+","+
                    rs.getString("transaction_id")+","+
                    rs.getString("session_id")+","+
                    rs.getString("database_name")+","+
                    rs.getString("drda_id")+","+
                    rs.getString("timing_id"));
        }
        rs.close();
    }
    
    /**
     * Added by DERBY-4587
     * Returns the stmt_id for this particular statement
     * @param s Statement
     * @return stmt_id
     * */
    private String getStmtID(Statement s) throws SQLException{
    	ResultSet rs;
        String stmt_id;
        rs = s.executeQuery( 
    		"select stmt_id from XPLTEST.sysxplain_statements"); 
        rs.next();
        stmt_id = rs.getString(1); 
        rs.close();
        return stmt_id;
    }
    
    /**
     * Added by DERBY-4587
     * Returns the stmt_id for this particular statement
     * @param s Statement
     * @return stmt_id
     * */
    private String[] getStmtIDArray(Statement s, int length) throws SQLException{
        ResultSet rs;
        String[] stmt_id = new String[length];
        int i=0;
        rs = s.executeQuery(
            "select stmt_id from XPLTEST.sysxplain_statements");
        while(rs.next()){
            stmt_id[i] = rs.getString(1);
            i++;
            if (i==length)
                assertFalse(rs.next());
        }
        rs.close();
        return stmt_id;
    }

    /**
     * Added by DERBY-4587
     * @param file name of the XML file - without extension
     * @return a Document object
     * @throws Exception
     */
    private Object getADocument(final String file) throws Exception {
    	Document document;
    	DocumentBuilderFactory factory =
    		DocumentBuilderFactory.newInstance();
    	DocumentBuilder builder = factory.newDocumentBuilder();

    	InputSource xml = new InputSource(
    			AccessController.doPrivileged
    			(new java.security.PrivilegedExceptionAction<InputStream>(){
    				public InputStream run()throws Exception{
    					return new FileInputStream(
    							SupportFilesSetup.getReadWriteURL(file+".xml").getPath()
    					);}})
    	);

    	document = builder.parse(xml);
    	document.getDocumentElement().normalize();

    	return document;
    }

    /**
     * Added by DERBY-4587
     * gets the &lt;statement&gt; element from the XML
     * @param file XML file
     * @return statement mentioned in the XML
     * @throws Exception
     */
    private String readStatement(final String file)
    throws Exception
    {
        Document document = (Document) getADocument(file);

    	return document.getElementsByTagName("statement").item(0).getChildNodes().item(0).getNodeValue();

    }

    /**
     * Added by DERBY-4587
     * count the # of &lt;node&gt; elements
     * @param file XML file
     * @return node count
     * @throws Exception
     */
    private int countNode(final String file) throws Exception{
        Document document = (Document) getADocument(file);

    	return document.getElementsByTagName("node").getLength();
    }

    /**
     * Added by DERBY-4587
     * gets the "name" attributes of all nodes separated by a "|"
     * @param file XML file 
     * @return "name" attribute's value of all nodes
     * 			eg: "PROJECTION|TABLESCAN|"
     * @throws Exception
     */
    private String getNodeName(final String file) throws Exception{
        Document document = (Document) getADocument(file);
    	NodeList lst=document.getElementsByTagName("node");
    	String name= "";
    	for(int i=0;i<lst.getLength();i++)
    		name+=lst.item(i).getAttributes().getNamedItem("name").getNodeValue()+"|";
    	return name;
    }

    /**
     * Added by DERBY-4587
     * gets attributes other than "name"
     * @param file XML file
     * @param attribute name of the attribute
     * @param node node which this attribute belongs to
     * @return if attribute doesn't exist ""
     * 			else value of the attribute
     * @throws Exception
     */
    private String getNodeAttribute(final String file, String attribute, int node) throws Exception{
    	Document document = (Document) getADocument(file);
    	NodeList lst=document.getElementsByTagName("node");
    	if(lst.item(node).getAttributes().getNamedItem(attribute)==null)
    		return "";
    	return lst.item(node).getAttributes().getNamedItem(attribute).getNodeValue();
    }

    public void testPlanExporterSchemaExistence()
	throws Exception
    {
	AccessDatabase access = 
    		new AccessDatabase(getConnection(), "NoSuchSchema", "nostmt"); 
	assertTrue( "Unexpectedly thought schema exists",
		! access.verifySchemaExistance() );
    }

    public void testPlanExporterIllegalFileAccess()
	throws Exception
    {
	AccessDatabase access = 
    		new AccessDatabase(getConnection(), "NoSuchSchema", "nostmt"); 
    	CreateXMLFile xml_file = new CreateXMLFile(access); 
	try
	{
    		xml_file.writeTheXMLFile("nostmt", "notime", null,
			"/illegal.xml", null);
		fail("Expected exception for illegal file access");
	}
	catch (java.security.AccessControlException ace)
	{
		// Expected this exception to be thrown
	}
	catch (Exception e)
	{
		e.printStackTrace();
		fail(e.getMessage());
	}
    }

    /**
     * Verify that the plan exporter doesn't choke on special characters in
     * the name of the XPLAIN schema. Regression test case for DERBY-4904.
     */
    public void testPlanExporterSpecialCharactersInSchema() throws Exception {
        String schema = "DERBY-4904 \"double\" and 'single' quoted schema";
        String query = "select * from sysibm.sysdummy1";

        PreparedStatement setStats = prepareStatement(
                "call syscs_util.syscs_set_runtimestatistics(?)");
        PreparedStatement setXplain = prepareStatement(
                "call syscs_util.syscs_set_xplain_schema(?)");

        // Enable XPLAIN statistics in a schema with special characters.
        setStats.setInt(1, 1);
        setStats.execute();
        setXplain.setString(1, schema);
        setXplain.execute();

        // Execute a statement while XPLAIN is enabled.
        Statement s = createStatement();
        JDBC.assertSingleValueResultSet(
                s.executeQuery(query),
                "Y");

        // Disable XPLAIN.
        setStats.setInt(1, 0);
        setStats.execute();
        setXplain.setString(1, "");
        setXplain.execute();

        // Get the XPLAIN statistics.
        ResultSet rs = s.executeQuery(
                "select stmt_id, stmt_text from " +
                JDBC.escape(schema, "SYSXPLAIN_STATEMENTS"));
        assertTrue(rs.next());
        String stmtId = rs.getString(1);
        assertEquals(query, rs.getString(2));
        assertFalse(rs.next());
        rs.close();

        // Create the XML file. This used to result in a syntax error.
        AccessDatabase access =
                new AccessDatabase(getConnection(), schema, stmtId);
        assertTrue(access.initializeDataArray());
        access.createXMLFragment();
        access.markTheDepth();
        CreateXMLFile create = new CreateXMLFile(access);
        create.writeTheXMLFile(
                access.statement(),
                access.time(),
                access.getData(),
                SupportFilesSetup.getReadWriteURL(stmtId + ".xml").getPath(),
                null);

        // If we have the required libraries for parsing XML files, verify
        // that the output contains valid data.
        if (XML.classpathMeetsXMLReqs()) {
            assertEquals(query, readStatement(stmtId));
        }
    }

    /**
     * Tests that invalidation of a statement after compile-time doesn't result
     * in duplicate entries in the XPLAIN-table(s).
     * <p>
     * This test is timing-dependent, and may not trigger the bug under all
     * circumstances.
     * <p>
     * See DERBY-4849.
     *
     * @throws Exception if something goes wrong
     */
    public void testSimpleQueryMultiWithInvalidation()
            throws Exception {
        // Start two threads; one selecting from COUNTRIES with XPLAIN on, and
        // one generating statistics for the same table.
        // The latter thread should cause the statement executed in the former
        // thread to be invalidated.
        long runTime = 10*1000; // Run for 10 seconds
        AbstractMTThread select = new MTSimpleSelect(
                openDefaultConnection(), runTime);
        AbstractMTThread invalidate = new MTSimpleInvalidate(
                openDefaultConnection(), runTime);
        Thread tInv = new Thread(invalidate);
        Thread tSel = new Thread(select);
        tInv.start();
        tSel.start();

        // Wait until the threads have finished.
        tInv.join();
        tSel.join();

        // Check if any errors were raised in the worker-threads.
        int selects = select.getActionCount();
        int invalidations = invalidate.getActionCount();
        println("selects=" + selects + ", invalidations=" + invalidations);
        if (select.failed()) {
            fail("select-thread failed", select.getError());
        }
        if (invalidate.failed()) {
            fail("invalidate-thread failed", invalidate.getError());
        }

        // There should be as many entries in the XPLAIN-table as the number
        // of times we have executed the select-statement.
        Statement s = createStatement();
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_statements"),
            Integer.toString(selects));
    }

    /**
     * Verify that XPLAIN style captures basic statistics and timings.
     *
     * This test runs
     *
     *   SELECT Country FROM Countries WHERE Region = 'Central America'
     *
     * and verifies that there are some reasonable values captured
     * into the XPLAIN system tables.
     * @throws IOException 
     * @throws PrivilegedActionException 
     * @throws MalformedURLException 
     */
    public void testSimpleQuery() throws Exception
    {
        Statement s = createStatement();

        enableXplainStyleWithTiming(s);

        String selectStatement = 
            "SELECT country from countries WHERE region = 'Central America'";
        JDBC.assertUnorderedResultSet(
                s.executeQuery(selectStatement),
            new String[][] {  {"Belize"}, {"Costa Rica"}, {"El Salvador"},
                {"Guatemala"}, {"Honduras"}, {"Nicaragua"} } );

        disableXplainStyle(s);
        
        // The statement should have been executed as a PROJECTION
        // wrapped around a TABLESCAN. The TABLESCAN should have had
        // scan properties. The TABLESCAN should have filtered 114 rows
        // down to 6 rows, and the PROJECTION should have no parent RS.
        //
        // THe XPLAIN system tables should contain the following rows:
        // STATEMENTS: 1 row, 
        // STATEMENT_TIMINGS: 1 row
        // RESULTSETS: 2 rows
        // RESULTSET_TIMINGS: 2 rows
        // SCAN_PROPS: 1 row
        // SORT PROPS: no rows
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_statements"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_statement_timings"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "2");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultset_timings"), "2");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_scan_props"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_sort_props"), "0");

        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_statements " +
            " where stmt_text like '%from countries%' "), "1");
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier from xpltest.sysxplain_resultsets"),
                new String[][] { {"PROJECTION"}, {"TABLESCAN"} } );
        // Statement type is 'S' for Select, Xplain_mode is 'F' for FULL
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select stmt_type, stmt_text, xplain_mode " +
                    " from xpltest.sysxplain_statements"),
                new String[][] { {"S",selectStatement, "F"} } );
        if (! usingDerbyNetClient())
            JDBC.assertSingleValueResultSet(s.executeQuery(
                "select drda_id from xpltest.sysxplain_statements"), null);
        else
            verifyNonNullDRDA_ID(s);

        // We should have opened the inner TABLESCAN resultset once, and
        // it should have seen 6 rows.
        JDBC.assertSingleValueResultSet(s.executeQuery(
                    "select no_opens from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'TABLESCAN'"), "1");
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select no_opens, seen_rows, returned_rows, " +
                    "       lock_mode, lock_granularity " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'TABLESCAN'"),
                new String[][] { {"1", "6", "6", "IS", "R"} } );
        // The TABLESCAN should have scanned the COUNTRIES heap using
        // read-committed isolation, and it should have visited 114 rows
        // on 2 pages and qualified 6 of those rows.
        // Columns {0, 2} should have been fetched.
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select sp.scan_object_name, sp.scan_object_type, " +
                    "sp.scan_type, sp.isolation_level, " +
                    "sp.no_visited_rows, sp.no_qualified_rows, "+
                    "sp.no_visited_pages, " +
                    "sp.no_fetched_columns, sp.bitset_of_fetched_columns " +
                    "from xpltest.sysxplain_scan_props sp " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on sp.scan_rs_id = rs.scan_rs_id " +
                    "where rs.op_identifier='TABLESCAN'"),
                new String[][] { {"COUNTRIES", "T", "HEAP",
                    "RC", "114", "6", "2", "2", "{0, 2}"} } );
        // Verify that the ID values can be used to join the rows:
        JDBC.assertSingleValueResultSet(s.executeQuery(
                    "select sp.no_visited_rows " +
                    "from xpltest.sysxplain_scan_props sp, " +
                    "     xpltest.sysxplain_resultsets rs, " +
                    "     xpltest.sysxplain_statements st " +
                    "where st.stmt_id = rs.stmt_id and " +
                    "     rs.scan_rs_id = sp.scan_rs_id and " +
                    "     rs.op_identifier = 'TABLESCAN' and " +
                    "     sp.scan_object_name = 'COUNTRIES'"),
                "114");
        
        verifySensibleStatementTimings(s);
        verifySensibleResultSetTimings(s);
        
        /*
         * This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         * */
        
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(2,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("PROJECTION|TABLESCAN|", 
        			getNodeName(stmt_id));

        	//for TABLESCAN node test whether the scan related entries
        	//are exist and verify their values.
        	Assert.assertEquals("COUNTRIES", 
        			getNodeAttribute(stmt_id,"scanned_object",1));
        	Assert.assertEquals("HEAP", 
        			getNodeAttribute(stmt_id,"scan_type",1));
        	Assert.assertEquals("2", 
        			getNodeAttribute(stmt_id,"visited_pages",1));
        	
        }
    }
    
    private String getStmtIDByName(Statement s, String sName)
			throws SQLException
    {
    	ResultSet rs;
        String stmt_id = "?";
        rs = s.executeQuery( 
    		"select stmt_id from XPLTEST.sysxplain_statements "+
		"where stmt_name='"+sName+"'"); 
        if( rs.next() )
	{
            stmt_id = rs.getString(1); 
	    //System.out.println("Found statemnt id " + stmt_id);
	}
        rs.close();
        return stmt_id;
    }
    public void testSimpleXplainOnly() throws Exception
    {
        Statement s = createStatement();

        enableXplainStyle(s);
        enableXplainOnlyMode(s);
	s.setCursorName("1");
        JDBC.assertEmpty(s.executeQuery(
        	"SELECT country from countries "+
	         "WHERE region = 'Central America'" ));
        clearXplainOnlyMode(s);
        disableXplainStyle(s);

	// dumpStatements(s);
        // dumpResultSets(s);
	// There should be 1 statement captured with stmt_id='1'.
	// It should have a PROJECTION and a TABLESCAN; the TABLESCAN should be
	// on the COUNTRIES table.
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_statements"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_statements "+
	    "where stmt_name='1'"), "1");
	String stmt_id = getStmtIDByName( s, "1" );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets "+
	    "where stmt_id='"+stmt_id+"'"), "2");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets "+
	    "where stmt_id='"+stmt_id+"' and op_identifier='PROJECTION'"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets "+
	    "where stmt_id='"+stmt_id+"' and op_identifier='TABLESCAN'"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select op_details from xpltest.sysxplain_resultsets "+
	    "where stmt_id='"+stmt_id+"' and op_identifier='TABLESCAN'"),
	    "T: COUNTRIES");

        enableXplainStyle(s);
        enableXplainOnlyMode(s);
	s.setCursorName("2");
        JDBC.assertEmpty(s.executeQuery(
            "select sql_text from syscs_diag.transaction_table " +
	    "where status != 'IDLE'" ));
        clearXplainOnlyMode(s);
        disableXplainStyle(s);

	//dumpStatements(s);
        //dumpResultSets(s);

	// This statement should have three result sets:
	// - PROJECTION (select sql_text)
	// - PROJECT-FILTER (where status != IDLE)
	// - VTI (syscs_diag.transaction_table)
	//
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_statements"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_statements "+
	    "where stmt_name='2'"), "1");
	stmt_id = getStmtIDByName( s, "2" );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets "+
	    "where stmt_id='"+stmt_id+"' and op_identifier='PROJECTION'"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets "+
	    "where stmt_id='"+stmt_id+"' and "+
	    "      op_identifier='PROJECT-FILTER'"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets "+
	    "where stmt_id='"+stmt_id+"' and op_identifier='VTI'"), "1");

        String selectStatement = 
            "select region, count(country) from app.countries group by region";
        enableXplainStyle(s);
        enableXplainOnlyMode(s);
	s.setCursorName("3");
        JDBC.assertEmpty(s.executeQuery(selectStatement));

        clearXplainOnlyMode(s);
        disableXplainStyle(s);
	//dumpStatements(s);
        //dumpResultSets(s);

        // This statement is executed as a PROJECTION with a child GROUPBY
        // with a child PROJECTION with a child TABLESCAN. The TABLESCAN
        // has a corresponding SCAN_PROPS row, the GROUPBY has a
        // corresponding SORT_PROPS row. But since we're XPLAIN-ONLY, none
	// of the actual processing did anything.
        //
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "4");
        JDBC.assertFullResultSet(s.executeQuery(
                    "select op_identifier from xpltest.sysxplain_resultsets " +
                    "order by op_identifier"),
            new String[][] {
                {"GROUPBY"},{"PROJECTION"},{"PROJECTION"},{"TABLESCAN"} } );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "where scan_rs_id is not null"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "where sort_rs_id is not null"), "1");
        JDBC.assertFullResultSet(s.executeQuery(
                    "select s.stmt_text, rs.op_identifier," +
                    " srt.no_input_rows, srt.no_output_rows " +
                    " from xpltest.sysxplain_sort_props srt, " +
                    " xpltest.sysxplain_resultsets rs, " +
                    " xpltest.sysxplain_statements s " +
                    " where rs.stmt_id = s.stmt_id and " +
                    " rs.sort_rs_id = srt.sort_rs_id"),
            new String[][] {
                {selectStatement, "GROUPBY", "0", "0"} } );

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select srt.sort_type, srt.no_input_rows, " +
                    " srt.no_output_rows, srt.no_merge_runs, " +
                    " srt.merge_run_details, srt.eliminate_duplicates, " +
                    " srt.in_sort_order, srt.distinct_aggregate " +
                    "from xpltest.sysxplain_sort_props srt " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on srt.sort_rs_id = rs.sort_rs_id " +
                    "where rs.op_identifier='GROUPBY'"),
                new String[][] {
                    {"IN","0","0",null, null, null,"N","N"} } );
    }

    public void testXplainOnlyExecutePrepared() throws Exception
    {
        Statement s = createStatement();

        String selectStatement = 
            "select region, count(country) from app.countries group by region";
        PreparedStatement ps = prepareStatement( selectStatement );
        enableXplainStyle(s);
        enableXplainOnlyMode(s);
        JDBC.assertEmpty(ps.executeQuery());
        clearXplainOnlyMode(s);
        disableXplainStyle(s);
	//dumpStatements(s);
        //dumpResultSets(s);

        // This statement is executed as a PROJECTION with a child GROUPBY
        // with a child PROJECTION with a child TABLESCAN. The TABLESCAN
        // has a corresponding SCAN_PROPS row, the GROUPBY has a
        // corresponding SORT_PROPS row. But since we're XPLAIN-ONLY, none
	// of the actual processing did anything.
        //
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "4");
        JDBC.assertFullResultSet(s.executeQuery(
                    "select op_identifier from xpltest.sysxplain_resultsets " +
                    "order by op_identifier"),
            new String[][] {
                {"GROUPBY"},{"PROJECTION"},{"PROJECTION"},{"TABLESCAN"} } );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "where scan_rs_id is not null"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "where sort_rs_id is not null"), "1");
        JDBC.assertFullResultSet(s.executeQuery(
                    "select s.stmt_text, rs.op_identifier," +
                    " srt.no_input_rows, srt.no_output_rows " +
                    " from xpltest.sysxplain_sort_props srt, " +
                    " xpltest.sysxplain_resultsets rs, " +
                    " xpltest.sysxplain_statements s " +
                    " where rs.stmt_id = s.stmt_id and " +
                    " rs.sort_rs_id = srt.sort_rs_id"),
            new String[][] {
                {selectStatement, "GROUPBY", "0", "0"} } );

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select srt.sort_type, srt.no_input_rows, " +
                    " srt.no_output_rows, srt.no_merge_runs, " +
                    " srt.merge_run_details, srt.eliminate_duplicates, " +
                    " srt.in_sort_order, srt.distinct_aggregate " +
                    "from xpltest.sysxplain_sort_props srt " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on srt.sort_rs_id = rs.sort_rs_id " +
                    "where rs.op_identifier='GROUPBY'"),
                new String[][] {
                    {"IN","0","0",null, null, null,"N","N"} } );

	
	// Since now we're not in XplainOnly mode, the prepared statement
	// returns the expected normal result set.

        JDBC.assertUnorderedResultSet(ps.executeQuery(),
            new String[][] { 
                {"Africa", "19"}, {"Asia", "15"},
                {"Australia and New Zealand", "2"}, {"Caribbean", "10"},
                {"Central America", "6"}, {"Central Asia", "4"},
                {"Europe", "29"}, {"Middle East", "7"},
                {"North Africa", "5"}, {"North America", "3"},
                {"Pacific Islands", "3"}, {"South America", "11"} } );

	// And then back to empty again:
        enableXplainStyle(s);
        enableXplainOnlyMode(s);
        JDBC.assertEmpty(ps.executeQuery());
        clearXplainOnlyMode(s);
        disableXplainStyle(s);

        // Verify that statistics were collected.
        JDBC.assertDrainResults(
                s.executeQuery("select * from xpltest.sysxplain_statements"),
                1);
    }
    
    public void testXplainOnlyPrepared() throws Exception
    {
        Statement s = createStatement();

        String selectStatement = 
            "select region, count(country) from app.countries group by region";

        // Try preparing the statement while we're in xplain-only mode, then
        // execute it normally.

        enableXplainStyle(s);
        enableXplainOnlyMode(s);
        PreparedStatement ps2 = prepareStatement( selectStatement );
        clearXplainOnlyMode(s);
        JDBC.assertUnorderedResultSet(ps2.executeQuery(),
            new String[][] { 
                {"Africa", "19"}, {"Asia", "15"},
                {"Australia and New Zealand", "2"}, {"Caribbean", "10"},
                {"Central America", "6"}, {"Central Asia", "4"},
                {"Europe", "29"}, {"Middle East", "7"},
                {"North Africa", "5"}, {"North America", "3"},
                {"Pacific Islands", "3"}, {"South America", "11"} } );
        disableXplainStyle(s);
	//dumpStatements(s);
        //dumpResultSets(s);

        // This statement is executed as a PROJECTION with a child GROUPBY
        // with a child PROJECTION with a child TABLESCAN. The TABLESCAN
        // has a corresponding SCAN_PROPS row, the GROUPBY has a
        // corresponding SORT_PROPS row. But since we're XPLAIN-ONLY, none
	// of the actual processing did anything.
        //
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "4");
        JDBC.assertFullResultSet(s.executeQuery(
                    "select op_identifier from xpltest.sysxplain_resultsets " +
                    "order by op_identifier"),
            new String[][] {
                {"GROUPBY"},{"PROJECTION"},{"PROJECTION"},{"TABLESCAN"} } );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "where scan_rs_id is not null"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "where sort_rs_id is not null"), "1");
        JDBC.assertFullResultSet(s.executeQuery(
                    "select s.stmt_text, rs.op_identifier," +
                    " srt.no_input_rows, srt.no_output_rows " +
                    " from xpltest.sysxplain_sort_props srt, " +
                    " xpltest.sysxplain_resultsets rs, " +
                    " xpltest.sysxplain_statements s " +
                    " where rs.stmt_id = s.stmt_id and " +
                    " rs.sort_rs_id = srt.sort_rs_id"),
            new String[][] {
                {selectStatement, "GROUPBY", "114", "12"} } );

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select srt.sort_type, srt.no_input_rows, " +
                    " srt.no_output_rows, srt.no_merge_runs, " +
                    " srt.merge_run_details, srt.eliminate_duplicates, " +
                    " srt.in_sort_order, srt.distinct_aggregate " +
                    "from xpltest.sysxplain_sort_props srt " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on srt.sort_rs_id = rs.sort_rs_id " +
                    "where rs.op_identifier='GROUPBY'"),
                new String[][] {
                    {"IN","114","12",null, null, null,"N","N"} } );
    }
    
    
    /**
      * Make some basic first-order checks on the STATEMENT_TIMINGS rows.
      *
      * This method runs through the rows in the STATEMENT_TIMINGS table
      * and makes some very high-level reasonableness checks:
      *
      * 1) The values for PARSE_TIME, BIND_TIME, OPTIMIZE_TIME,
      *    GENERATE_TIME, COMPILE_TIME, and EXECUTE_TIME should all
      *    be non-negative integer values.
      * 2) The values of PARSE_TIME, BIND_TIME, OPTIMIZE_TIME, and
      *    GENERATE_TIME, summed together, should equal the value
      *    of COMPILE_TIME.
      * 3) The values of BEGIN_COMP_TIME, END_COMP_TIME, BEGIN_EXE_TIME,
      *    and END_EXE_TIME should all be valid times, and should all
      *    be greater than the time when we started running these tests.
      * 4) END_COMP_TIME should be G.E. BEGIN_COMP_TIME,
      *    BEGIN_EXE_TIME should be G.E. END_COMP_TIME, and
      *    END_EXE_TIME should be G.E. BEGIN_EXE_TIME
      */
    private void verifySensibleStatementTimings(Statement s)
        throws SQLException
    {
        ResultSet rs = s.executeQuery(
                "select * from xpltest.sysxplain_statement_timings");
        while (rs.next())
        {
            long parseTime = getNonNegativeLong(rs, "PARSE_TIME");
            long bindTime = getNonNegativeLong(rs, "BIND_TIME");
            long optimizeTime = getNonNegativeLong(rs, "OPTIMIZE_TIME");
            long generateTime = getNonNegativeLong(rs, "GENERATE_TIME");
            long compileTime = getNonNegativeLong(rs, "COMPILE_TIME");
            long executeTime = getNonNegativeLong(rs, "EXECUTE_TIME");

            // Due to rounding errors, this is not always exact? I think that
            // the largest rounding error should be 2, because there are
            // 4 sub-elements which each may be rounded off by 0.5 millis?
            //
            // So we'll accept a difference of 1 or 2 in the overall comp time
            //
            long compTimeRoundingError = 
                compileTime - (parseTime+bindTime+optimizeTime+generateTime);
            if (compTimeRoundingError < 0 || compTimeRoundingError > 2)
                assertEquals("compilation time did not compute (" +
                    parseTime+","+bindTime+","+optimizeTime+","+
                    generateTime+")", 
                compileTime, (parseTime+bindTime+optimizeTime+generateTime));

            Timestamp beginCompTime=getNonNullTimestamp(rs, "BEGIN_COMP_TIME");
            Timestamp endCompTime = getNonNullTimestamp(rs, "END_COMP_TIME");
            Timestamp beginExeTime = getNonNullTimestamp(rs, "BEGIN_EXE_TIME");
            Timestamp endExeTime = getNonNullTimestamp(rs, "END_EXE_TIME");

            if (endCompTime.before(beginCompTime))
                fail("END_COMP_TIME " + endCompTime +
                    " unexpectedly before BEGIN_COMP_TIME " + beginCompTime);
            if (beginExeTime.before(endCompTime))
                fail("BEGIN_EXE_TIME " + beginExeTime +
                    " unexpectedly before END_COMP_TIME " + endCompTime);
            if (endExeTime.before(beginExeTime))
                fail("END_EXE_TIME " + endExeTime +
                    " unexpectedly before BEGIN_EXE_TIME " + beginExeTime);
        }
        rs.close();
    }

    /**
      * Make some basic first-order checks on the RESULTSET_TIMINGS rows.
      *
      * This method runs through the rows in the RESULTSET_TIMINGS table
      * and makes some very simple checks for reasonableness of the values:
      *
      * 1) For each row in RESULTSETS, there should be a row in 
      *    RESULTSET_TIMINGS
      * 2) There should be non-negative values for CONSTRUCTOR_TIME, OPEN_TIME,
      *    NEXT_TIME, CLOSE_TIME, and EXECUTE_TIME.
      * 3) If the result set has a non-zero value for RETURNED_ROWS, then
      *    there should be a non-negative value for AVG_NEXT_TIME_PER_ROW;
      *    conversely if the result set did not have any returned rows, then
      *    AVG_NEXT_TIME_PER_ROW should be null.
      * 4) If the result set is a PROJECTION, then PROJECTION_TIME and
      *    RESTRICTION_TIME should have valid non-negative values;
      *    otherwise those columns should be NULL.
      * 5) If the result set is a MATERIALIZE, then TEMP_CONG_CREATE_TIME
      *    and TEMP_CONG_FETCH_TIME should have valid non-negative values;
      *    otherwise those columns should be null.
      */
    private void verifySensibleResultSetTimings(Statement s)
        throws SQLException
    {
        ResultSet rs = s.executeQuery(
                "select rs.op_identifier, rs.returned_rows, rt.* " +
                "from xpltest.sysxplain_resultsets rs left outer join " +
                "     xpltest.sysxplain_resultset_timings rt " +
                "on rs.timing_id = rt.timing_id");

        while (rs.next())
        {
            String opIdentifier = rs.getString("OP_IDENTIFIER");
            // Since we performed a LEFT OUTER JOIN, the TIMING_ID will be
            // NULL if the resultset is missing a timings row.
            String timingId = rs.getString("TIMING_ID");
            assertNotNull("RESULTSET row missing for " + opIdentifier,
                    timingId);

            getNonNegativeLong(rs, "CONSTRUCTOR_TIME");
            getNonNegativeLong(rs, "OPEN_TIME");
            getNonNegativeLong(rs, "NEXT_TIME");
            getNonNegativeLong(rs, "CLOSE_TIME");
            // FIXME -- EXECUTE_TIME is unexpectedly negative for various
            // result sets.
            // getNonNegativeLong(rs, "EXECUTE_TIME");

            long returnedRows = rs.getLong("RETURNED_ROWS");
            if (returnedRows > 0)
            {
                getNonNegativeLong(rs, "AVG_NEXT_TIME_PER_ROW");
            }
            else
            {
                long avgNextTimePerRow = rs.getLong("AVG_NEXT_TIME_PER_ROW");
                assertTrue("Expected NULL for avg-next on rs " + opIdentifier,
                        rs.wasNull());
            }
            if (opIdentifier.equals("PROJECTION"))
            {
                getNonNegativeLong(rs, "PROJECTION_TIME");
                getNonNegativeLong(rs, "RESTRICTION_TIME");
            }
            else
            {
                rs.getLong("PROJECTION_TIME");
                assertTrue("Expected NULL PROJECTION_TIME for " + opIdentifier,
                        rs.wasNull());
                rs.getLong("RESTRICTION_TIME");
                assertTrue("Expected NULL RESTRICTION_TIME for " + opIdentifier,
                        rs.wasNull());
            }
            if (opIdentifier.equals("MATERIALIZE"))
            {
                getNonNegativeLong(rs, "TEMP_CONG_CREATE_TIME");
                getNonNegativeLong(rs, "TEMP_CONG_FETCH_TIME");
            }
            else
            {
                rs.getLong("TEMP_CONG_CREATE_TIME");
                assertTrue("Expected NULL TEMP_CONG_CREATE_TIME for " +
                    opIdentifier, rs.wasNull());
                rs.getLong("TEMP_CONG_FETCH_TIME");
                assertTrue("Expected NULL TEMP_CONG_FETCH_TIME for " +
                    opIdentifier, rs.wasNull());
            }
        }
    }

    private long getNonNegativeLong(ResultSet rs, String cName)
        throws SQLException
    {
        long result = rs.getLong(cName);
        assertTrue(cName + " unexpectedly NULL", ! rs.wasNull() );
        assertTrue(cName + " unexpectedly negative(" + result + ")",
                result >= 0);
        return result;
    }

    private Timestamp getNonNullTimestamp(ResultSet rs, String cName)
        throws SQLException
    {
        Timestamp result = rs.getTimestamp(cName);
        assertTrue(cName + " unexpectedly NULL", ! rs.wasNull() );
        assertNotNull(cName + " unexpectedly NULL", result);
        assertTrue("Test started at " + timeSuiteStarted +
                " but " + cName + " value is " + result.getTime(),
                result.getTime() >= timeSuiteStarted);
        return result;
    }

        /**
          * Verify XPLAIN style handling of an INDEX scan.
          *
          * This test runs a query against FLIGHTS using the dest_airport
          * index, and verifies the captured query plan statistics.
          */
    public void testIndexScan() throws Exception
    {
        Statement s = createStatement();

        enableXplainStyleWithTiming(s);

        String selectStatement = 
            "SELECT flight_id from flights where dest_airport = 'ABQ'";
        JDBC.assertUnorderedResultSet(s.executeQuery(selectStatement),
            new String[][] {  {"AA1112"}, {"AA1114"}, {"AA1116"} } );

        disableXplainStyle(s);

        // This query should have been executed as a PROJECTION whose child
        // is a ROWIDSCAN whose child is an INDEXSCAN. The INDEXSCAN should
        // be linked to a BTREE SCAN_PROPS row against the DESTINDEX index.
        // The index scan should have visited 1 page and 4 rows, and should
        // have qualified 3 of those rows, fetching 2 (ALL) the columns
        // from those rows.
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier from xpltest.sysxplain_resultsets"),
            new String[][] { {"PROJECTION"}, {"ROWIDSCAN"}, {"INDEXSCAN"} } );
        JDBC.assertSingleValueResultSet(s.executeQuery(
                    "select p.parent_rs_id " +
                    " from xpltest.sysxplain_resultsets p, " +
                    "      xpltest.sysxplain_resultsets r, " +
                    "      xpltest.sysxplain_resultsets i  " +
                    " where p.rs_id = r.parent_rs_id and " +
                    "       r.rs_id = i.parent_rs_id and " +
                    "       p.op_identifier='PROJECTION' and " +
                    "       r.op_identifier='ROWIDSCAN' and " +
                    "       i.op_identifier='INDEXSCAN'"),
                null);
        // All 3 resultsets rows should join to the same statements row.
        JDBC.assertSingleValueResultSet(s.executeQuery(
                    "select count(rs.op_identifier) " +
                    " from xpltest.sysxplain_statements st " +
                    " join xpltest.sysxplain_resultsets rs " +
                    "   on st.stmt_id = rs.stmt_id"),
                "3");
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select sp.scan_object_name, sp.scan_object_type, " +
                    "sp.scan_type, sp.isolation_level, " +
                    "rs.lock_mode, rs.lock_granularity, " +
                    "sp.no_visited_rows, sp.no_qualified_rows, "+
                    "sp.no_visited_pages, sp.btree_height, sp.fetch_size, " +
                    "sp.no_fetched_columns, sp.bitset_of_fetched_columns " +
                    "from xpltest.sysxplain_scan_props sp " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on sp.scan_rs_id = rs.scan_rs_id " +
                    "where rs.op_identifier='INDEXSCAN'"),
                new String[][] { {"DESTINDEX", "I", "BTREE",
                    "RC", "IS", "R",
                    "4", "3", "1", "-1", "16", "2", "ALL"} } );
        verifySensibleStatementTimings(s);
        verifySensibleResultSetTimings(s);
        
        
        /* This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(3,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("PROJECTION|ROWIDSCAN|INDEXSCAN|", 
        			getNodeName(stmt_id));

        	//for INDEXSCAN node test whether the scan related entries
        	//are exist and verify their values.
        	Assert.assertEquals("DESTINDEX", 
        			getNodeAttribute(stmt_id,"scanned_object",2));
        	Assert.assertEquals("BTREE", 
        			getNodeAttribute(stmt_id,"scan_type",2));
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"visited_pages",2));
        }
    }

        /**
          * Verify XPLAIN style handling of a CONSTRAINT scan.
          *
          * This test runs a query against COUNTRIES using the
          * COUNTRY_ISO_CODE constraint,
          * and verifies the captured query plan statistics.
          */
    public void testConstraintScan() throws Exception
    {
        Statement s = createStatement();

        enableXplainStyle(s);
        String selectStatement = 
            "SELECT region from countries where country = 'Cameroon'";
        JDBC.assertSingleValueResultSet(s.executeQuery(selectStatement),
                "Africa");
        disableXplainStyle(s);
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier from xpltest.sysxplain_resultsets"),
            new String[][] {
                {"PROJECTION"}, {"ROWIDSCAN"}, {"CONSTRAINTSCAN"} } );
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select sp.scan_object_name, sp.scan_object_type, " +
                    "sp.scan_type, sp.isolation_level, " +
                    "sp.no_visited_rows, sp.no_qualified_rows, "+
                    "sp.no_visited_pages, " +
                    "rs.lock_mode, rs.lock_granularity, " +
                    "sp.no_fetched_columns, sp.bitset_of_fetched_columns " +
                    "from xpltest.sysxplain_scan_props sp " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on sp.scan_rs_id = rs.scan_rs_id " +
                    "where rs.op_identifier='CONSTRAINTSCAN'"),
                new String[][] { {"COUNTRIES_UNQ_NM", "C", "BTREE",
                    "RC", "1", "1", "1", "SH", "R", "2", "ALL"} } );
        
        /*
         * This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);
        	
        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(3,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("PROJECTION|ROWIDSCAN|CONSTRAINTSCAN|", 
        			getNodeName(stmt_id));

        	//for CONSTRAINTSCAN node test whether the scan related entries
        	//are exist and verify their values.
        	Assert.assertEquals("COUNTRIES_UNQ_NM", 
        			getNodeAttribute(stmt_id,"scanned_object",2));
        	Assert.assertEquals("BTREE", 
        			getNodeAttribute(stmt_id,"scan_type",2));
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"visited_pages",2));
        }
    }

    /**
      * Verify XPLAIN style handling of sort properties.
      *
      * This test runs a simple query against the COUNTRIES table and
      * verifies that reasonable values are captured for the sort properties.
      */
    public void testGroupBySortProps()
        throws Exception
    {
        Statement s = createStatement();

        enableXplainStyleWithTiming(s);
        String selectStatement = 
            "select region, count(country) from countries group by region";
        JDBC.assertUnorderedResultSet(s.executeQuery(selectStatement),
            new String[][] { 
                {"Africa", "19"}, {"Asia", "15"},
                {"Australia and New Zealand", "2"}, {"Caribbean", "10"},
                {"Central America", "6"}, {"Central Asia", "4"},
                {"Europe", "29"}, {"Middle East", "7"},
                {"North Africa", "5"}, {"North America", "3"},
                {"Pacific Islands", "3"}, {"South America", "11"} } );

        disableXplainStyle(s);

        // This statement is executed as a PROJECTION with a child GROUPBY
        // with a child PROJECTION with a child TABLESCAN. The TABLESCAN
        // has a corresponding SCAN_PROPS row, the GROUPBY has a
        // corresponding SORT_PROPS row. In this test, we're mostly
        // interested in the values in the SORT_PROPS row.
        //
        // The sort groups the 114 input rows into 12 groups. The rows
        // are not provided in sort order as the input, and the sort
        // is not a distinct aggregate.
        //
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "4");
        JDBC.assertFullResultSet(s.executeQuery(
                    "select op_identifier from xpltest.sysxplain_resultsets " +
                    "order by op_identifier"),
            new String[][] {
                {"GROUPBY"},{"PROJECTION"},{"PROJECTION"},{"TABLESCAN"} } );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "where scan_rs_id is not null"), "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "where sort_rs_id is not null"), "1");
        JDBC.assertFullResultSet(s.executeQuery(
                    "select s.stmt_text, rs.op_identifier," +
                    " srt.no_input_rows, srt.no_output_rows " +
                    " from xpltest.sysxplain_sort_props srt, " +
                    " xpltest.sysxplain_resultsets rs, " +
                    " xpltest.sysxplain_statements s " +
                    " where rs.stmt_id = s.stmt_id and " +
                    " rs.sort_rs_id = srt.sort_rs_id"),
            new String[][] {
                {selectStatement, "GROUPBY", "114", "12"} } );

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select srt.sort_type, srt.no_input_rows, " +
                    " srt.no_output_rows, srt.no_merge_runs, " +
                    " srt.merge_run_details, srt.eliminate_duplicates, " +
                    " srt.in_sort_order, srt.distinct_aggregate " +
                    "from xpltest.sysxplain_sort_props srt " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on srt.sort_rs_id = rs.sort_rs_id " +
                    "where rs.op_identifier='GROUPBY'"),
                new String[][] {
                    {"IN","114","12",null, null, null,"N","N"} } );
        verifySensibleStatementTimings(s);
        verifySensibleResultSetTimings(s);
        
        
        /* * This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(4,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("PROJECTION|GROUPBY|PROJECTION|TABLESCAN|", 
        			getNodeName(stmt_id));

        	//for GROUPBY node test whether the sort related entries
        	//are exist and verify their values.
        	Assert.assertEquals("IN", 
        			getNodeAttribute(stmt_id,"sort_type",1));
        	Assert.assertEquals("12", 
        			getNodeAttribute(stmt_id,"sorter_output",1));
        }
    }

    /**
      * Verify XPLAIN style handling of DISTINCT_AGGREGATE sort properties.
      *
      * This test runs a query which involves a distinct aggreagte and
      * verifies that the DISTINCT_AGGREGATE field in SORT_PROPS gets set.
      */
    public void testDistinctAggregateSortProps()
        throws Exception
    {
        Statement s = createStatement();

        enableXplainStyleWithTiming(s);
        String selectStatement = 
            "select orig_airport, avg(distinct miles) from flights " +
            "group by orig_airport";
        // Execute the statement and throw away the results. We just want
        // to look at the statistics.
        s.executeQuery(selectStatement).close();
        disableXplainStyle(s);

        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_sort_props"), "1");

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select srt.sort_type, srt.no_input_rows, " +
                    " srt.no_output_rows, srt.no_merge_runs, " +
                    " srt.merge_run_details, srt.eliminate_duplicates, " +
                    " srt.in_sort_order, srt.distinct_aggregate " +
                    "from xpltest.sysxplain_sort_props srt " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on srt.sort_rs_id = rs.sort_rs_id " +
                    "where rs.op_identifier='GROUPBY'"),
                new String[][] {
                    {"IN","10","10",null, null, null,"N","Y"} } );
        verifySensibleStatementTimings(s);
        verifySensibleResultSetTimings(s);
        
        
         /** This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	// This statement is executed as a PROJECTION with a child GROUPBY
        	// with a child PROJECTION with a child TABLESCAN. 

        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(4,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("PROJECTION|GROUPBY|PROJECTION|TABLESCAN|", 
        			getNodeName(stmt_id));

        	//for GROUPBY node test whether the sort related entries
        	//are exist and verify their values.
        	Assert.assertEquals("IN", 
        			getNodeAttribute(stmt_id,"sort_type",1));
        	Assert.assertEquals("10", 
        			getNodeAttribute(stmt_id,"sorter_output",1));
        }
    }
    /**
      * A simple test of an AGGREGATION result set.
      */
    public void testAggregationResultSet()
        throws Exception
    {
        Statement s = createStatement();
        enableXplainStyle(s);
        String selectStatement = 
            "select count(distinct region) from countries";
        JDBC.assertSingleValueResultSet(s.executeQuery(selectStatement), "12");
        disableXplainStyle(s);

        // The above statement results in the query execution:
        // PROJECTION(AGGREGATION(PROJECTION(TABLESCAN)))
        //
        // In this test case, we are interested in verifying that the
        // content of the AGGREGATION resultset row is reasonable.

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'AGGREGATION'"),
                new String[][] { {"AGGREGATION", "DISTINCT", "1"} } );
        //
        // FIXME -- why are INPUT_ROWS, SEEN_ROWS, FILTERED_ROWS, and
        // RETURNED_ROWS apparently meaningless for an AGGREGATION RS?
        
        
         /** This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(4,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("PROJECTION|AGGREGATION|PROJECTION|TABLESCAN|", 
        			getNodeName(stmt_id));

        	//for AGGREGATION node test no_opens entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"no_opens",1));
        }
        
    }

    /**
      * A simple test of an INSERT result set.
      */
    public void testInsertResultSet()
        throws Exception
    {
        Statement s = createStatement();
        // Make sure we don't have the tuple to be inserted already:
        s.executeUpdate("delete from AIRLINES"); 
        enableXplainStyle(s);
        String insertStatement = 
            "insert into AIRLINES values " +
            "('AA','Amazonian Airways',0.18,0.03,0.5,1.5,20,10,5)";
        int numRows = s.executeUpdate(insertStatement);
        disableXplainStyle(s);
        assertEquals("Failed to insert into AIRLINES", 1, numRows);
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select stmt_type, stmt_text " +
                    " from xpltest.sysxplain_statements"),
                new String[][] { {"I",insertStatement} } );
        //
        // The above INSERT statement results in the query execution:
        // INSERT(NORMALIZE(ROW))
        //
        // Verify some of the basic contents of the SYSXPLAIN_RESULTSETS
        // rows for those result sets.
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "3");
        // FIXME -- why is lock_mode NULL? Shouldn't it be IX?
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, " +
                    "       no_index_updates, lock_mode, " +
                    "       lock_granularity, parent_rs_id, " +
                    "       affected_rows, deferred_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'INSERT'"),
                new String[][] {
                    {"INSERT",null,"1",null,"R", null,"1","N"} } );
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       seen_rows, filtered_rows, returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'NORMALIZE'"),
                new String[][] {
                    {"NORMALIZE",null,"1","1","0","1"} } );
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       seen_rows, filtered_rows, returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'ROW'"),
                new String[][] {
                    {"ROW",null,"1","0","0","1"} } );
        
        
        /* * This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			insertStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(3,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("INSERT|NORMALIZE|ROW|", 
        			getNodeName(stmt_id));

        	//for ROW node, test no_opens entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"no_opens",1));
        	
        	//for NORMALIZE node, test returned_rows entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"returned_rows",1));
        }
    }

    /**
      * A simple test of an UPDATE result set.
      */
    public void testUpdateResultSet()
        throws Exception
    {
        Statement s = createStatement();
        s.executeUpdate("delete from AIRLINES");
        String insertStatement = 
            "insert into AIRLINES values " +
            "('AA','Amazonian Airways',0.18,0.03,0.5,1.5,20,10,5)";
        int numRows = s.executeUpdate(insertStatement);
        assertEquals("Failed to insert into AIRLINES", 1, numRows);
        String updateStatement = 
            "update AIRLINES set economy_seats=23,business_seats=7 " +
            " where airline='AA'";
        enableXplainStyle(s);
        numRows = s.executeUpdate(updateStatement);
        assertEquals("Failed to update AIRLINES", 1, numRows);
        disableXplainStyle(s);
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select stmt_type, stmt_text " +
                    " from xpltest.sysxplain_statements"),
                new String[][] { {"U",updateStatement} } );
        //
        // The above UPDATE statement results in the query execution:
        // UPDATE(PROJECTION(ROWIDSCAN(CONSTRAINTSCAN)))
        // The CONSTRAINTSCAN has a SCAN_PROPS associated with it.
        //
        // Verify some of the basic contents of the SYSXPLAIN_RESULTSETS
        // rows for those result sets.
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "4");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select op_identifier from xpltest.sysxplain_resultsets " +
            " where scan_rs_id is not null"), "CONSTRAINTSCAN");
        // FIXME -- shouldn't lock_mode be 'IX'?
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       no_index_updates, lock_mode, " +
                    "       lock_granularity, parent_rs_id, " +
                    "       affected_rows, deferred_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'UPDATE'"),
                new String[][] {
                    {"UPDATE",null,null,"0",null,"R",null,"1","N"} } );
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       seen_rows, filtered_rows, returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'PROJECTION'"),
                new String[][] {
                    {"PROJECTION","2;","1","1","0","1"} } );
        ResultSet myRS = s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       seen_rows, filtered_rows, returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'ROWIDSCAN'");
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       seen_rows, filtered_rows, returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'ROWIDSCAN'"),
                new String[][] {
                    {"ROWIDSCAN","(0),AIRLINES","1","1","0","1"} } );
        //dumpResultSets(s);
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       lock_mode, lock_granularity, " +
                    "       seen_rows, filtered_rows, returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'CONSTRAINTSCAN'"),
                new String[][] {
                    {"CONSTRAINTSCAN","C: AIRLINES_PK",
                        "1","EX","R","1","0","1"} } );
        // Verify that the CONSTRAINTSCAN is linked to a type=C scan_props
        // row, which should have visited 1 row on 1 page, fetching 2 (ALL)
        // columns, with non-null start_position, stop_position, and
        // scan_qualifiers set to 'None'.
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select sp.scan_object_name, sp.scan_object_type, " +
                    "sp.scan_type, sp.isolation_level, " +
                    "sp.no_visited_rows, sp.no_qualified_rows, "+
                    "sp.no_visited_pages, sp.no_visited_deleted_rows, " +
                    "sp.no_fetched_columns, sp.bitset_of_fetched_columns, " +
                    "sp.scan_qualifiers " +
                    "from xpltest.sysxplain_scan_props sp " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on sp.scan_rs_id = rs.scan_rs_id " +
                    "where rs.op_identifier='CONSTRAINTSCAN'"),
                new String[][] {
                    {"AIRLINES_PK", "C", "BTREE", "RC",
                        "1", "1", "1", "0", "2", "ALL", "None"} } );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_scan_props " +
            "  where start_position is not null " +
            "    and stop_position is not null"), "1");
        
        
        
         /** This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			updateStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(4,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("UPDATE|PROJECTION|ROWIDSCAN|CONSTRAINTSCAN|", 
        			getNodeName(stmt_id));

        	//for UPDATE node, test no_opens entry.
        	//Since it's null, there should be no such entry.
        	Assert.assertEquals("", 
        			getNodeAttribute(stmt_id,"no_opens",0));
        	
        	//for PROJECTION node, test returned_rows entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"returned_rows",1));
        	
        	//for PROJECTION node, test no_opens entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"no_opens",1));
        	
        	//for ROWIDSCAN node, test returned_rows entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"returned_rows",2));
        	
        	//for ROWIDSCAN node, test no_opens entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"no_opens",2));
        	
        	//for CONSTRAINTSCAN node, test scan_qualifiers entry
        	//is exist and verify its value.
        	Assert.assertEquals("None", 
        			getNodeAttribute(stmt_id,"scan_qualifiers",3));
        	
        	//for CONSTRAINTSCAN node, test visited_pages entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"visited_pages",3));
        }
    }

    /**
      * A simple test of an DELETE result set.
      */
    public void testDeleteResultSet()
        throws Exception
    {
        Statement s = createStatement();
        s.executeUpdate("delete from AIRLINES");
        String insertStatement = 
            "insert into AIRLINES values " +
            "('AA','Amazonian Airways',0.18,0.03,0.5,1.5,20,10,5)";
        int numRows = s.executeUpdate(insertStatement);
        assertEquals("Failed to insert into AIRLINES", 1, numRows);
        String deleteStatement = "delete from airlines where airline='AA'";
        enableXplainStyle(s);
        numRows = s.executeUpdate(deleteStatement);
        assertEquals("Failed to delete from AIRLINES", 1, numRows);
        disableXplainStyle(s);
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select stmt_type, stmt_text " +
                    " from xpltest.sysxplain_statements"),
                new String[][] { {"D",deleteStatement} } );
        //
        // The above DELETE statement results in the query execution:
        // DELETE(PROJECTION(PROJECTION(CONSTRAINTSCAN)))
        // The CONSTRAINTSCAN has a SCAN_PROPS associated with it.
        //
        // We basically just check the DELETE result set, since we've
        // checked the other result sets in other test cases.
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "4");
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select distinct op_identifier " +
                    " from xpltest.sysxplain_resultsets " +
                    " order by op_identifier"),
                new String[][] {
                    {"CONSTRAINTSCAN"}, {"DELETE"}, {"PROJECTION"} } );
        // FIXME -- shouldn't lock_mode be 'IX'?
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       no_index_updates, lock_mode, " +
                    "       lock_granularity, parent_rs_id, " +
                    "       affected_rows, deferred_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'DELETE'"),
                new String[][] {
                    {"DELETE",null,null,"1",null,"R",null,"1","N"} } );
        
        
         /** This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			deleteStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(4,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("DELETE|PROJECTION|PROJECTION|CONSTRAINTSCAN|", 
        			getNodeName(stmt_id));

        	//for DELETE node, test no_opens entry.
        	//Since it's null, there should be no such entry.
        	Assert.assertEquals("", 
        			getNodeAttribute(stmt_id,"no_opens",0));
        }
    }

    /**
      * A simple test of a SORT result set.
      */
    public void testSortResultSet()
        throws Exception
    {
        Statement s = createStatement();
        enableXplainStyle(s);
        String selectStatement = 
            "select region from countries order by country";
        s.executeQuery(selectStatement).close(); // Discard the results
        disableXplainStyle(s);

        // The above statement results in the query execution:
        // PROJECTION(SORT(PROJECTION(TABLESCAN)))
        //
        // In this test case, we are interested in verifying that the
        // content of the SORT resultset row and the content of
        // the corresponding SORT_PROPS row are reasonable.

        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "4");
        // FIXME -- why is RETURNED_ROWS 0 for this sort? Shouldn't it be 114?
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       input_rows, seen_rows, filtered_rows, " +
                    "       returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'SORT'"),
                new String[][] {
                    {"SORT", null, "1","114","0","0","0"} } );
        JDBC.assertFullResultSet(s.executeQuery(
                    "select s.stmt_text, rs.op_identifier," +
                    " srt.no_input_rows, srt.no_output_rows, " +
                    " srt.sort_type, srt.eliminate_duplicates, " +
                    " srt.in_sort_order, srt.distinct_aggregate " +
                    " from xpltest.sysxplain_sort_props srt, " +
                    " xpltest.sysxplain_resultsets rs, " +
                    " xpltest.sysxplain_statements s " +
                    " where rs.stmt_id = s.stmt_id and " +
                    " rs.sort_rs_id = srt.sort_rs_id"),
            new String[][] {
                {selectStatement, "SORT", "114", "114", "IN", "N", "N", null}
            } );
        
        
        
         /** This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         **/ 
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(4,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals("PROJECTION|SORT|PROJECTION|TABLESCAN|", 
        			getNodeName(stmt_id));

        	//for SORT node test whether the sort related entries
        	//are exist and verify their values.
        	Assert.assertEquals("IN", 
        			getNodeAttribute(stmt_id,"sort_type",1));
        	Assert.assertEquals("114", 
        			getNodeAttribute(stmt_id,"sorter_output",1));
        	Assert.assertEquals("114", 
        			getNodeAttribute(stmt_id,"input_rows",1));
        }
    }

    /**
      * A simple test of a UNION query.
      */
    public void testUnionQuery()
        throws Exception
    {
        Statement s = createStatement();
        enableXplainStyle(s);
        String selectStatement = 
            "select country from countries where region = 'Central America' "+
            " union " +
            "select country from countries where region = 'Africa'";
        s.executeQuery(selectStatement).close(); // Discard the results
        disableXplainStyle(s);

        // The above statement results in the query execution:
        // SORT(UNION(PROJECTION(TABLESCAN),PROJECTION(TABLESCAN)))
        //
        // Note that the UNION resultset has TWO child result sets.
        //
        // There is also 1 SORT_PROPS row, for the top-level SORT, and
        // 2 SCAN_PROPS rows, for the two TABLESCAN nodes.
        //
        // We verify the overall structure of the result set nodes, and
        // spot check a few of the fields.

        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "6");
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets " +
            "  where parent_rs_id = " +
            "        (select rs_id from xpltest.sysxplain_resultsets" +
            "         where op_identifier = 'UNION')"), "2");
        JDBC.assertUnorderedResultSet(s.executeQuery(
            "select op_identifier from xpltest.sysxplain_resultsets " +
            "where scan_rs_id is not null"),
                new String[][] { {"TABLESCAN"}, {"TABLESCAN"} } );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select op_identifier from xpltest.sysxplain_resultsets " +
            " where sort_rs_id is not null and parent_rs_id is null"),
                "SORT");

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       seen_rows, seen_rows_right, filtered_rows, " +
                    "       returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'UNION'"),
                new String[][] {
                    {"UNION", "(2)", "1", "6","19","0","25"} } );
        JDBC.assertFullResultSet(s.executeQuery(
                    "select s.stmt_text, rs.op_identifier," +
                    " srt.no_input_rows, srt.no_output_rows, " +
                    " srt.sort_type, srt.eliminate_duplicates, " +
                    " srt.in_sort_order, srt.distinct_aggregate " +
                    " from xpltest.sysxplain_sort_props srt, " +
                    " xpltest.sysxplain_resultsets rs, " +
                    " xpltest.sysxplain_statements s " +
                    " where rs.stmt_id = s.stmt_id and " +
                    " rs.sort_rs_id = srt.sort_rs_id"),
            new String[][] {
                {selectStatement, "SORT", "25", "25", "IN", "Y", "N", null}
            } );
        
        
         /** This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(6,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals(
        			"SORT|UNION|PROJECTION|TABLESCAN|PROJECTION|TABLESCAN|", 
        			getNodeName(stmt_id));

        	//for SORT node test whether the sort related entries
        	//are exist and verify their values.
        	Assert.assertEquals("IN", 
        			getNodeAttribute(stmt_id,"sort_type",0));
        	Assert.assertEquals("25", 
        			getNodeAttribute(stmt_id,"sorter_output",0));
        	Assert.assertEquals("25", 
        			getNodeAttribute(stmt_id,"input_rows",0));
        	
        	//for UNION node test whether the related entries
        	//are exist and verify their values.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"no_opens",1));
        	Assert.assertEquals("25", 
        			getNodeAttribute(stmt_id,"returned_rows",1));
        }
    }

    /**
      * A simple test of capturing statistics for a DDL statement.
      */
    public void testDDLCreateTable()
        throws Exception
    {
        Statement s = createStatement();
        enableXplainStyle(s);
        String ddlStatement = 
            "create table t1 (a int, b char(10), c timestamp)";
        s.executeUpdate(ddlStatement);
        disableXplainStyle(s);

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select stmt_type, stmt_text " +
                    " from xpltest.sysxplain_statements"),
                new String[][] { {"DDL",ddlStatement} } );
        JDBC.assertSingleValueResultSet(s.executeQuery(
            "select count(*) from xpltest.sysxplain_resultsets"), "0");
    }

    /**
      * A simple test of the INDEX_KEY_OPT special situation.
      */
    public void testMaxFromIndex()
        throws Exception
    {
        Statement s = createStatement();
        enableXplainStyle(s);
        String selectStatement = 
            "select max(country_iso_code) from countries";
        s.executeQuery(selectStatement).close();
        disableXplainStyle(s);

        // The above query is executed as
        // PROJECTION(AGGREGATION(PROJECTION(LASTINDEXKEYSCAN)))
        //
        // The AGGREGATION resultset has index_key_opt = 'Y'.
        //
        // We verify some of the information in the result sets.

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       input_rows, seen_rows, filtered_rows, " +
                    "       returned_rows, index_key_opt " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'AGGREGATION'"),
                new String[][] {
                    {"AGGREGATION", null, "1", "0","0","0","0","Y"} } );
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       seen_rows, filtered_rows, returned_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'LASTINDEXKEYSCAN'"),
                new String[][] {
                    {"LASTINDEXKEYSCAN", "I: COUNTRIES_PK, T: COUNTRIES",
                     "1", "1","0","1"} } );
        // FIXME -- seems like the scan information for the LASTINDEXKEYSCAN
        // is rather incomplete
        // In fact, SCAN_TYPE seems to come back as NULL even though that
        // column is not nullable?
        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select sp.scan_object_name, sp.scan_object_type, " +
                    //"sp.scan_type, sp.isolation_level, " +
                    " sp.isolation_level, " +
                    "sp.no_visited_rows, sp.no_qualified_rows, "+
                    "sp.no_visited_pages, sp.no_visited_deleted_rows, " +
                    "sp.no_fetched_columns, sp.bitset_of_fetched_columns, " +
                    "sp.scan_qualifiers " +
                    "from xpltest.sysxplain_scan_props sp " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on sp.scan_rs_id = rs.scan_rs_id " +
                    "where rs.op_identifier='LASTINDEXKEYSCAN'"),
                new String[][] {
                    {"COUNTRIES_PK", "I",
                        // null,
                        "RC", null, null, null, null, null, null, null} } );
        
        
         /** This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);
        	
        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(4,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals(
        			"PROJECTION|AGGREGATION|PROJECTION|LASTINDEXKEYSCAN|", 
        			getNodeName(stmt_id));

        	//for LASTINDEXKEYSCAN node, test no_opens entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"no_opens",3));
        	
        	//for LASTINDEXKEYSCAN node, test returned_rows entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"returned_rows",3));
        	
        	//for LASTINDEXKEYSCAN node, test scanned_object entry
        	//is exist and verify its value.
        	Assert.assertEquals("COUNTRIES_PK", 
        			getNodeAttribute(stmt_id,"scanned_object",3));
        	
        	//for LASTINDEXKEYSCAN node, test visited_pages entry.
        	//Since it's null, there should be no entry in xml.
        	Assert.assertEquals("", 
        			getNodeAttribute(stmt_id,"visited_pages",3));
        	
        	//for LASTINDEXKEYSCAN node, test scan_qualifiers entry.
        	//Since it's null, there should be no entry in xml.
        	Assert.assertEquals("", 
        			getNodeAttribute(stmt_id,"scan_qualifiers",3));
        }
    }

    /**
      * A simple test of a LEFT OUTER JOIN and EMPTY_RIGHT_ROWS values.
      */
    public void testOuterJoin()
        throws Exception
    {
        Statement s = createStatement();
        enableXplainStyle(s);
        String selectStatement = 
            "select f.orig_airport,c.country " +
            " from flights f left outer join countries c " +
            " on f.orig_airport = c.country_iso_code";
        JDBC.assertUnorderedResultSet(s.executeQuery(selectStatement),
                new String[][] {
                    {"ABQ",null},{"LAX",null},{"ABQ",null},{"PHX",null},
                    {"ABQ",null},{"OKC",null},{"AKL",null},{"HNL",null},
                    {"AKL",null},{"NRT",null}
        });
        disableXplainStyle(s);

        // We should get a Nested Loop Outer Join which  reads 10 rows
        // from the left (SEEN_ROWS), constructs 10 EMPTY_RIGHT_ROWS,
        // and has 10 RETURNED_ROWS.
        //
        // The overall statement is:
        // PROJECTION(LONLJOIN(INDEXSCAN,ROWIDSCAN(CONSTRAINTSCAN)))
        // The nested loop join is a scan of the orig_airport index,
        // joining against probes into the countries_pk constraint-index.

        JDBC.assertUnorderedResultSet(s.executeQuery(
                    "select op_identifier from xpltest.sysxplain_resultsets "),
                new String[][] {
                    {"PROJECTION"}, {"LONLJOIN"}, {"INDEXSCAN"},
                    {"ROWIDSCAN"}, {"CONSTRAINTSCAN"} } );
        // FIXME -- I think EMPTY_RIGHT_ROWS should be 10, not 0?
        JDBC.assertFullResultSet(s.executeQuery(
                    "select op_identifier, op_details, no_opens, " +
                    "       seen_rows, seen_rows_right, filtered_rows, " +
                    "       returned_rows, empty_right_rows " +
                    "from xpltest.sysxplain_resultsets " +
                    "where op_identifier = 'LONLJOIN'"),
                new String[][] {
                    {"LONLJOIN", "(1), Nested Loop Left Outer Join ResultSet",
                     "1", "10", "0", "0", "10", "0"} } );
        
        
         /** This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(5,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals(
        			"PROJECTION|LONLJOIN|INDEXSCAN|ROWIDSCAN|CONSTRAINTSCAN|", 
        			getNodeName(stmt_id));

        	//for LONLJOIN node, test no_opens entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"no_opens",1));
        	
        	//for LONLJOIN node, test returned_rows entry
        	//is exist and verify its value.
        	Assert.assertEquals("10", 
        			getNodeAttribute(stmt_id,"returned_rows",1));
        }
    }

    /**
      * A simple test to verify that startPosition and stopPosition work.
      */
    public void testScanPositions()
        throws Exception
    {
        Statement s = createStatement();
        // Try several different syntaxes of index scans, to see what we get:
        String []searches = {
            "select * from flights where dest_airport = 'ABQ'",
            "select * from flights where dest_airport > 'HNL'",
            "select * from flights where dest_airport < 'HNL'",
            "select * from flights where dest_airport between 'H' and 'J'",
            "select * from flights where dest_airport like 'AB%'",
        };
        String []startPrefixes = {
            ">= on first 1 column(s).",
            "> on first 1 column(s).",
            "None",
            ">= on first 1 column(s).",
            ">= on first 1 column(s).",
        };
        String []stopPrefixes = {
            "> on first 1 column(s).",
            "None",
            ">= on first 1 column(s).",
            "> on first 1 column(s).",
            ">= on first 1 column(s).",
        };
        enableXplainStyle(s);
        for (int i = 0; i < searches.length; i++)
            s.executeQuery(searches[i]).close();
        disableXplainStyle(s);

        ResultSet rs = s.executeQuery(
                "select s.stmt_text, sp.start_position, sp.stop_position " +
                "  from xpltest.sysxplain_statements s, " +
                "       xpltest.sysxplain_resultsets rs, " +
                "       xpltest.sysxplain_scan_props sp " +
                " where s.stmt_id = rs.stmt_id and " +
                "       rs.scan_rs_id = sp.scan_rs_id");
        int matchedStatements = 0;
        while (rs.next())
        {
            String sText = rs.getString("stmt_text");
            String startPos = rs.getString("start_position").trim();
            String stopPos = rs.getString("stop_position").trim();
            boolean foundStmt = false;
            for (int i = 0; i < searches.length; i++)
            {
                if (searches[i].equals(sText))
                {
                    matchedStatements++;
                    foundStmt = true;
                    if (! startPos.startsWith(startPrefixes[i]))
                        fail("Expected start_position for statement '" +
                                searches[i] + "' to start with '" +
                                startPrefixes[i] + "', but it actually was " +
                                startPos);
                    if (! stopPos.startsWith(stopPrefixes[i]))
                        fail("Expected stop_position for statement '" +
                                searches[i] + "' to start with '" +
                                stopPrefixes[i] + "', but it actually was " +
                                stopPos);
                    break;
                }
            }
            assertTrue("Found unexpected statement " + sText, foundStmt);
        }
        assertEquals("Captured wrong number of statements?",
                searches.length, matchedStatements);

        /* * This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         * This test specially checks the behaviour of the tool
         * when the query executed contains special XML characters
         * such as <,> etc.
         * */
        if(XML.classpathMeetsXMLReqs()){

            //getting the stmt_ids, because files are generated using
            //stmt_id as their name.
            String[] stmt_id = getStmtIDArray(s, searches.length);

            for(int i=0;i<stmt_id.length;i++){
                //testing the <statement> element
                Assert.assertEquals(
                        searches[i],
                        readStatement(stmt_id[i]));
            }
        }

    }

    /**
      * A simple test of a non-zero value for numDeletedRowsVisited.
      */
    public void testScanDeletedRows()
        throws Exception
    {
        Statement s = createStatement();
        enableXplainStyle(s);
        String selectStatement = "select x from t";
        JDBC.assertUnorderedResultSet(s.executeQuery(selectStatement),
                new String[][] { {"1"},{"2"},{"4"} });
        disableXplainStyle(s);
        
        // There should be a CONSTRAINTSCAN result set with a SCAN PROPS
        // which indicates that we visited 1 deleted row while scanning
        // the index.
        JDBC.assertFullResultSet(s.executeQuery(
                    "select sp.scan_object_type, " +
                    "sp.scan_type, sp.isolation_level, " +
                    "sp.no_visited_rows, sp.no_qualified_rows, "+
                    "sp.no_visited_pages, sp.no_visited_deleted_rows, " +
                    "sp.no_fetched_columns, sp.bitset_of_fetched_columns, " +
                    "sp.scan_qualifiers " +
                    "from xpltest.sysxplain_scan_props sp " +
                    "join xpltest.sysxplain_resultsets rs " +
                    "on sp.scan_rs_id = rs.scan_rs_id " +
                    "where rs.op_identifier='CONSTRAINTSCAN'"),
                new String[][] {
                    {"C","BTREE","RC","4","3","1","1","1","{0}","None"}});
        
        
        
        /* * This test is added by DERBY-4587, to verify the content
         * of the XML file generated by the new tool, PlanExporter.
         *  
         * */
        if(XML.classpathMeetsXMLReqs()){
        	//getting the stmt_id, because files are generated using
        	//stmt_id as their name.
        	String stmt_id = getStmtID(s);

        	//testing the <statement> element
        	Assert.assertEquals(
        			selectStatement, 
        			readStatement(stmt_id));

        	//testing the count of <node> elements
        	Assert.assertEquals(1,
        			countNode(stmt_id));

        	//testing the root <node> element's name attributes
        	Assert.assertEquals(
        			"CONSTRAINTSCAN|", 
        			getNodeName(stmt_id));

        	//for CONSTRAINTSCAN node, test scan_type entry
        	//is exist and verify its value.
        	Assert.assertEquals("BTREE", 
        			getNodeAttribute(stmt_id,"scan_type",0));
        	
        	//for CONSTRAINTSCAN node, test visited_pages entry
        	//is exist and verify its value.
        	Assert.assertEquals("1", 
        			getNodeAttribute(stmt_id,"visited_pages",0));
       
        	//for CONSTRAINTSCAN node, test visited_pages entry
        	//is exist and verify its value.
        	Assert.assertEquals("None", 
        			getNodeAttribute(stmt_id,"scan_qualifiers",0));
        }
    }

    /**
     * Test that run-time statistics are recorded even if the query has a
     * predicate that always evaluates to false. Before DERBY-6268, the logic
     * that saved the statistics would be short-circuited in such queries.
     */
    public void testAlwaysEmptyResultSet() throws Exception {
        // Execute a query with a predicate that is known at compile time to
        // evaluate to false. The predicate FALSE should do.
        String sql = "select * from sysibm.sysdummy1 where false -- DERBY-6268";
        Statement s = createStatement();
        enableXplainStyle(s);
        JDBC.assertEmpty(s.executeQuery(sql));
        disableXplainStyle(s);

        // Now, see if we find the query among the recorded statements.
        PreparedStatement ps = prepareStatement(
            "select * from xpltest.sysxplain_statements where stmt_text = ?");
        ps.setString(1, sql);
        JDBC.assertDrainResults(ps.executeQuery(), 1);
    }

    /**
      * A simple test of table with the wrong 'shape'.
      */
    public void testTableNotValid()
        throws SQLException
    {
        Statement s = createStatement();
        for (int i = 0; i < tableNames.length; i++)
            if (hasTable(s, "XPLTEST", tableNames[i]))
                s.executeUpdate("drop table xpltest."+tableNames[i]);
        s.executeUpdate("create table xpltest.sysxplain_resultsets(a int)");
        try
        {
            enableXplainStyle(s);
            fail("Expected an error from table with wrong shape");
        }
        catch (SQLException e)
        {
            assertSQLState("Expected 42X14 error for missing column",
                    "42X14", e);
            if (e.getMessage().indexOf("RS_ID") < 0)
                fail("Expected message about missing column RS_ID, not " +
                        e.getMessage());
        } finally {
            // Drop the created table if this testcase is not run as the last.
            s.executeUpdate("drop table xpltest.sysxplain_resultsets");
        }
    }

    /**
     * Test that queries that contain non-ASCII characters are not garbled
     * by the plan exporter tool. Regression test case for DERBY-4902.
     */
    public void testPlanExporterEncoding() throws Exception {
        String table = "D4902_BL\u00C5B\u00C6R";
        String queryText = "SELECT * FROM " + table;

        Statement s = createStatement();
        s.execute("CREATE TABLE " + table + "(X INT)");

        enableXplainStyle(s);
        JDBC.assertEmpty(s.executeQuery(queryText));
        disableXplainStyle(s);

        ResultSet rs = s.executeQuery(
                "SELECT STMT_ID, STMT_TEXT FROM XPLTEST.SYSXPLAIN_STATEMENTS");
        assertTrue(rs.next());
        String stmtId = rs.getString(1);
        assertEquals(queryText, rs.getString(2));
        assertFalse(rs.next());
        rs.close();

        if (XML.classpathMeetsXMLReqs()) {
            assertEquals(queryText, readStatement(stmtId));
        }
    }

    /**
     * Test that queries that contain characters with a special meaning in
     * XML are not garbled by the plan exporter tool. Regression test case
     * for DERBY-4903.
     */
    public void testPlanExporterHandlingSpecialCharacters() throws Exception{
        String table =
                "A \"double\" and 'single' quoted table name " +
                "with some other special characters, like <, > and &";

        String escapedTable = JDBC.escape(table);

        String queryText =
                "SELECT * FROM " + escapedTable +
                " WHERE X < LENGTH('a & b') AND X > 1";

        Statement s = createStatement();
        s.execute("CREATE TABLE " + escapedTable + "(X INT)");

        enableXplainStyle(s);
        JDBC.assertEmpty(s.executeQuery(queryText));
        disableXplainStyle(s);

        ResultSet rs = s.executeQuery(
        "SELECT STMT_ID, STMT_TEXT FROM XPLTEST.SYSXPLAIN_STATEMENTS");
        assertTrue(rs.next());
        String stmtId = rs.getString(1);
        assertEquals(queryText, rs.getString(2));
        assertFalse(rs.next());
        rs.close();

        if (XML.classpathMeetsXMLReqs()) {
            assertEquals(queryText, readStatement(stmtId));

            //testing the root <node> element's name attributes
            Assert.assertEquals(
                    "TABLESCAN|",
                    getNodeName(stmtId));

            //for TABLESCAN node, test scanned_object entry
            //is exist and verify its value correctly replaced
            //by special XML symbols.
            assertEquals(table, getNodeAttribute(stmtId, "scanned_object", 0));
        }

    }

    /**
     * Abstract class for a thread executing a database action (i.e. a query).
     */
    private static abstract class AbstractMTThread
            implements Runnable {

        protected final Connection con;
        /** Duration of the run. */
        protected final long runTime;
        /** Tells how many times the action has been performed. */
        protected int count;
        /** If an error is raised when performing the action. */
        protected Throwable error;

        protected AbstractMTThread(Connection con, long runTime) {
            this.con = con;
            this.runTime = runTime;
        }

        public int getActionCount() {
            return count;
        }

        public boolean failed() {
            return error != null;
        }

        public Throwable getError() {
            return error;
        }
    }

    /**
     * Thread for selecting from the COUNTRIES table in a loop, with the XPLAIN-
     * functionality enabled.
     */
    private static class MTSimpleSelect
            extends AbstractMTThread {

        public MTSimpleSelect(Connection con, long runTime) {
            super(con, runTime);
        }

        /**
         * Selects from the COUNTRIES table in a loop.
         */
        public void run() {
            Random rand = new Random();
            final long start = System.currentTimeMillis();
            try {
                Statement s = con.createStatement();
                enableXplainStyleWithTiming(s);
                PreparedStatement ps = con.prepareStatement(
                        "SELECT country from countries WHERE " +
                        "region = 'Central America'");
                while (System.currentTimeMillis() - start < runTime) {
                    JDBC.assertUnorderedResultSet(ps.executeQuery(),
                        new String[][] {
                            {"Belize"}, {"Costa Rica"}, {"El Salvador"},
                            {"Guatemala"}, {"Honduras"}, {"Nicaragua"} } );
                    count++;
                    try {
                        Thread.sleep(rand.nextInt(50));
                    } catch (InterruptedException ie) {
                        // Ignore
                    }
                }
                // Don't disable XPLAIN here, as it takes a long time due to the
                // high number of recorded plans - these are currently being
                // exported to disk as XML in the disable-method.
                // This connection is going away anyway.
                //disableXplainStyle(s);
                s.close();
                ps.close();
            } catch (Throwable t) {
                // Signal failure
                error = t;
            } finally {
                try {
                    con.rollback();
                    con.close();
                } catch (SQLException sqle) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Thread for invalidating the COUNTRIES table in a loop.
     */
    private static class MTSimpleInvalidate
            extends AbstractMTThread {

        public MTSimpleInvalidate(Connection con, long runTime) {
            super(con, runTime);
        }

        /**
         * Invalidates the COUNTRIES table continuously by updating the
         * associated index statistics. Loops until the run-time is up.
         */
        public void run() {
            final long start = System.currentTimeMillis();
            try {
                PreparedStatement ps = con.prepareStatement(
                        "call SYSCS_UTIL.SYSCS_UPDATE_STATISTICS(?, ?, ?)");
                ps.setString(1, "APP");
                ps.setString(2, "COUNTRIES");
                ps.setNull(3, Types.VARCHAR);
                while (System.currentTimeMillis() - start < runTime) {
                    ps.execute();
                    count++;
                }
                ps.close();
            } catch (Throwable t) {
                // Signal failure
                error = t;
            } finally {
                try {
                    con.rollback();
                    con.close();
                } catch (SQLException sqle) {
                    // Ignore
                }
            }
        }
    }
}
