/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.derbyTesting.junit;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class SQLUtilities {

    public static String VALID_DATE_STRING = "'2000-01-01'";
    public static String VALID_TIME_STRING = "'15:30:20'";
    public static String VALID_TIMESTAMP_STRING = "'2000-01-01 15:30:20'";
    public static String NULL_VALUE="NULL";

    public static String[] allDataTypesColumnNames =
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2463
            "SMALLINTCOL",
            "INTEGERCOL",
            "BIGINTCOL",
            "DECIMALCOL",
            "REALCOL",
            "DOUBLECOL",
            "CHARCOL",
            "VARCHARCOL",
            "LONGVARCHARCOL",
            "CHARFORBITCOL",
            "VARCHARFORBITCOL",
            "LVARCHARFORBITCOL",
            "CLOBCOL",
            "DATECOL",
            "TIMECOL",
            "TIMESTAMPCOL",
            "BLOBCOL",

    };

    private static String[][]allDataTypesSQLData =
    {
            {NULL_VALUE, "0","1","2"},       // SMALLINT
            {NULL_VALUE,"0","1","21"},       // INTEGER
            {NULL_VALUE,"0","1","22"},       // BIGINT
            {NULL_VALUE,"0.0","1.0","23.0"},      // DECIMAL(10,5)
            {NULL_VALUE,"0.0","1.0","24.0"},      // REAL,
            {NULL_VALUE,"0.0","1.0","25.0"},      // DOUBLE
            {NULL_VALUE,"'0'","'aa'","'2.0'"},      // CHAR(60)
            {NULL_VALUE,"'0'","'aa'",VALID_TIME_STRING},      //VARCHAR(60)",
            {NULL_VALUE,"'0'","'aa'",VALID_TIMESTAMP_STRING},      // LONG VARCHAR
            {NULL_VALUE,"X'10aa'",NULL_VALUE,"X'10aaaa'"},  // CHAR(60)  FOR BIT DATA
            {NULL_VALUE,"X'10aa'",NULL_VALUE,"X'10aaba'"},  // VARCHAR(60) FOR BIT DATA
            {NULL_VALUE,"X'10aa'",NULL_VALUE,"X'10aaca'"},  //LONG VARCHAR FOR BIT DATA
            {NULL_VALUE,"'13'","'14'",NULL_VALUE},     //CLOB(1k)
            {NULL_VALUE,SQLUtilities.VALID_DATE_STRING,SQLUtilities.VALID_DATE_STRING,NULL_VALUE},        // DATE
            {NULL_VALUE,VALID_TIME_STRING,VALID_TIME_STRING,VALID_TIME_STRING},        // TIME
            {NULL_VALUE,VALID_TIMESTAMP_STRING,VALID_TIMESTAMP_STRING,VALID_TIMESTAMP_STRING},   // TIMESTAMP
            {NULL_VALUE,NULL_VALUE,NULL_VALUE,NULL_VALUE}                 // BLOB
    };

    /**
     * Create a table AllDataTypesTable and populate with data
     * @param s
     * @throws SQLException
     */
    public  static void createAndPopulateAllDataTypesTable(Statement s) throws SQLException {
        try {
            s.executeUpdate("DROP TABLE AllDataTypesTable");
        } catch (SQLException se) {
        }

        StringBuffer createSQL = new StringBuffer(
                "create table AllDataTypesTable (");
//IC see: https://issues.apache.org/jira/browse/DERBY-3034
        for (int type = 0; type < SQLUtilities.SQLTypes.length - 1; type++) {
            createSQL.append(allDataTypesColumnNames[type] + " " + SQLUtilities.SQLTypes[type]
                    + ",");
        }
        createSQL.append(allDataTypesColumnNames[SQLUtilities.SQLTypes.length - 1] + " "
                + SQLUtilities.SQLTypes[SQLUtilities.SQLTypes.length - 1] + ")");
        s.executeUpdate(createSQL.toString());

        for (int row = 0; row < allDataTypesSQLData[0].length; row++) {
            createSQL = new StringBuffer(
                    "insert into AllDataTypesTable values(");
            for (int type = 0; type < SQLUtilities.SQLTypes.length - 1; type++) {
                createSQL.append(allDataTypesSQLData[type][row] + ",");
            }
            createSQL.append(allDataTypesSQLData[SQLUtilities.SQLTypes.length - 1][row] + ")");
            
            s.executeUpdate(createSQL.toString());
        }
    }

    
    
    /**
     * 
     * Assumes user previously executed 
     * "call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)";
     * @param s Statement to use for calling runtime statistics function
     * @return a runtime statistics parser  
     * @throws SQLException
     */
    public static RuntimeStatisticsParser  getRuntimeStatisticsParser(Statement s) throws SQLException
    {
        ResultSet rs = s.executeQuery("values SYSCS_UTIL.SYSCS_GET_RUNTIMESTATISTICS()");
        rs.next();
        String rts = rs.getString(1);
//IC see: https://issues.apache.org/jira/browse/DERBY-2478
        rs.close();
        return new RuntimeStatisticsParser(rts);
    }

    public static RuntimeStatisticsParser executeAndGetRuntimeStatistics(Connection conn, String sql ) throws SQLException
    {
        Statement s = conn.createStatement();
        Statement s2 = conn.createStatement();
        CallableStatement cs = conn.prepareCall("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
        cs.execute();
        cs.close();
        s.execute(sql);
        ResultSet rs = s.getResultSet();
        if (rs != null)
            JDBC.assertDrainResults(rs);
//IC see: https://issues.apache.org/jira/browse/DERBY-2478
        RuntimeStatisticsParser parser = getRuntimeStatisticsParser(s2);
        s.close();
        s2.close();
        return parser;
    }

	// Note: This array is accessed in lang.NullIfTest
	public static String[] SQLTypes =
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3034
	        "SMALLINT",
	        "INTEGER",
	        "BIGINT",
	        "DECIMAL(10,5)",
	        "REAL",
	        "DOUBLE",
	        "CHAR(60)",
	        "VARCHAR(60)",
	        "LONG VARCHAR",
	        "CHAR(60) FOR BIT DATA",
	        "VARCHAR(60) FOR BIT DATA",
	        "LONG VARCHAR FOR BIT DATA",
	        "CLOB(1k)",
	        "DATE",
	        "TIME",
	        "TIMESTAMP",
	        "BLOB(1k)",
	};
    
}
