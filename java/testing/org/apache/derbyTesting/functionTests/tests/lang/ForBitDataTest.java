/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.forbitdata

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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.IOException;
import java.sql.Types;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.extensions.TestSetup;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;


public class ForBitDataTest extends BaseJDBCTestCase {

        private static String[] TABLES = { 
        	"CREATE TABLE FBDOK.T001 (C001 CHAR FOR BIT DATA)",
        	"CREATE TABLE FBDOK.T002 (C002 CHAR(1) FOR BIT DATA)",
		"CREATE TABLE FBDOK.T003 (C003 CHAR(10) FOR BIT DATA)",
		"CREATE TABLE FBDOK.T004 (C004 CHAR(254) FOR BIT DATA)",
		"CREATE TABLE FBDOK.T005 (C005 VARCHAR(1) FOR BIT DATA)",
		"CREATE TABLE FBDOK.T006 (C006 VARCHAR(100) FOR BIT DATA)",
		"CREATE TABLE FBDOK.T007 (C007 VARCHAR(32672) FOR BIT DATA)",
		"CREATE TABLE FBDOK.T008 (C008 LONG VARCHAR FOR BIT DATA)",
		"CREATE TABLE FBDVAL.T001(ID INT NOT NULL PRIMARY KEY, C1 CHAR(10) FOR BIT DATA, C2 VARCHAR(10) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(10))",
		"CREATE TABLE FBDVAL.X001(XID INT NOT NULL PRIMARY KEY, X1 CHAR(12) FOR BIT DATA, C2 VARCHAR(12) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(12))",
		"CREATE TABLE FBDVAL.T002(ID INT NOT NULL PRIMARY KEY, C1 CHAR(10) FOR BIT DATA, C2 VARCHAR(10) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(10))",
		"CREATE TABLE FBDVAL.TEL(C2 VARCHAR(32672) FOR BIT DATA, C3 LONG VARCHAR FOR BIT DATA, C4 BLOB(128k))"
           
        };
           
     
	/* testTypesExpectedValues is a table of expected values for the testTypes fixture */
	public static String[] testTypesExpectedValues = {
		"FBDOK,T001,C001,-2,CHAR () FOR BIT DATA,1,null,null,1,null,null,1,YES",
		"FBDOK,T002,C002,-2,CHAR () FOR BIT DATA,1,null,null,1,null,null,1,YES",
		"FBDOK,T003,C003,-2,CHAR () FOR BIT DATA,10,null,null,1,null,null,1,YES",
		"FBDOK,T004,C004,-2,CHAR () FOR BIT DATA,254,null,null,1,null,null,1,YES",
		"FBDOK,T005,C005,-3,VARCHAR () FOR BIT DATA,1,null,null,1,null,null,1,YES",
		"FBDOK,T006,C006,-3,VARCHAR () FOR BIT DATA,100,null,null,1,null,null,1,YES",
		"FBDOK,T007,C007,-3,VARCHAR () FOR BIT DATA,32672,null,null,1,null,null,1,YES",
		"FBDOK,T008,C008,-4,LONG VARCHAR FOR BIT DATA,32700,null,null,1,null,null,1,YES"
	};

	/* testTypesExpectedValues2Embedded is a table of expected values for the testTypes fixture*/  
	public static String[] testTypesExpectedValuesEmbedded = {
		"C001 CHAR () FOR BIT DATA precision 1",
		"C002 CHAR () FOR BIT DATA precision 1",
		"C003 CHAR () FOR BIT DATA precision 10",
		"C004 CHAR () FOR BIT DATA precision 254",
		"C005 VARCHAR () FOR BIT DATA precision 1",
		"C006 VARCHAR () FOR BIT DATA precision 100",
		"C007 VARCHAR () FOR BIT DATA precision 32672",
		"C008 LONG VARCHAR FOR BIT DATA precision 32700",
	};

        /* testTypesExpectedValues2NetworkServer is a table of expected values for the testTypes fixture */  
	public static String[] testTypesExpectedValuesNetworkServer = {
		"C001 CHAR FOR BIT DATA precision 1",
		"C002 CHAR FOR BIT DATA precision 1",
		"C003 CHAR FOR BIT DATA precision 10",
		"C004 CHAR FOR BIT DATA precision 254",
		"C005 VARCHAR FOR BIT DATA precision 1",
		"C006 VARCHAR FOR BIT DATA precision 100",
		"C007 VARCHAR FOR BIT DATA precision 32672",
		"C008 LONG VARCHAR FOR BIT DATA precision 32700",
	};

	/* testTypesExpectedValues3 is a table of expected values for the testTypes fixture  */
	public static String[] testTypesExpectedValues3 = {
		"LONG VARCHAR FOR BIT DATA(-4) precision 32700",
		"VARCHAR () FOR BIT DATA(-3) precision 32672",
		"CHAR () FOR BIT DATA(-2) precision 254",
	};

	/* testCompareExpectedValues is a table of expected values for the testCompare fixture */ 
	public static String[][] testCompareExpectedValues = {
		{ "30 0423a2fd202020202020 (10)   30 0423a2fd202020202020 (10) ",
			"30 0423a2fd202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"40 0423a1fd202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   50 0423a2ff202020202020 (10) ", 
			"60 0423a2fd202020202020 (10)   30 0423a2fd202020202020 (10) ",
			"60 0423a2fd202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{ "30 0423a2fd202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"30 0423a2fd202020202020 (10)   50 0423a2ff202020202020 (10) ",
			"30 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd202020202020 (10)   30 0423a2fd202020202020 (10) ", 
			"40 0423a1fd202020202020 (10)   50 0423a2ff202020202020 (10) ", 
			"40 0423a1fd202020202020 (10)   60 0423a2fd202020202020 (10) ", 
			"40 0423a1fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"50 0423a2ff202020202020 (10)   30 0423a2fd202020202020 (10) ", 
			"50 0423a2ff202020202020 (10)   40 0423a1fd202020202020 (10) ", 
			"50 0423a2ff202020202020 (10)   60 0423a2fd202020202020 (10) ", 
			"50 0423a2ff202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"60 0423a2fd202020202020 (10)   40 0423a1fd202020202020 (10) ", 
			"60 0423a2fd202020202020 (10)   50 0423a2ff202020202020 (10) ", 
			"60 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd202020202020 (10) ", 
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd202020202020 (10) ", 
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff202020202020 (10) ", 
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd202020202020 (10) "
		},
		{ "30 0423a2fd202020202020 (10)   50 0423a2ff202020202020 (10) ", 
			"30 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"40 0423a1fd202020202020 (10)   30 0423a2fd202020202020 (10) ", 
			"40 0423a1fd202020202020 (10)   50 0423a2ff202020202020 (10) ", 
			"40 0423a1fd202020202020 (10)   60 0423a2fd202020202020 (10) ", 
			"40 0423a1fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"50 0423a2ff202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"60 0423a2fd202020202020 (10)   50 0423a2ff202020202020 (10) ", 
			"60 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{ "30 0423a2fd202020202020 (10)   30 0423a2fd202020202020 (10) ", 
			"30 0423a2fd202020202020 (10)   50 0423a2ff202020202020 (10) ",
			"30 0423a2fd202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"30 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd202020202020 (10)   30 0423a2fd202020202020 (10) ",
			"40 0423a1fd202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"40 0423a1fd202020202020 (10)   50 0423a2ff202020202020 (10) ",
			"40 0423a1fd202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"40 0423a1fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ",
			"50 0423a2ff202020202020 (10)   50 0423a2ff202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd202020202020 (10)   30 0423a2fd202020202020 (10) ",
			"60 0423a2fd202020202020 (10)   50 0423a2ff202020202020 (10) ",
			"60 0423a2fd202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"60 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) " 
		},
		{ "30 0423a2fd202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   30 0423a2fd202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"60 0423a2fd202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd202020202020 (10) " 
		},
		{ "30 0423a2fd202020202020 (10)   30 0423a2fd202020202020 (10) ", 
			"30 0423a2fd202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"30 0423a2fd202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"40 0423a1fd202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   30 0423a2fd202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   50 0423a2ff202020202020 (10) ",
			"50 0423a2ff202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"60 0423a2fd202020202020 (10)   30 0423a2fd202020202020 (10) ",
			"60 0423a2fd202020202020 (10)   40 0423a1fd202020202020 (10) ",
			"60 0423a2fd202020202020 (10)   60 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{ "30 0423a2fd202020202020 (10)   30 0423a2fd (4) ",
			"30 0423a2fd202020202020 (10)   60 0423a2fd20 (5) ",
			"40 0423a1fd202020202020 (10)   40 0423a1fd (4) ",			"50 0423a2ff202020202020 (10)   50 0423a2ff (4) ",
			"60 0423a2fd202020202020 (10)   30 0423a2fd (4) ",
			"60 0423a2fd202020202020 (10)   60 0423a2fd20 (5) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "  
		},
		{ "30 0423a2fd202020202020 (10)   40 0423a1fd (4) ",
			"30 0423a2fd202020202020 (10)   50 0423a2ff (4) ",
			"30 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"40 0423a1fd202020202020 (10)   30 0423a2fd (4) ",
			"40 0423a1fd202020202020 (10)   50 0423a2ff (4) ",
			"40 0423a1fd202020202020 (10)   60 0423a2fd20 (5) ",
			"40 0423a1fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"50 0423a2ff202020202020 (10)   30 0423a2fd (4) ",
			"50 0423a2ff202020202020 (10)   40 0423a1fd (4) ",
			"50 0423a2ff202020202020 (10)   60 0423a2fd20 (5) ",
			"50 0423a2ff202020202020 (10)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd202020202020 (10)   40 0423a1fd (4) ",
			"60 0423a2fd202020202020 (10)   50 0423a2ff (4) ",
			"60 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd (4) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd (4) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff (4) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd20 (5) "
		},
		{ "30 0423a2fd202020202020 (10)   50 0423a2ff (4) ",
			"30 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"40 0423a1fd202020202020 (10)   30 0423a2fd (4) ",
			"40 0423a1fd202020202020 (10)   50 0423a2ff (4) ",
			"40 0423a1fd202020202020 (10)   60 0423a2fd20 (5) ",
			"40 0423a1fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"50 0423a2ff202020202020 (10)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd202020202020 (10)   50 0423a2ff (4) ",
			"60 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{ "30 0423a2fd202020202020 (10)   30 0423a2fd (4) ",
			"30 0423a2fd202020202020 (10)   50 0423a2ff (4) ",
			"30 0423a2fd202020202020 (10)   60 0423a2fd20 (5) ",
			"30 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd202020202020 (10)   30 0423a2fd (4) ",
			"40 0423a1fd202020202020 (10)   40 0423a1fd (4) ",
			"40 0423a1fd202020202020 (10)   50 0423a2ff (4) ",
			"40 0423a1fd202020202020 (10)   60 0423a2fd20 (5) ",
			"40 0423a1fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"50 0423a2ff202020202020 (10)   50 0423a2ff (4) ",
			"50 0423a2ff202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"60 0423a2fd202020202020 (10)   30 0423a2fd (4) ",
			"60 0423a2fd202020202020 (10)   50 0423a2ff (4) ",
			"60 0423a2fd202020202020 (10)   60 0423a2fd20 (5) ",
			"60 0423a2fd202020202020 (10)   70 0b27a2fd016de2356690 (10) ", 
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{ "30 0423a2fd202020202020 (10)   40 0423a1fd (4) ",
			"50 0423a2ff202020202020 (10)   30 0423a2fd (4) ", 
			"50 0423a2ff202020202020 (10)   40 0423a1fd (4) ",
			"50 0423a2ff202020202020 (10)   60 0423a2fd20 (5) ",
			"60 0423a2fd202020202020 (10)   40 0423a1fd (4) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd (4) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd (4) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff (4) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd20 (5) "
		},
		{ "30 0423a2fd202020202020 (10)   30 0423a2fd (4) ", 
			"30 0423a2fd202020202020 (10)   40 0423a1fd (4) ",
			"30 0423a2fd202020202020 (10)   60 0423a2fd20 (5) ",
			"40 0423a1fd202020202020 (10)   40 0423a1fd (4) ",
			"50 0423a2ff202020202020 (10)   30 0423a2fd (4) ",
			"50 0423a2ff202020202020 (10)   40 0423a1fd (4) ",
			"50 0423a2ff202020202020 (10)   50 0423a2ff (4) ",
			"50 0423a2ff202020202020 (10)   60 0423a2fd20 (5) ",
			"60 0423a2fd202020202020 (10)   30 0423a2fd (4) ",
			"60 0423a2fd202020202020 (10)   40 0423a1fd (4) ",
			"60 0423a2fd202020202020 (10)   60 0423a2fd20 (5) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd (4) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd (4) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff (4) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd20 (5) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "  
		},
		{ "42818 types not comparable C1 ... C3"
		},
		{ "42818 types not comparable C1 ... C3" 
		},
		{ "42818 types not comparable C1 ... C3"
		},
		{ "42818 types not comparable C1 ... C3"
		},
		{ "42818 types not comparable C1 ... C3"
		},
		{ "42818 types not comparable C1 ... C3"
		},
		{ "42818 types not comparable C1 ... C4"
		},
		{ "42818 types not comparable C1 ... C4"
		},
		{ "42818 types not comparable C1 ... C4"
		},
		{ "42818 types not comparable C1 ... C4"
		},
		{ "42818 types not comparable C1 ... C4"
		},
		{ "42818 types not comparable C1 ... C4"
		},
		{ "30 0423a2fd (4)   30 0423a2fd202020202020 (10) ", 
			"30 0423a2fd (4)   60 0423a2fd202020202020 (10) ",
			"40 0423a1fd (4)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff (4)   50 0423a2ff202020202020 (10) ",
			"60 0423a2fd20 (5)   30 0423a2fd202020202020 (10) ",
			"60 0423a2fd20 (5)   60 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{ "30 0423a2fd (4)   40 0423a1fd202020202020 (10) ",
			"30 0423a2fd (4)   50 0423a2ff202020202020 (10) ",
			"30 0423a2fd (4)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd (4)   30 0423a2fd202020202020 (10) ",
			"40 0423a1fd (4)   50 0423a2ff202020202020 (10) ",
			"40 0423a1fd (4)   60 0423a2fd202020202020 (10) ",
			"40 0423a1fd (4)   70 0b27a2fd016de2356690 (10) ",
			"50 0423a2ff (4)   30 0423a2fd202020202020 (10) ",
			"50 0423a2ff (4)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff (4)   60 0423a2fd202020202020 (10) ",
			"50 0423a2ff (4)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd20 (5)   40 0423a1fd202020202020 (10) ",
			"60 0423a2fd20 (5)   50 0423a2ff202020202020 (10) ",
			"60 0423a2fd20 (5)   70 0b27a2fd016de2356690 (10) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd202020202020 (10) "
		},
		{"30 0423a2fd (4)   50 0423a2ff202020202020 (10) ",
			"30 0423a2fd (4)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd (4)   30 0423a2fd202020202020 (10) ",
			"40 0423a1fd (4)   50 0423a2ff202020202020 (10) ",
			"40 0423a1fd (4)   60 0423a2fd202020202020 (10) ",
			"40 0423a1fd (4)   70 0b27a2fd016de2356690 (10) ",
			"50 0423a2ff (4)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd20 (5)   50 0423a2ff202020202020 (10) ",
			"60 0423a2fd20 (5)   70 0b27a2fd016de2356690 (10) "
		},
		{"30 0423a2fd (4)   30 0423a2fd202020202020 (10) ",
			"30 0423a2fd (4)   50 0423a2ff202020202020 (10) ",
			"30 0423a2fd (4)   60 0423a2fd202020202020 (10) ",
			"30 0423a2fd (4)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd (4)   30 0423a2fd202020202020 (10) ",
			"40 0423a1fd (4)   40 0423a1fd202020202020 (10) ",
			"40 0423a1fd (4)   50 0423a2ff202020202020 (10) ",
			"40 0423a1fd (4)   60 0423a2fd202020202020 (10) ",
			"40 0423a1fd (4)   70 0b27a2fd016de2356690 (10) ",
			"50 0423a2ff (4)   50 0423a2ff202020202020 (10) ",
			"50 0423a2ff (4)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd20 (5)   30 0423a2fd202020202020 (10) ",
			"60 0423a2fd20 (5)   50 0423a2ff202020202020 (10) ",
			"60 0423a2fd20 (5)   60 0423a2fd202020202020 (10) ",
			"60 0423a2fd20 (5)   70 0b27a2fd016de2356690 (10) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{"30 0423a2fd (4)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff (4)   30 0423a2fd202020202020 (10) ",
			"50 0423a2ff (4)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff (4)   60 0423a2fd202020202020 (10) ",
			"60 0423a2fd20 (5)   40 0423a1fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd202020202020 (10) "
		},
		{"30 0423a2fd (4)   30 0423a2fd202020202020 (10) ",
			"30 0423a2fd (4)   40 0423a1fd202020202020 (10) ",
			"30 0423a2fd (4)   60 0423a2fd202020202020 (10) ",
			"40 0423a1fd (4)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff (4)   30 0423a2fd202020202020 (10) ",
			"50 0423a2ff (4)   40 0423a1fd202020202020 (10) ",
			"50 0423a2ff (4)   50 0423a2ff202020202020 (10) ",
			"50 0423a2ff (4)   60 0423a2fd202020202020 (10) ",
			"60 0423a2fd20 (5)   30 0423a2fd202020202020 (10) ",
			"60 0423a2fd20 (5)   40 0423a1fd202020202020 (10) ",
			"60 0423a2fd20 (5)   60 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd202020202020 (10) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{"30 0423a2fd (4)   30 0423a2fd (4) ",
			"30 0423a2fd (4)   60 0423a2fd20 (5) ",
			"40 0423a1fd (4)   40 0423a1fd (4) ",
			"50 0423a2ff (4)   50 0423a2ff (4) ",
			"60 0423a2fd20 (5)   30 0423a2fd (4) ",
			"60 0423a2fd20 (5)   60 0423a2fd20 (5) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{"30 0423a2fd (4)   40 0423a1fd (4) ",
			"30 0423a2fd (4)   50 0423a2ff (4) ",
			"30 0423a2fd (4)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd (4)   30 0423a2fd (4) ",
			"40 0423a1fd (4)   50 0423a2ff (4) ",
			"40 0423a1fd (4)   60 0423a2fd20 (5) ",
			"40 0423a1fd (4)   70 0b27a2fd016de2356690 (10) ",
			"50 0423a2ff (4)   30 0423a2fd (4) ",
			"50 0423a2ff (4)   40 0423a1fd (4) ",
			"50 0423a2ff (4)   60 0423a2fd20 (5) ",
			"50 0423a2ff (4)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd20 (5)   40 0423a1fd (4) ",
			"60 0423a2fd20 (5)   50 0423a2ff (4) ",
			"60 0423a2fd20 (5)   70 0b27a2fd016de2356690 (10) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd (4) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd (4) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff (4) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd20 (5) "
		},
		{"30 0423a2fd (4)   50 0423a2ff (4) ",
			"30 0423a2fd (4)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd (4)   30 0423a2fd (4) ",
			"40 0423a1fd (4)   50 0423a2ff (4) ",
			"40 0423a1fd (4)   60 0423a2fd20 (5) ",
			"40 0423a1fd (4)   70 0b27a2fd016de2356690 (10) ",
			"50 0423a2ff (4)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd20 (5)   50 0423a2ff (4) ",
			"60 0423a2fd20 (5)   70 0b27a2fd016de2356690 (10) "
		},
		{"30 0423a2fd (4)   30 0423a2fd (4) ",
			"30 0423a2fd (4)   50 0423a2ff (4) ",
			"30 0423a2fd (4)   60 0423a2fd20 (5) ",
			"30 0423a2fd (4)   70 0b27a2fd016de2356690 (10) ",
			"40 0423a1fd (4)   30 0423a2fd (4) ",
			"40 0423a1fd (4)   40 0423a1fd (4) ",
			"40 0423a1fd (4)   50 0423a2ff (4) ",
			"40 0423a1fd (4)   60 0423a2fd20 (5) ",
			"40 0423a1fd (4)   70 0b27a2fd016de2356690 (10) ",
			"50 0423a2ff (4)   50 0423a2ff (4) ",
			"50 0423a2ff (4)   70 0b27a2fd016de2356690 (10) ",
			"60 0423a2fd20 (5)   30 0423a2fd (4) ",
			"60 0423a2fd20 (5)   50 0423a2ff (4) ",
			"60 0423a2fd20 (5)   60 0423a2fd20 (5) ",
			"60 0423a2fd20 (5)   70 0b27a2fd016de2356690 (10) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) " 
		},
		{ "30 0423a2fd (4)   40 0423a1fd (4) ",
			"50 0423a2ff (4)   30 0423a2fd (4) ",
			"50 0423a2ff (4)   40 0423a1fd (4) ",
			"50 0423a2ff (4)   60 0423a2fd20 (5) ",
			"60 0423a2fd20 (5)   40 0423a1fd (4) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd (4) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd (4) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff (4) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd20 (5) "
		},
		{"30 0423a2fd (4)   30 0423a2fd (4) ",
			"30 0423a2fd (4)   40 0423a1fd (4) ",
			"30 0423a2fd (4)   60 0423a2fd20 (5) ",
			"40 0423a1fd (4)   40 0423a1fd (4) ",
			"50 0423a2ff (4)   30 0423a2fd (4) ",
			"50 0423a2ff (4)   40 0423a1fd (4) ",
			"50 0423a2ff (4)   50 0423a2ff (4) ",
			"50 0423a2ff (4)   60 0423a2fd20 (5) ",
			"60 0423a2fd20 (5)   30 0423a2fd (4) ",
			"60 0423a2fd20 (5)   40 0423a1fd (4) ",
			"60 0423a2fd20 (5)   60 0423a2fd20 (5) ",
			"70 0b27a2fd016de2356690 (10)   30 0423a2fd (4) ",
			"70 0b27a2fd016de2356690 (10)   40 0423a1fd (4) ",
			"70 0b27a2fd016de2356690 (10)   50 0423a2ff (4) ",
			"70 0b27a2fd016de2356690 (10)   60 0423a2fd20 (5) ",
			"70 0b27a2fd016de2356690 (10)   70 0b27a2fd016de2356690 (10) "
		},
		{"42818 types not comparable C2 ... C3"},
		{"42818 types not comparable C2 ... C3"},
		{"42818 types not comparable C2 ... C3"},
		{"42818 types not comparable C2 ... C3"},
		{"42818 types not comparable C2 ... C3"},
		{"42818 types not comparable C2 ... C3"},
		{"42818 types not comparable C2 ... C4"},
		{"42818 types not comparable C2 ... C4"},
		{"42818 types not comparable C2 ... C4"},
		{"42818 types not comparable C2 ... C4"},
		{"42818 types not comparable C2 ... C4"},
		{"42818 types not comparable C2 ... C4"},
		{"42818 types not comparable C3 ... C1"},
		{"42818 types not comparable C3 ... C1"},
		{"42818 types not comparable C3 ... C1"},
		{"42818 types not comparable C3 ... C1"},
		{"42818 types not comparable C3 ... C1"},
		{"42818 types not comparable C3 ... C1"},
		{"42818 types not comparable C3 ... C2"},
		{"42818 types not comparable C3 ... C2"},
		{"42818 types not comparable C3 ... C2"},
		{"42818 types not comparable C3 ... C2"},
		{"42818 types not comparable C3 ... C2"},
		{"42818 types not comparable C3 ... C2"},
		{"42818 types not comparable C3 ... C3"},
		{"42818 types not comparable C3 ... C3"},
		{"42818 types not comparable C3 ... C3"},
		{"42818 types not comparable C3 ... C3"},
		{"42818 types not comparable C3 ... C3"},
		{"42818 types not comparable C3 ... C3"},
		{"42818 types not comparable C3 ... C4"},
		{"42818 types not comparable C3 ... C4"},
		{"42818 types not comparable C3 ... C4"},
		{"42818 types not comparable C3 ... C4"},
		{"42818 types not comparable C3 ... C4"},
		{"42818 types not comparable C3 ... C4"},
		{"42818 types not comparable C4 ... C1"},
		{"42818 types not comparable C4 ... C1"},
		{"42818 types not comparable C4 ... C1"},
		{"42818 types not comparable C4 ... C1"},
		{"42818 types not comparable C4 ... C1"},
		{"42818 types not comparable C4 ... C1"},
		{"42818 types not comparable C4 ... C2"},
		{"42818 types not comparable C4 ... C2"},
		{"42818 types not comparable C4 ... C2"},
		{"42818 types not comparable C4 ... C2"},
		{"42818 types not comparable C4 ... C2"},
		{"42818 types not comparable C4 ... C2"},
		{"42818 types not comparable C4 ... C3"},
		{"42818 types not comparable C4 ... C3"},
		{"42818 types not comparable C4 ... C3"},
		{"42818 types not comparable C4 ... C3"},
		{"42818 types not comparable C4 ... C3"},
		{"42818 types not comparable C4 ... C3"},
		{"42818 types not comparable C4 ... C4"},
		{"42818 types not comparable C4 ... C4"},
		{"42818 types not comparable C4 ... C4"},
		{"42818 types not comparable C4 ... C4"},
		{"42818 types not comparable C4 ... C4"},
		{"42818 types not comparable C4 ... C4"} 
	};

	/* testEncodedLengthsExpectedValues is a table of expected values for the testEncodedLengths() fixture */
	public static String[] testEncodedLengthsExpectedValues = {
		"C1 OK DATA OK C2 OK DATA OK C3 OK DATA OK",
		"C1 NULL C2 OK DATA OK C3 OK DATA OK",
		"C1 NULL C2 NULL C3 OK DATA OK"
	};

	
	
	/* Public constructor required for running test as standalone JUnit. */    
	public ForBitDataTest(String name) {
		super(name);
	}
	
	/**
        Negative for bit data tests. 
        FBD001,FBD007 negative syntax
        FBD005 maximum char length
        FBD009 maximum varchar length
        */
	public void testNegative() throws SQLException {		
		assertCompileError("42611", "CREATE TABLE FBDFAIL.T001 (C001 CHAR(255) FOR BIT DATA)");
        assertCompileError("42611", "CREATE TABLE FBDFAIL.T001 (C001 CHAR(255) FOR BIT DATA)");
        assertCompileError("42611", "CREATE TABLE FBDFAIL.T002 (C002 VARCHAR(32673) FOR BIT DATA)");
        assertCompileError("42X01", "CREATE TABLE FBDFAIL.T003 (C003 VARCHAR FOR BIT DATA)");	
        assertCompileError("42X01", "CREATE TABLE FBDFAIL.T004 (C004 LONG VARCHAR(100) FOR BIT DATA)");
	}

	/**
	FBD001,FBD007 - positive syntax 
	FBD004 - CHAR length defaults to one
	FBD037 - create table
	FBD006, FBD011, FBD014 - correct JDBC type
	 */
	public void testTypes() throws SQLException {
		Connection conn = getConnection();
		ResultSet rs = conn.getMetaData().getColumns(null, "FBDOK", null, null);
		String actualValue = null;

		int i=0;
		while (rs.next()) {
			// skip 1 catalog
			actualValue  = rs.getString(2) + ",";
			actualValue += rs.getString(3) + ",";
			actualValue += rs.getString(4) + ",";
			actualValue += rs.getString(5) + ",";
			actualValue += rs.getString(6) + ",";
			actualValue += rs.getString(7) + ",";
			actualValue += rs.getString(9) + ",";
			actualValue += rs.getString(10) + ",";
			actualValue += rs.getString(11) + ",";
			actualValue += rs.getString(13) + ",";
			actualValue += rs.getString(16) + ",";
			actualValue += rs.getString(17) + ",";
			actualValue += rs.getString(18);
                        assertEquals(testTypesExpectedValues[i], actualValue); 
			i++;
		}
		rs.close();

		for (i = 1; i <= 8; i++) {
			PreparedStatement ps = prepareStatement("SELECT * FROM FBDOK.T00" + i);
			ResultSetMetaData rsmd = ps.getMetaData();
			actualValue = rsmd.getColumnName(1) + " " + rsmd.getColumnTypeName(1) + " precision " + rsmd.getPrecision(1);
                        
			if ( usingDerbyNetClient() )  
			  assertEquals(testTypesExpectedValuesNetworkServer[i-1], actualValue);
			else 
                          assertEquals(testTypesExpectedValuesEmbedded[i-1], actualValue);
			ps.close();
		}

		DatabaseMetaData dmd = conn.getMetaData();
		rs = dmd.getTypeInfo();

		int j=0;
		while (rs.next()) {
			String name = rs.getString(1);
			int jdbcType = rs.getInt(2);
			switch (jdbcType) {
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				break;
			default:
				continue;
			}
			actualValue = name + "(" + jdbcType + ") " + "precision " + rs.getInt(3)+ "" ;
			assertEquals(testTypesExpectedValues3[j], actualValue);
			j++;
		}
		rs.close();

		{
			String sql = "VALUES X'2345d45a2e44'";
			PreparedStatement psv = prepareStatement(sql);
			ResultSetMetaData rsmd = psv.getMetaData();
			actualValue = rsmd.getColumnName(1) + " " + rsmd.getColumnTypeName(1) + " precision " + rsmd.getPrecision(1);
			if ( usingDerbyNetClient() ) 
			    assertEquals("1 CHAR FOR BIT DATA precision 6", actualValue);
			else 
			    assertEquals("1 CHAR () FOR BIT DATA precision 6", actualValue);
		}

		{
			String sql = "VALUES X''";
			PreparedStatement psv = prepareStatement(sql);
			ResultSetMetaData rsmd = psv.getMetaData();
			actualValue = rsmd.getColumnName(1) + " " + rsmd.getColumnTypeName(1) + " precision " + rsmd.getPrecision(1);
			if ( usingDerbyNetClient() ) 
			    assertEquals("1 CHAR FOR BIT DATA precision 0", actualValue);
			else
			    assertEquals("1 CHAR () FOR BIT DATA precision 0", actualValue);
		}
	}

        /* testValues fixture */
	public void testValues() throws SQLException {
                // Empty the table before inserting 
                Statement deleteStmt = createStatement();
                deleteStmt.execute("delete from FBDVAL.T001");
		PreparedStatement psI = prepareStatement("INSERT INTO FBDVAL.T001 VALUES(?, ?, ?, ?, ?)");
		PreparedStatement psS = prepareStatement("SELECT C1, C2, C3, C4, ID FROM FBDVAL.T001 WHERE ID >= ? AND ID < ? ORDER BY ID");

		insertData(psI, 0, null, 10, true);
		showData(psS, 0, null, "ORG <NULL> CHR <NULL> VAR <NULL> LVC <NULL> BLOB <NULL> ");

		byte[] empty = new byte[7];
		insertData(psI, 10, empty, 10, true);
		showData(psS, 10, empty, "ORG 00000000000000 (7) CHR 00000000000000202020 (10) VAR 00000000000000 (7) LVC 00000000000000 (7) BLOB 00000000000000 (7) ");

		byte[] empty2 = new byte[15];
		insertData(psI, 20, empty2, 10, true);
		showData(psS, 20, empty2, "ORG 000000000000000000000000000000 (15) CHR <NULL> VAR <NULL> LVC 000000000000000000000000000000 (15) BLOB <NULL> ");

		byte[] four = new byte[4];
		four[0] = (byte) 0x04;
		four[1] = (byte) 0x23;
		four[2] = (byte) 0xA2;
		four[3] = (byte) 0xFD;
		insertData(psI, 30, four, 10, true);
		showData(psS, 30, four, "ORG 0423a2fd (4) CHR 0423a2fd202020202020 (10) VAR 0423a2fd (4) LVC 0423a2fd (4) BLOB 0423a2fd (4) ");

		byte[] ten = new byte[10];
		ten[0] = (byte) 0x0B;
		ten[1] = (byte) 0x27;
		ten[2] = (byte) 0xA2;
		ten[3] = (byte) 0xFD;
		ten[4] = (byte) 0x01;
		ten[5] = (byte) 0x6D;
		ten[6] = (byte) 0xE2;
		ten[7] = (byte) 0x35;
		ten[8] = (byte) 0x66;
		ten[9] = (byte) 0x90;
		insertData(psI, 40, ten, 10, true);
		showData(psS, 40, ten, "ORG 0b27a2fd016de2356690 (10) CHR 0b27a2fd016de2356690 (10) VAR 0b27a2fd016de2356690 (10) LVC 0b27a2fd016de2356690 (10) BLOB 0b27a2fd016de2356690 (10) ");

		byte[] l15 = new byte[15];
		l15[0] = (byte) 0xEB;
		l15[1] = (byte) 0xCA;
		l15[2] = (byte) 0xFE;
		l15[3] = (byte) 0xBA;
		l15[4] = (byte) 0xBE;
		l15[5] = (byte) 0xFE;
		l15[6] = (byte) 0xED;
		l15[7] = (byte) 0xFA;
		l15[8] = (byte) 0xCE;
		l15[9] = (byte) 0x24;
		l15[10] = (byte) 0x78;
		l15[11] = (byte) 0x43;
		l15[12] = (byte) 0x92;
		l15[13] = (byte) 0x31;
		l15[14] = (byte) 0x6D;
		insertData(psI, 50, l15, 10, true);
		showData(psS, 50, l15, "ORG ebcafebabefeedface24784392316d (15) CHR <NULL> VAR <NULL> LVC ebcafebabefeedface24784392316d (15) BLOB <NULL> ");

		byte[] space4 = new byte[4];
		space4[0] = (byte) 0x20;
		space4[1] = (byte) 0x20;
		space4[2] = (byte) 0x20;
		space4[3] = (byte) 0x20;
		insertData(psI, 60, space4, 10, true);
		showData(psS, 60, space4, "ORG 20202020 (4) CHR 20202020202020202020 (10) VAR 20202020 (4) LVC 20202020 (4) BLOB 20202020 (4) ");

		byte[] space6 = new byte[6];
		space6[0] = (byte) 0xca;
		space6[1] = (byte) 0xfe;
		space6[2] = (byte) 0x20;
		space6[3] = (byte) 0x20;
		space6[4] = (byte) 0x20;
		space6[5] = (byte) 0x20;
		insertData(psI, 70, space6, 10, true);
		showData(psS, 70, space6, "ORG cafe20202020 (6) CHR cafe2020202020202020 (10) VAR cafe20202020 (6) LVC cafe20202020 (6) BLOB cafe20202020 (6) ");

		byte[] space12 = new byte[12];
		space12[0] = (byte) 0xca;
		space12[1] = (byte) 0xfe;
		space12[2] = (byte) 0x20;
		space12[3] = (byte) 0x20;
		space12[4] = (byte) 0x20;
		space12[5] = (byte) 0x20;
		space12[6] = (byte) 0xca;
		space12[7] = (byte) 0xfe;
		space12[8] = (byte) 0x20;
		space12[9] = (byte) 0x20;
		space12[10] = (byte) 0x20;
		space12[11] = (byte) 0x20;
		insertData(psI, 210, space12, 10, true);
		showData(psS, 210, space12, "ORG cafe20202020cafe20202020 (12) CHR <NULL> VAR <NULL> LVC cafe20202020cafe20202020 (12) BLOB <NULL> ");
        
        psI.close();

        Statement stmt = createStatement();

		String sql = "INSERT INTO FBDVAL.T001 VALUES(80, X'2020202020', X'2020202020', X'2020202020', null)";
		stmt.executeUpdate(sql);
		showData(psS, 80, space4, "ORG 20202020 (4) CHR 20202020202020202020 (10) VAR 2020202020 (5) LVC 2020202020 (5) BLOB <NULL> ");

		sql = "INSERT INTO FBDVAL.T001 VALUES(90, X'CAFE20202020CAFE20202020', null, null, null)";
		stmt.executeUpdate(sql);
		showData(psS, 90, space12, "ORG cafe20202020cafe20202020 (12) CHR cafe20202020cafe2020 (10) VAR <NULL> LVC <NULL> BLOB <NULL> ");

		sql = "INSERT INTO FBDVAL.T001 VALUES(100, null, X'CAFE20202020CAFE20202020', null, null)";
		stmt.executeUpdate(sql);
		showData(psS, 100, space12, "ORG cafe20202020cafe20202020 (12) CHR <NULL> VAR cafe20202020cafe2020 (10) LVC <NULL> BLOB <NULL> ");

		sql = "INSERT INTO FBDVAL.T001 VALUES(110, null, null, X'CAFE20202020CAFE20202020', null)";
		stmt.executeUpdate(sql);
		showData(psS, 110, space12, "ORG cafe20202020cafe20202020 (12) CHR <NULL> VAR <NULL> LVC cafe20202020cafe20202020 (12) BLOB <NULL> ");

		sql = "INSERT INTO FBDVAL.T001 VALUES(120, X'CAFE20202020CAFE20202020DD', null, null, null)";
		try {
			stmt.executeUpdate(sql);
		} catch (SQLException sqle) {
			assertSQLState("22001", sqle);
		}

		sql = "INSERT INTO FBDVAL.T001 VALUES(130, null, X'CAFE20202020CAFE20202020DD', null, null)";
		try {
			stmt.executeUpdate(sql);
		} catch (SQLException sqle) {
			assertSQLState("22001", sqle);
		}

		sql = "INSERT INTO FBDVAL.T001 VALUES(140, null, null, X'CAFE20202020CAFE20202020DD', null)";
		stmt.executeUpdate(sql);
		showData(psS, 140, space12, "ORG cafe20202020cafe20202020 (12) CHR <NULL> VAR <NULL> LVC cafe20202020cafe20202020dd (13) BLOB <NULL> ");

		sql = "INSERT INTO FBDVAL.X001 VALUES(200, X'CAFE20202020CAFE20202020', null, null, null)";
		stmt.executeUpdate(sql);

		sql = "INSERT INTO FBDVAL.T001 SELECT * FROM FBDVAL.X001";
		stmt.executeUpdate(sql);
		showData(psS, 200, space12, "ORG cafe20202020cafe20202020 (12) CHR cafe20202020cafe2020 (10) VAR <NULL> LVC <NULL> BLOB <NULL> ");
        
        psS.close();
        
        stmt.close();
	}


        /* testCompare fixture */
	public void testCompare() throws SQLException {
        Statement stmt = createStatement();
		stmt.execute("delete from FBDVAL.T001");
		PreparedStatement psI = prepareStatement("INSERT INTO FBDVAL.T001 VALUES(?, ?, ?, ?, ?)");
		PreparedStatement psI2 = prepareStatement("INSERT INTO FBDVAL.T002 VALUES(?, ?, ?, ?, ?)");

		insertData(psI, 0, null, 10, false);
		insertData(psI2, 0, null, 10, false);

		byte[] four = new byte[4];
		four[0] = (byte) 0x04;
		four[1] = (byte) 0x23;
		four[2] = (byte) 0xA2;
		four[3] = (byte) 0xFD;

		insertData(psI, 30, four, 10, false);
		insertData(psI2, 30, four, 10, false);
		four[2] = (byte) 0xA1;
		insertData(psI, 40, four, 10, false);
		insertData(psI2, 40, four, 10, false);
		four[2] = (byte) 0xA2;
		four[3] = (byte) 0xFF;
		insertData(psI, 50, four, 10, false);
		insertData(psI2, 50, four, 10, false);

		byte[] four_plus_space = new byte[5];
		four_plus_space[0] = (byte) 0x04;
		four_plus_space[1] = (byte) 0x23;
		four_plus_space[2] = (byte) 0xA2;
		four_plus_space[3] = (byte) 0xFD;
		four_plus_space[4] = (byte) 0x20;
		insertData(psI, 60, four_plus_space, 10, false);
		insertData(psI2, 60, four_plus_space, 10, false);

		byte[] ten = new byte[10];
		ten[0] = (byte) 0x0B;
		ten[1] = (byte) 0x27;
		ten[2] = (byte) 0xA2;
		ten[3] = (byte) 0xFD;
		ten[4] = (byte) 0x01;
		ten[5] = (byte) 0x6D;
		ten[6] = (byte) 0xE2;
		ten[7] = (byte) 0x35;
		ten[8] = (byte) 0x66;
		ten[9] = (byte) 0x90;

		insertData(psI, 70, ten, 10, false);
		insertData(psI2, 70, ten, 10, false);

		String[] COLS = {"C1", "C2", "C3", "C4"};
		String[] OPS = {"=", "<>", "<", "<=", ">", ">="};

		int i=0;
		for (int t = 0; t < COLS.length; t++) {
		    for (int o = 0; o < COLS.length; o++) {
			for (int a = 0; a < OPS.length; a++) {
			    String sql = "SELECT T.ID, T." + COLS[t] + ", O.ID, O." + COLS[o] + " FROM FBDVAL.T001 O, FBDVAL.T002 T WHERE T." + COLS[t] + " " + OPS[a] + " O." + COLS[o] + " ORDER BY 1,3";

			    try {
				PreparedStatement psS = prepareStatement(sql);
				showCompareData(psS, testCompareExpectedValues [i]);
			    } catch (SQLException sqle) {
				assertSQLState("42818", sqle);
			    }
			    i++;
			}
		    }
		}
        stmt.close();
	}

	/**
	The length of a binary type is encoded when stored, this
	test makes sure all the code paths are tested.
	The encoded length is hidden from the JDBC client.
	 */
	public void testEncodedLengths() throws SQLException, IOException {	
		PreparedStatement psi = prepareStatement("INSERT INTO FBDVAL.TEL VALUES(?, ?, ?)");
		PreparedStatement pss = prepareStatement("SELECT * FROM FBDVAL.TEL");
		PreparedStatement psd = prepareStatement("DELETE FROM FBDVAL.TEL");

		//insertEL(psi, pss, psd, 0);
		insertEL(psi, pss, psd,  10, testEncodedLengthsExpectedValues[0]);
		insertEL(psi, pss, psd,  30, testEncodedLengthsExpectedValues[0]);
		insertEL(psi, pss, psd,  31, testEncodedLengthsExpectedValues[0]);
		insertEL(psi, pss, psd,  32, testEncodedLengthsExpectedValues[0]); // switch to 2 byte length
		insertEL(psi, pss, psd,  1345, testEncodedLengthsExpectedValues[0]);
		insertEL(psi, pss, psd,  23456, testEncodedLengthsExpectedValues[0]);
		insertEL(psi, pss, psd,  32672, testEncodedLengthsExpectedValues[0]);
		insertEL(psi, pss, psd,  32700, testEncodedLengthsExpectedValues[1]);
		insertEL(psi, pss, psd,  (32*1024) - 1, testEncodedLengthsExpectedValues[2]);
		insertEL(psi, pss, psd,  (32*1024), testEncodedLengthsExpectedValues[2]);
		insertEL(psi, pss, psd,  (32*1024) + 1, testEncodedLengthsExpectedValues[2]);
		insertEL(psi, pss, psd,  (64*1024) - 1, testEncodedLengthsExpectedValues[2]);
		insertEL(psi, pss, psd,  (64*1024), testEncodedLengthsExpectedValues[2]); // switch to 4 byte length
		insertEL(psi, pss, psd,  (64*1024) + 1, testEncodedLengthsExpectedValues[2]);
		insertEL(psi, pss, psd,  (110*1024) + 3242, testEncodedLengthsExpectedValues[2]);

		psi.close();
		pss.close();
		psd.close();
	}

        /**
         * Create a suite of tests.
         **/
        public static Test suite() {
        	TestSuite suite = new TestSuite("ForBitTestData");
        	suite.addTest(baseSuite("ForBitTestData:embedded"));
        	suite.addTest(TestConfiguration.clientServerDecorator(baseSuite("ForBitTestData:client")));
        	return suite;
    	}

	protected static Test baseSuite(String name) {
        	TestSuite suite = new TestSuite(name);
        	suite.addTestSuite(ForBitDataTest.class);
        	
		return new CleanDatabaseTestSetup(suite) 
        	{
            		protected void decorateSQL(Statement s) throws SQLException
            		{
                		for (int i = 0; i < TABLES.length; i++) {
                    			s.execute(TABLES[i]);
                		}
            		}
        	};
    	} 

        /***********************************************************************************************
         * All the methods below this line are used by fixtures
         **********************************************************************************************/
	private void insertEL(PreparedStatement psi, PreparedStatement pss, PreparedStatement psd, int length, String expectedValue) throws SQLException, IOException 
	{
                byte[] data = new byte[length];

		// random simple value check
		int off = (int)  (System.currentTimeMillis() % ((long) length));
		data[off] = 0x23;

		psi.setBytes(1, (length <= 32672) ? data : null);
		psi.setBytes(2, (length <= 32700) ? data : null);
		psi.setBinaryStream(3, new java.io.ByteArrayInputStream(data), length); // BLOB column

		psi.executeUpdate();
		selectData(pss,data,off,length, expectedValue);
		psd.executeUpdate();

		// Set values using stream and then verify that select is successful
		psi.setBinaryStream(1, (length <= 32672) ? new java.io.ByteArrayInputStream(data) : null, length);
		psi.setBinaryStream(2, (length <= 32700) ? new java.io.ByteArrayInputStream(data) : null, length);
		psi.setBinaryStream(3, new java.io.ByteArrayInputStream(data), length); // BLOB column
		psi.executeUpdate();

		selectData(pss,data,off,length, expectedValue);
		psd.executeUpdate();
	}

	private void selectData(PreparedStatement pss,byte[] data,int off,int length, String expectedValue)
	throws SQLException,IOException
	{
                String actualValue = null;
		ResultSet rs = pss.executeQuery();
		while (rs.next())
		{
			byte[] v = rs.getBytes(1);
			
			if (v != null) {
				actualValue =  "C1 " + ((v.length == length) ? "OK" : ("FAIL <" + v.length + ">"));
				actualValue += " DATA " + ((v[off] == 0x23) ? "OK" : ("FAIL " + off));

			}
			else {
				actualValue = "C1 NULL";
			}
			v = rs.getBytes(2);
			if (v != null) {
				actualValue += " C2 " + ((v.length == length) ? "OK" : ("FAIL <" + v.length + ">"));
				actualValue += " DATA " + ((v[off] == 0x23) ? "OK" : ("FAIL " + off));
				
			}
			else {
				actualValue += " C2 NULL";
			}
			InputStream c3 = rs.getBinaryStream(3);
			String returnValue = checkEncodedLengthValue(" C3", c3, length, off);
			actualValue += returnValue;
			assertEquals(expectedValue, actualValue);
		}
		rs.close();

		rs = pss.executeQuery();
		while (rs.next())
		{
			actualValue = checkEncodedLengthValue("C1", rs.getBinaryStream(1), length, off) +
					     checkEncodedLengthValue(" C2", rs.getBinaryStream(2), length, off) +
                                             checkEncodedLengthValue(" C3", rs.getBinaryStream(3), length, off);									
			assertEquals(expectedValue, actualValue);
		}
		rs.close();

	}
	private String checkEncodedLengthValue(String col, InputStream is, int length, int off) throws IOException {
		String returnStr = null;
		if (is == null) {
			returnStr = col + " NULL";
			return returnStr;
		}
		byte[] buf = new byte[3213];
		boolean dataOK = false;
		int sl = 0;
		for (;;) {
			int r = is.read(buf);
			if (r < 0)
				break;

			if ((off >= sl) && (off < (sl + r))) {
				if (buf[off - sl] == 0x23)
					dataOK = true;
			}
			sl += r;
		}
		returnStr =  col + " " + ((sl == length) ? "OK" : ("FAIL <" + sl + ">"));
		returnStr += " DATA " + (dataOK ? "OK" : ("FAIL " + off));

		return returnStr;
	}

        
	private void showCompareData(PreparedStatement psS, String[] expectedValues) throws SQLException {
		ResultSet rs = psS.executeQuery();
		int i=0;
		while (rs.next()) {
                        String actualValue = rs.getInt(1) + " " + showData(rs.getBytes(2)) + "  " + rs.getInt(3) + " " + showData(rs.getBytes(4));
			assertEquals(expectedValues[i],actualValue);
			i++;
		}
		rs.close();
		psS.close();
	}

	private void insertData(PreparedStatement psI, int id, byte[] original, int maxLen, boolean streamAsWell) throws SQLException 
	{

		int ol = original == null ? 0: original.length;

		if (original == null || original.length <= maxLen) {
			// simple case.
			psI.setInt(1, id);
			psI.setBytes(2, original);
			psI.setBytes(3, original);
			psI.setBytes(4, original);
			psI.setBytes(5, original);
			psI.executeUpdate();

			if (streamAsWell) {
				psI.setInt(1, id+1);
				psI.setBinaryStream(2, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.setBinaryStream(3, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.setBinaryStream(4, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.setBinaryStream(5, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.executeUpdate();
			}
			return;
		}	

		// Insert potentially out of range value one at a time into the table
		try {
			psI.setInt(1, id);
			psI.setBytes(2, original);
			psI.setBytes(3, null);
			psI.setBytes(4, null);
			psI.setBytes(5, null);
			psI.executeUpdate();
		} catch (SQLException sqle) {
			assertSQLState("22001", sqle);
		}
		if (streamAsWell) {
			try {
				psI.setInt(1, id+1);
				psI.setBinaryStream(2, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.executeUpdate();
			} catch (SQLException sqle) {
				assertSQLState("22001", sqle);
			}

		}

		try {
			psI.setInt(1, id+2);
			psI.setBytes(2, null);
			psI.setBytes(3, original);
			psI.setBytes(4, null);
			psI.setBytes(5, null);
			psI.executeUpdate();
		} catch (SQLException sqle) {
			assertSQLState("22001", sqle);
		}
		if (streamAsWell) {
			try {
				psI.setInt(1, id+3);
				psI.setBinaryStream(3, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.executeUpdate();
			} catch (SQLException sqle) {
				assertSQLState("22001", sqle);
			}
		}


		try {
			psI.setInt(1, id+4);
			psI.setBytes(2, null);
			psI.setBytes(3, null);
			psI.setBytes(4, original);
			psI.setBytes(5, null);
			psI.executeUpdate();
		} catch (SQLException sqle) {
			assertSQLState("22001", sqle);
		}
		if (streamAsWell) {
			try {
				psI.setInt(1, id+5);
				psI.setBinaryStream(4, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.executeUpdate();
			} catch (SQLException sqle) {
				assertSQLState("22001", sqle);
			}
		}

		try {
			psI.setInt(1, id+6);
			psI.setBytes(2, null);
			psI.setBytes(3, null);
			psI.setBytes(4, null);
			psI.setBytes(5, original);
			psI.executeUpdate();
		} catch (SQLException sqle) {
			assertSQLState("22001", sqle);
		}
		if (streamAsWell) {
			try {
				psI.setInt(1, id+7);
				psI.setBinaryStream(5, original == null ? null : new ByteArrayInputStream(original), ol);
				psI.executeUpdate();
			} catch (SQLException sqle) {
				assertSQLState("22001", sqle);
			}
		}
	}

	private void showData(PreparedStatement psS, int id, byte[] original, String expectedValue) throws SQLException {
		psS.setInt(1, id);
		psS.setInt(2, id + 10);
		ResultSet rs = psS.executeQuery();
		while (rs.next()) {
			String actualValue = "ORG " + showData(original) + "";
			actualValue += "CHR " + showData(rs.getBytes(1)) + "";
			actualValue += "VAR " + showData(rs.getBytes(2)) + "";
			actualValue += "LVC " + showData(rs.getBytes(3)) + "";
			actualValue += "BLOB " + showData(rs.getBytes(4));
			assertEquals(expectedValue, actualValue);
		}
		rs.close();

	}

	private String showData(byte[] data) {
       	        if (data == null)
		   return "<NULL> ";

	        StringBuffer sb = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			String s = Integer.toHexString(data[i] & 0xff);
			if (s.length() == 1)
				sb.append('0');
			sb.append(s);
		}

		sb.append(' ');
		sb.append('(');
		sb.append(data.length);
		sb.append(')');
		sb.append(' ');

		return sb.toString();
	}

}
