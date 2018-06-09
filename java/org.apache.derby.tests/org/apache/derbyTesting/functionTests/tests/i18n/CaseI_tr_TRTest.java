/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.i8n.caseI_tr_TR
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.derbyTesting.functionTests.tests.i18n;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public class CaseI_tr_TRTest extends BaseJDBCTestCase {

    public CaseI_tr_TRTest(String name) {
        super(name);
    }
 
    /**
     * Test Turkish I casing.  Turkish has two i's. lower case i upper cases 
     * to a upper case i with a dot. Lowercase i with no dot uppercases to I with
     * no dot.
     * @throws SQLException
     */
    public void testTurkishIcase() throws SQLException {
        PreparedStatement ps = prepareStatement("values UCASE(?)");
        ps.setString(1, "i");
        //\u0130 is upper case i with a dot
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "\u0130");
 
        // \u0131 is lower case i no dot
        ps.setString(1, "\u0131");       
        JDBC.assertSingleValueResultSet(ps.executeQuery(),
                "I");

        PreparedStatement ps2 = prepareStatement("values LCASE(?)");
        ps2.setString(1, "I");
        JDBC.assertSingleValueResultSet(ps2.executeQuery(),
                "\u0131");
        
        ps2.setString(1, "\u0130");
        JDBC.assertSingleValueResultSet(ps2.executeQuery(),
                "i");
        

 
        
    }
    
    public static Test suite() {
        Test test = TestConfiguration.defaultSuite(CaseI_tr_TRTest.class);
        Properties attributes = new Properties();
        attributes.put("territory","tr_TR");
        return Decorator.attributesDatabase(attributes, test);
    }
}

    
