/*
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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import org.apache.derby.shared.common.reference.JDBC40Translation;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;
import junit.framework.Test;

/**
 * JUnit test which checks that the constants in JDBC40Translation are
 * correct. Each constant in JDBC40Translation should have a test
 * method comparing it to the value in the java.sql interface.
 */
public class JDBC40TranslationTest extends BaseTestCase {

    public JDBC40TranslationTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(JDBC40TranslationTest.class);
    }

    public void testDatabaseMetaDataFUNCTION_PARAMETER_UNKNOWN() {
        assertEquals(DatabaseMetaData.functionColumnUnknown,
                     JDBC40Translation.FUNCTION_PARAMETER_UNKNOWN);
    }

    public void testDatabaseMetaDataFUNCTION_PARAMETER_IN() {
        assertEquals(DatabaseMetaData.functionColumnIn,
                     JDBC40Translation.FUNCTION_PARAMETER_IN);
    }

    public void testDatabaseMetaDataFUNCTION_PARAMETER_INOUT() {
        assertEquals(DatabaseMetaData.functionColumnInOut,
                     JDBC40Translation.FUNCTION_PARAMETER_INOUT);
    }

    public void testDatabaseMetaDataFUNCTION_PARAMETER_OUT() {
        assertEquals(DatabaseMetaData.functionColumnOut,
                     JDBC40Translation.FUNCTION_PARAMETER_OUT);
    }

    public void testDatabaseMetaDataFUNCTION_RETURN() {
        assertEquals(DatabaseMetaData.functionReturn,
                     JDBC40Translation.FUNCTION_RETURN);
    }

    public void testDatabaseMetaDataFUNCTION_RESULT_UNKNOWN() {
        assertEquals(DatabaseMetaData.functionResultUnknown,
                     JDBC40Translation.FUNCTION_RESULT_UNKNOWN);
    }

    public void testDatabaseMetaDataFUNCTION_NO_TABLE() {
        assertEquals(DatabaseMetaData.functionNoTable,
                     JDBC40Translation.FUNCTION_NO_TABLE);
    }

    public void testDatabaseMetaDataFUNCTION_RETURNS_TABLE() {
        assertEquals(DatabaseMetaData.functionReturnsTable,
                     JDBC40Translation.FUNCTION_RETURNS_TABLE);
    }

    public void testDatabaseMetaDataFUNCTION_NO_NULLS() {
        assertEquals(DatabaseMetaData.functionNoNulls,
                     JDBC40Translation.FUNCTION_NO_NULLS);
    }

    public void testDatabaseMetaDataFUNCTION_NULLABLE() {
        assertEquals(DatabaseMetaData.functionNullable,
                     JDBC40Translation.FUNCTION_NULLABLE);
    }

    public void testDatabaseMetaDataFUNCTION_NULLABLE_UNKNOWN() {
        assertEquals(DatabaseMetaData.functionNullableUnknown,
                     JDBC40Translation.FUNCTION_NULLABLE_UNKNOWN);
    }

    public void testTypesNCHAR() {
        assertEquals(Types.NCHAR, JDBC40Translation.NCHAR);
    }

    public void testTypesNVARCHAR() {
        assertEquals(Types.NVARCHAR, JDBC40Translation.NVARCHAR);
    }

    public void testTypesLONGNVARCHAR() {
        assertEquals(Types.LONGNVARCHAR, JDBC40Translation.LONGNVARCHAR);
    }

    public void testTypesNCLOB() {
        assertEquals(Types.NCLOB, JDBC40Translation.NCLOB);
    }

    public void testTypesROWID() {
        assertEquals(Types.ROWID, JDBC40Translation.ROWID);
    }

    public void testTypesSQLXML() {
        assertEquals(Types.SQLXML, JDBC40Translation.SQLXML);
    }
}
