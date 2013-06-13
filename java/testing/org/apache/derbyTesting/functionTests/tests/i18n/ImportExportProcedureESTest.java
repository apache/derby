/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.i18n.ImportExportProcedureESTest

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

 */
package org.apache.derbyTesting.functionTests.tests.i18n;

import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.tests.tools.ImportExportProcedureTest;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ImportExportProcedureESTest extends ImportExportProcedureTest {
    
    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public ImportExportProcedureESTest(String name)
    {
        super(name);
    }
    
    /**
     * Run tests from iepnegative.sql with es_MX locale.
     * These  have already been converted in tools/ImportExportProcedureTest
     * So we will just run that whole test in es_MX
     */
    public static Test suite() {        
        Test test = TestConfiguration.embeddedSuite(ImportExportProcedureTest.class);
        Properties attributes = new Properties();
        attributes.put("territory","es_MX");
        test = Decorator.attributesDatabase(attributes, test);
        return new SupportFilesSetup(test, new String[] { 
                "functionTests/testData/ImportExport/db2ttypes.del",
                "functionTests/testData/ImportExport/mixednl.del",
                "functionTests/testData/ImportExport/position_info.del"
        });
    }
}
