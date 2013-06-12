/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.i18.I18NImportExport
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * i18n governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.i18n;

import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;

import junit.framework.Test;
import junit.framework.TestSuite;

public class I18NImportExport extends ScriptTestCase {
    
 
    /*
     * A single JUnit test that runs a single SQL script.
     */
    private I18NImportExport(String i18NImportExport){
        super(i18NImportExport);
    }
    
    public static Test suite() {
        
        TestSuite suite = new TestSuite("I18NImportExport");
        suite.addTest(
                new CleanDatabaseTestSetup(
                new I18NImportExport("I18NImportExport")));
        
        return getIJConfig(new SupportFilesSetup(suite, new String[] {
                "functionTests/tests/i18n/data/Tab1_fr.ctrl",
                "functionTests/tests/i18n/data/Tab1_il.ctrl",
                "functionTests/tests/i18n/data/Tab1_jp.ctrl" } ));
    }
}
