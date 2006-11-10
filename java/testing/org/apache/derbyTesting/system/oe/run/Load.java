/*
 * 
 * Derby - Class Load
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.derbyTesting.system.oe.run;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBCPerfTestCase;


/**
 * Driver to do the load phase. (partial)
 * This class currently only creates the schema. 
 * After the population code is checked in, one can measure performance
 * using 1) load data after creating constraints 2) load data and then create
 * constraints.
 */ 
public class Load extends JDBCPerfTestCase{
    
    
    
    /**
     * Create a test case with the given name.
     *
     * @param name of the test case.
     */
    public Load(String name)
    {
        super(name);
    }
    
    /**
     * junit tests to do the OE load.
     * @return the tests to run
     */
    public static Test suite()
    { 
        TestSuite suite = new TestSuite("OE_Load");
        suite.addTest(new Load("createSchemaWithConstraints"));        
        
        // more to come. (see DERBY-1987)
        // need to add the population phase.
        
        return suite;
        
    }
    
    /**
     * Create tables, primary, foreign key constraints 
     * and indexes for the OE benchmark
     */
    public void createSchemaWithConstraints()
    throws Exception
    {
        int numExceptions = 0;
        numExceptions += runScript("org/apache/derbyTesting/system/oe/schema/schema.sql","US-ASCII");
        numExceptions += runScript("org/apache/derbyTesting/system/oe/schema/primarykey.sql","US-ASCII");
        numExceptions += runScript("org/apache/derbyTesting/system/oe/schema/foreignkey.sql","US-ASCII");
        numExceptions += runScript("org/apache/derbyTesting/system/oe/schema/index.sql","US-ASCII");
        assertEquals("Number of sql exceptions ",0,numExceptions);
    }
    
}
