/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.Test_6496

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
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
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Test the loading of optional tools when the CompilerContext is not available
 * at execution time. See DERBY-6496.
 * </p>
 */
public class Test_6496 extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";

    private static  final   String      LOAD_METADATA_TOOL = "call syscs_util.syscs_register_tool( 'databaseMetaData', true )";
    private static  final   String      UNLOAD_METADATA_TOOL = "call syscs_util.syscs_register_tool( 'databaseMetaData', false )";

    private static  final   String      LOAD_OPTIMIZER_TOOL = "call syscs_util.syscs_register_tool('optimizerTracing', true, 'custom', 'org.apache.derbyTesting.functionTests.tests.lang.DummyOptTrace')";
    private static  final   String      UNLOAD_OPTIMIZER_TOOL = "call syscs_util.syscs_register_tool('optimizerTracing', false)";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new instance.
     */

    public Test_6496(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(Test_6496.class);

        return suite;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that we can load the metadata tool twice without popping an NPE.
     * </p>
     */
    public  void    test_001_metadata()
        throws Exception
    {
        metadataTester();
    }

    /**
     * <p>
     * Test that we can load the metadata tool twice without popping an NPE.
     * </p>
     */
    public  void    test_002_metadata()
        throws Exception
    {
        metadataTester();
    }
    private void    metadataTester() throws Exception
    {
        toolTester( LOAD_METADATA_TOOL, UNLOAD_METADATA_TOOL );
    }

    /**
     * <p>
     * Test that we can load the optimizer tool twice without popping an NPE.
     * </p>
     */
    public  void    test_003_optimizer()
        throws Exception
    {
        optimizerTester();
    }

    /**
     * <p>
     * Test that we can load the optimizer tool twice without popping an NPE.
     * </p>
     */
    public  void    test_004_optimizer()
        throws Exception
    {
        optimizerTester();
    }
    private void    optimizerTester() throws Exception
    {
        toolTester( LOAD_OPTIMIZER_TOOL, UNLOAD_OPTIMIZER_TOOL );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private void    toolTester( String loadTool, String unloadTool ) throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );

        goodStatement( dboConnection, loadTool );
        goodStatement( dboConnection, unloadTool );
    }
}
