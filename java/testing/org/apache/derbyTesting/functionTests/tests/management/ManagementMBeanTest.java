/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.management.ManagementMBeanTest

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

package org.apache.derbyTesting.functionTests.tests.management;

import java.util.Set;

import javax.management.ObjectName;

import junit.framework.Test;


/**
 * <p>
 * Test the ManagementMBean interface provided by Derby
 * which has two implementations. A built in one and
 * one that can be created by a user.
 * </p>
 *
 * <p>
 * If you set the debug flag (-Dderby.tests.debug=true), then the test
 * will print out the number of MBeans which it finds. This should be
 * EXPECTED_BEAN_COUNT but could be something greater if MBeans
 * are left hanging around from other tests.
 * </p>
 */
public class ManagementMBeanTest extends MBeanTest {

    private static final String MANAGEMENT = "Management";
    private static final String VERSION = "Version";

    // 1 NetworkServer, 1 JDBC, 2 Version, 2 Management beans
    private static final int EXPECTED_BEAN_COUNT = 6;
    
    // MBean names
    private static final String[] MBEAN_TYPES =
    {
        "NetworkServer",
        MANAGEMENT,
        "JDBC",
        VERSION,
    };
    
    public ManagementMBeanTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        
        return MBeanTest.suite(ManagementMBeanTest.class, 
                                        "ManagementMBeanTest");
    }
    
    /**
     * Test that the MBean created by the application can
     * successfully start and stop Derby's JMX management.
     */
    public void testStartStopManagementFromApplication()
        throws Exception
    {
        ObjectName appMgmtBean = getApplicationManagementMBean();
        startStopManagement(appMgmtBean);
    }
    
    /**
     * Test that the MBean with the passed in name can
     * successfully start and stop Derby's JMX management.
     */
    private void startStopManagement(ObjectName mbean) throws Exception
    {
        // Test fixtures start off active
        assertBooleanAttribute(true, mbean, "ManagementActive");

        // may include MBeans left over from other engines which ran
        // in earlier tests
        StatsTuple originalStats = getCurrentStats( "Original" );

        assertTrue("DerbyMBeanCount:" + originalStats.getBeanCount(), originalStats.getBeanCount() >= EXPECTED_BEAN_COUNT );
        
        // Should be a no-op
        invokeOperation(mbean, "startManagement");
        assertBooleanAttribute(true, mbean, "ManagementActive");
        
        // so should have the same number of MBeans registered
        StatsTuple nopStats = getCurrentStats( "NOP" );

        compareStats( originalStats, nopStats );
        
        // now stop management
        invokeOperation(mbean, "stopManagement");
        assertBooleanAttribute(false, mbean, "ManagementActive");
        
        // the stop should have brought down 1 JDBC bean, 1 NetworkServer bean
        // and 2 Version beans. it should have left 2 Management beans standing.
        StatsTuple afterStopping = getCurrentStats( "After Stopping" );
        
        int[] expectedCounts = new int[ MBEAN_TYPES.length ];
        for ( int i = 0; i < MBEAN_TYPES.length; i++ )
        {
            int expectedDifference = 1;

            if ( MANAGEMENT.equals( MBEAN_TYPES[ i ] ) ) { expectedDifference = 0; }
            else if ( VERSION.equals( MBEAN_TYPES[ i ] ) ) { expectedDifference = 2; }

            expectedCounts[ i ] = originalStats.typeCounts[ i ] - expectedDifference;
        }
        StatsTuple expectedStats = new StatsTuple( null, expectedCounts );

        compareStats( expectedStats, afterStopping );
        
        // now start management again and have the original MBeans.
        invokeOperation(mbean, "startManagement");
        assertBooleanAttribute(true, mbean, "ManagementActive");
        
        StatsTuple afterRestarting = getCurrentStats( "After Restarting" );

        compareStats( originalStats, afterRestarting );
    }

    /**
     * Get information on the current MBeans.
     */
    private StatsTuple getCurrentStats( String tag ) throws Exception
    {
        Set<ObjectName> beanNames = getDerbyDomainMBeans();
        StatsTuple retval = new StatsTuple( beanNames, countMBeanTypes( beanNames ) );

        println( tag + " bean count = " + retval.getBeanCount() );

        return retval;
    }


    /**
     * Verify that the mbean information is what we expect.
     */
    private void compareStats( StatsTuple expected, StatsTuple actual ) throws Exception
    {
        assertEquals( expected.getBeanCount(), actual.getBeanCount() );

        for ( int i = 0; i < MBEAN_TYPES.length; i++ )
        {
            assertEquals( MBEAN_TYPES[ i ], expected.typeCounts[ i ], actual.typeCounts[ i ] );
        }
    }

    /**
     * Count the number of MBeans per type.
     */
    private int[] countMBeanTypes( Set<ObjectName> names ) throws Exception
    {
        int[] retval = new int[ MBEAN_TYPES.length ];

        for (ObjectName name : names)
        {
            String beanType = name.getKeyProperty("type");

            for ( int i = 0; i < MBEAN_TYPES.length; i++ )
            {
                if ( MBEAN_TYPES[ i ].equals( beanType ) ) { retval[ i ]++; }
            }
        }

        return retval;
    }

    private static final class StatsTuple
    {
        Set<ObjectName> beanNames;
        int[]  typeCounts;

        public StatsTuple( Set<ObjectName> beanNames, int[] typeCounts )
        {
            this.beanNames = beanNames;
            this.typeCounts = typeCounts;
        }

        public int getBeanCount()
        {
            int total = 0;

            for ( int i = 0; i < typeCounts.length; i++ ) { total += typeCounts[ i ]; }

            return total;
        }
    }

}
