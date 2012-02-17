/*

   Derby - Class org.apache.derbyTesting.unitTests.junit.DerbyVersionTest

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

package org.apache.derbyTesting.unitTests.junit;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.DerbyVersion;

/**
 * Tests the functionality of {@code DerbyVersion}.
 */
public class DerbyVersionTest
        extends BaseTestCase {

    private static final DerbyVersion _10_2     = new DerbyVersion(10,2,0,0);
    private static final DerbyVersion _10_2_2_0 = new DerbyVersion(10,2,2,2);
    private static final DerbyVersion _10_3     = new DerbyVersion(10,3,0,0);
    private static final DerbyVersion _10_4     = new DerbyVersion(10,4,0,0);
    private static final DerbyVersion _10_5     = new DerbyVersion(10,5,0,0);
    private static final DerbyVersion _10_5_1_1 = new DerbyVersion(10,5,1,1);
    private static final DerbyVersion _10_5_2_0 = new DerbyVersion(10,5,2,0);
    private static final DerbyVersion _10_5_3_0 = new DerbyVersion(10,5,3,0);
    private static final DerbyVersion _10_6     = new DerbyVersion(10,6,0,0);
    private static final DerbyVersion _10_7     = new DerbyVersion(10,7,0,0);
    private static final DerbyVersion _10_8     = new DerbyVersion(10,8,0,0);
    private static final DerbyVersion _10_8_1_2 = new DerbyVersion(10,8,1,2);
    private static final DerbyVersion _10_9     = new DerbyVersion(10,9,0,0);

    private static final DerbyVersion _11_0 = new DerbyVersion(11,0,0,0);
    private static final DerbyVersion _11_9 = new DerbyVersion(11,9,9,9);


    public DerbyVersionTest(String name) {
        super(name);
    }
    public static Test suite() {
        return new TestSuite(DerbyVersionTest.class, "DerbyVersionTest tests");
    }

    public void testLessThan() {
        assertTrue(_10_2.lessThan(_10_2_2_0));
        assertTrue(_10_2.lessThan(_10_3));
        assertTrue(_10_5.lessThan(_10_5_1_1));
        assertTrue(_10_5_1_1.lessThan(_10_5_2_0));
        assertTrue(_10_5_2_0.lessThan(_10_5_3_0));
        assertTrue(_10_8_1_2.lessThan(_11_0));
        assertTrue(_10_8_1_2.lessThan(_11_9));

        assertFalse(_10_5.lessThan(_10_4));
        assertFalse(_10_5.lessThan(_10_5));
        assertFalse(_11_0.lessThan(_10_8));
        assertFalse(_11_9.lessThan(_10_7));
    }

    public void testAtLeast() {
        assertTrue(_10_4.atLeast(_10_4));
        assertTrue(_10_4.atLeast(_10_3));
        assertTrue(_10_5_2_0.atLeast(_10_5_1_1));

        assertFalse(_10_2.atLeast(_10_4));
        assertFalse(_10_2.atLeast(_11_0));
        assertFalse(_10_5_1_1.atLeast(_10_5_3_0));
    }

    public void testGreaterThan() {
        assertTrue(_10_5_3_0.greaterThan(_10_5_2_0));
        assertTrue(_10_5_3_0.greaterThan(_10_5_1_1));
        assertTrue(_10_5_3_0.greaterThan(_10_2));

        assertFalse(_10_2.greaterThan(_10_3));
        assertFalse(_10_8.greaterThan(_11_0));
    }


    public void testAtMost() {
        assertTrue(_10_4.atMost(_10_5));
        assertTrue(_10_8.atMost(_11_9));

        assertFalse(_10_7.atMost(_10_2));
        assertFalse(_11_0.atMost(_10_5_3_0));
    }

    public void testAtMajorMinor() {
        assertTrue(_10_4.atMajorMinor(10, 4));

        assertFalse(_10_2.atMajorMinor(10, 1));
        assertFalse(_10_2.atMajorMinor(10, 3));
        assertFalse(_10_2.atMajorMinor(11, 2));
    }

    public void testAtMajorMinorOf() {
        assertTrue(_10_5.atMajorMinorOf(_10_5_1_1));
        assertTrue(_10_5.atMajorMinorOf(_10_5_2_0));
        assertTrue(_10_5.atMajorMinorOf(_10_5_3_0));
        assertTrue(_10_5_3_0.atMajorMinorOf(_10_5_3_0));
        assertTrue(_10_5_3_0.atMajorMinorOf(_10_5_1_1));

        assertFalse(_10_5_2_0.atMajorMinorOf(_10_3));
        assertFalse(_10_5_2_0.atMajorMinorOf(_11_9));
        assertFalse(_10_5_2_0.atMajorMinorOf(_10_2));
        assertFalse(_10_5_2_0.atMajorMinorOf(_10_2_2_0));
    }

    public void testGetMajor() {
        assertEquals(10, _10_5_1_1.getMajor());
        assertEquals(10, _10_8.getMajor());
        assertEquals(11, _11_9.getMajor());
    }

    public void testGetMinor() {
        assertEquals(5, _10_5_1_1.getMinor());
        assertEquals(8, _10_8.getMinor());
        assertEquals(9, _11_9.getMinor());
    }

    public void testGetFixpack() {
        assertEquals(1, _10_5_1_1.getFixpack());
        assertEquals(0, _10_8.getFixpack());
        assertEquals(9, _11_9.getFixpack());
    }

    public void testGetPoint() {
        assertEquals(1, _10_5_1_1.getPoint());
        assertEquals(0, _10_8.getPoint());
        assertEquals(9, _11_9.getPoint());
    }

    public void testCreateGet() {
        DerbyVersion v = new DerbyVersion(1, 2, 3, 4);
        assertEquals(1, v.getMajor());
        assertEquals(2, v.getMinor());
        assertEquals(3, v.getFixpack());
        assertEquals(4, v.getPoint());
    }

    public void testCompareTo() {
        assertTrue(_10_5_1_1.compareTo(_10_2) > 0);
        assertTrue(_10_5_1_1.compareTo(_10_8) < 0);
        assertTrue(_11_0.compareTo(_11_9) < 0);

        assertEquals(0, _11_0.compareTo(_11_0));
        assertEquals(0, _10_2.compareTo(_10_2));
    }

    public void testEquals() {
        assertTrue(_10_6.equals(_10_6));

        assertFalse(_10_2.equals(this));
        assertFalse(_10_2.equals(null));
        assertFalse(_11_0.equals(_10_8));
        assertFalse(_10_5_2_0.equals(_10_5_3_0));
    }

    public void testParseString() {
        DerbyVersion dv = DerbyVersion.parseVersionString(
                "10.9.0.0 alpha - (1180861M)");
        assertTrue(dv.equals(_10_9));
        assertEquals(0, dv.compareTo(_10_9));

        dv = DerbyVersion.parseVersionString("10.8.1.2");
        assertTrue(dv.equals(_10_8_1_2));
        assertEquals(0, dv.compareTo(_10_8_1_2));
        
        dv = DerbyVersion.parseVersionString("   10.8.1.2   ");
        assertTrue(dv.equals(_10_8_1_2));
        assertEquals(0, dv.compareTo(_10_8_1_2));

        try {
            dv = DerbyVersion.parseVersionString("10.8.1");
            fail("should have failed");
        } catch (IllegalArgumentException iae) {
            // As expected.
        }

        try {
            dv = DerbyVersion.parseVersionString("10.8.1.");
            fail("should have failed");
        } catch (IllegalArgumentException iae) {
            // As expected.
        }

        try {
            dv = DerbyVersion.parseVersionString("10.8.1.two");
            fail("should have failed");
        } catch (IllegalArgumentException iae) {
            // As expected.
        }
    }
}

