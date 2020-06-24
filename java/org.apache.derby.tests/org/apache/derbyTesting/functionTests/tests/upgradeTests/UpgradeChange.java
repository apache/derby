/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.UpgradeChange

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DerbyVersion;

/**
 * Abstract class to provide support for test fixtures for
 * upgrade change testing.
 *
 */
abstract class UpgradeChange extends BaseJDBCTestCase {
      
    /**
     * Thread local for the phase of the test set.
     * Contains an Integer object.
     */
    static final ThreadLocal<Integer> phase = new ThreadLocal<Integer>();
    
    /**
     * Thread local for the old version of the engine.
     * Contains a int array with four entries corresponding
     * to the four part Derby number.
     */
    static final ThreadLocal<int[]> oldVersion = new ThreadLocal<int[]>();
    
    /**
     * SWL state thrown when a feature requires upgrade
     * to a newer version and thus cannot be run in soft
     * upgrade mode.
     */
    static final String SQLSTATE_NEED_UPGRADE = "XCL47";
    
    /**
     * Phases in upgrade test
     */
    static final String[] PHASES =
    {"CREATE", "SOFT UPGRADE", "POST SOFT UPGRADE", "UPGRADE", "POST UPGRADE"};
    
    
    /**
     * Create a database with old version
     */
    static final int PH_CREATE = 0;
    /**
     * Perform soft upgrade with new version
     */
    static final int PH_SOFT_UPGRADE = 1;
    /**
     * Boot the database with old release after soft upgrade
     */
    static final int PH_POST_SOFT_UPGRADE = 2;
    /**
     * Perform hard upgrade with new version
     */
    
    static final int PH_HARD_UPGRADE = 3;
    /**
     * Boot the database with old release after hard upgrade.
     * Expected to fail to connect, so no tests need to have
     * cases for this condition.
     */
    static final int PH_POST_HARD_UPGRADE = 4;
    
    public UpgradeChange(String name) {
        super(name);
    }

    /**
     * Get the phase of the upgrade sequence we are running.
     * One of PH_CREATE, PH_SOFT_UPGRADE, PH_POST_SOFT_UPGRADE,
     * PH_HARD_UPGRADE, PH_POST_HARD_UPGRADE.
     */
    final int getPhase()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        return phase.get();
    }
    
    /**
     * Returns a {@code DerbyVersion} object describing the old version.
     *
     * @return A version object.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-6064
    final DerbyVersion getOldVersion() {
        return new DerbyVersion(
                getOldMajor(), getOldMinor(), getOldFixPack(), getOldPoint());
    }

    /**
     * Get the major number of the old version being upgraded
     * from.
     */
    final int getOldMajor() {
        return ((int[]) oldVersion.get())[0];
    }
    
    /**
     * Get the minor number of the old version being upgraded
     * from.
     */    
    final int getOldMinor() {
        return ((int[]) oldVersion.get())[1];
    }
    
    /**
     * Get the fixpack number of the old version being upgraded
     * from.
     */    
    final int getOldFixPack() {
        return ((int[]) oldVersion.get())[2];
    }
    
    /**
     * Get the point number of the old version being upgraded
     * from.
     */    
    final int getOldPoint() {
        return ((int[]) oldVersion.get())[3];
    }
    
    /**
     * Return true if the old version is equal to or more
     * recent that the passed in major and minor version.
     */
    boolean oldAtLeast(int requiredMajor, int requiredMinor) 
    {
        if (getOldMajor() > requiredMajor)
            return true;
        if ((getOldMajor() == requiredMajor)
            && (getOldMinor() >= requiredMinor))
            return true;
        return false;
    } 

    /**
     * Return true if and only if the old version is less than the
     * specified version.
     */
    boolean oldLessThan(int major, int minor, int fixpack, int point) {
        int[] old = (int[]) oldVersion.get();
        int[] version = new int[]{major, minor, fixpack, point};

//IC see: https://issues.apache.org/jira/browse/DERBY-5105
//IC see: https://issues.apache.org/jira/browse/DERBY-4835
        for (int i = 0; i < old.length; i++) {
            if (old[i] < version[i]) return true;
            if (old[i] > version[i]) return false;
        }

        // Old version matches exactly. That is, not less than.
        return false;
    } 

    /**
     * Return true if and only if the old version is equal to the
     *  passed major, minor, fixpack and point version
     * specified version.
     */
    boolean oldIs(int requiredMajor, int requiredMinor, 
    		int requiredFixpack, int requiredPoint) {
        return (getOldMajor() == requiredMajor)
        && (getOldMinor() == requiredMinor) 
        && (getOldFixPack() == requiredFixpack)
        && (getOldPoint() == requiredPoint);
    }

    /**
     * Return true if the old version is equal
     *  the passed in major and minor version.
     */
    boolean oldIs(int requiredMajor, int requiredMinor) 
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-2217
        return (getOldMajor() == requiredMajor)
          && (getOldMinor() == requiredMinor);
     }
    
    /**
     * Pretty-print the phase.
     */
    String  getPhaseString()
    {
        return PHASES[ getPhase() ];
    }

    /**
     * Pretty-print the original version number.
     */
    String  getOldVersionString()
    {
        return "( " + getOldMajor() + ", " + getOldMinor() + ", " + getOldFixPack() + ", " + getOldPoint() + " )";
    }
}
