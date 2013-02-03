/*

   Derby - Class org.apache.derbyTesting.junit.DerbyVersion

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

package org.apache.derbyTesting.junit;

/**
 * Representation of a Derby version on the form major.minor.fixpack.point,
 * for instance "10.8.1.2".
 * <p>
 * This class doesn't consider the alpha/beta flag nor the revision number.
 */
public class DerbyVersion
        implements Comparable {

    // A list of all known "branches" (major and minor levels).
    public static final DerbyVersion _10_0     = new DerbyVersion(10,0,0,0);
    public static final DerbyVersion _10_1     = new DerbyVersion(10,1,0,0);
    public static final DerbyVersion _10_2     = new DerbyVersion(10,2,0,0);
    public static final DerbyVersion _10_3     = new DerbyVersion(10,3,0,0);
    public static final DerbyVersion _10_4     = new DerbyVersion(10,4,0,0);
    public static final DerbyVersion _10_5     = new DerbyVersion(10,5,0,0);
    public static final DerbyVersion _10_6     = new DerbyVersion(10,6,0,0);
    public static final DerbyVersion _10_7     = new DerbyVersion(10,7,0,0);
    public static final DerbyVersion _10_8     = new DerbyVersion(10,8,0,0);
    public static final DerbyVersion _10_9     = new DerbyVersion(10,9,0,0);

    // A list of all known official Derby releases.

    /** 10.0.2.1 (incubator release) */
    public static final DerbyVersion _10_0_2_1 = new DerbyVersion(10,0,2,1);
    /** 10.1.1.0 (Aug 3, 2005 / SVN 208786) */
    public static final DerbyVersion _10_1_1_0 = new DerbyVersion(10,1,1,0);
    /** 10.1.2.1 (Nov 18, 2005 / SVN 330608) */
    public static final DerbyVersion _10_1_2_1 = new DerbyVersion(10,1,2,1);
    /** 10.1.3.1 (Jun 30, 2006 / SVN 417277) */
    public static final DerbyVersion _10_1_3_1 = new DerbyVersion(10,1,3,1);
    /** 10.2.1.6 (Oct 02, 2006 / SVN 452058) */
    public static final DerbyVersion _10_2_1_6 = new DerbyVersion(10,2,1,6);
    /** 10.2.2.0 (Dec 12, 2006 / SVN 485682) */
    public static final DerbyVersion _10_2_2_0 = new DerbyVersion(10,2,2,0);
    /** 10.3.1.4 (Aug 1, 2007 / SVN 561794) */
    public static final DerbyVersion _10_3_1_4 = new DerbyVersion(10,3,1,4);
    /** 10.3.3.0 (May 12, 2008 / SVN 652961) */
    public static final DerbyVersion _10_3_3_0 = new DerbyVersion(10,3,3,0);
    /** 10.4.1.3 (April 24, 2008 / SVN 648739) */
    public static final DerbyVersion _10_4_1_3 = new DerbyVersion(10,4,1,3);
    /** 10.4.2.0 (September 05, 2008 / SVN 693552) */
    public static final DerbyVersion _10_4_2_0 = new DerbyVersion(10,4,2,0);
    /** 10.5.1.1 (April 28, 2009 / SVN 764942) */
    public static final DerbyVersion _10_5_1_1 = new DerbyVersion(10,5,1,1);
    /** 10.5.3.0 (August 21, 2009 / SVN 802917) */
    public static final DerbyVersion _10_5_3_0 = new DerbyVersion(10,5,3,0);
    /** 10.6.1.0 (May 18, 2010/ SVN 938214) */
    public static final DerbyVersion _10_6_1_0 = new DerbyVersion(10,6,1,0);
    /** 10.6.2.1 (Oct 6, 2010/ SVN 999685) */
    public static final DerbyVersion _10_6_2_1 = new DerbyVersion(10,6,2,1);
    /** 10.7.1.1 (December 14, 2010/ SVN 1040133) */
    public static final DerbyVersion _10_7_1_1 = new DerbyVersion(10,7,1,1);
    /** 10.8.1.2 (April 29, 2011/ SVN 1095077) */
    public static final DerbyVersion _10_8_1_2 = new DerbyVersion(10,8,1,2);
    /** 10.8.2.2 (October 24, 2011/ SVN 1181258) */
    public static final DerbyVersion _10_8_2_2 = new DerbyVersion(10,8,2,2);
    /** 10.8.3.0 (November 16, 2012/ SVN 1405108) */
    public static final DerbyVersion _10_8_3_0 = new DerbyVersion(10,8,3,0);
    /** 10.9.1.0 (June 25, 2012/ SVN 1344872) */
    public static final DerbyVersion _10_9_1_0 = new DerbyVersion(10,9,1,0);

    private final int major;
    private final int minor;
    private final int fixpack;
    private final int point;
    private final Version simpleVersion;

    /**
     * Parses the given string as a Derby version.
     *
     * @param versionString the string to parse, for instance "10.7.1.1" or
     *      "10.9.0.0 alpha - (1180861M)"
     * @return A Derby version object.
     * @throws IllegalArgumentException if the specified version string cannot
     *      be parsed
     */
    public static DerbyVersion parseVersionString(String versionString)
            throws IllegalArgumentException {
        String[] components = Utilities.split(versionString.trim(), ' ');
        components = Utilities.split(components[0], '.');
        if (components.length != 4) {
            throw new IllegalArgumentException(
                    "invalid number of version components, got " +
                    components.length + " expected 4: " + versionString);
        }
        return new DerbyVersion(
                Integer.parseInt(components[0]),
                Integer.parseInt(components[1]),
                Integer.parseInt(components[2]),
                Integer.parseInt(components[3]));
    }

    public DerbyVersion(int major, int minor, int fixpack, int point) {
        this.major = major;
        this.minor = minor;
        this.fixpack = fixpack;
        this.point = point;
        this.simpleVersion = new Version(major, minor);
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getFixpack() {
        return fixpack;
    }

    public int getPoint() {
        return point;
    }

    public boolean lessThan(DerbyVersion other) {
        return compareTo(other) < 0;
    }

    public boolean greaterThan(DerbyVersion other) {
        return compareTo(other) > 0;
    }

    /**
     * Checks if this version is at a greater minor level than the other
     * version.
     *
     * @param other version to compare with
     * @return {@code true} if the minor level is greater, {@code false} if the
     *      minor level is the same or smaller.
     * @throws IllegalArgumentException if the major level of the two versions
     *      are unequal
     */
    public boolean greaterMinorThan(DerbyVersion other) {
        if (!sameMajorAs(other)) {
            throw new IllegalArgumentException(
                    "major versions must be equal");
        }
        return minor > other.minor;
    }

    /**
     * Checks if this version is at the same or higher level as the other
     * version.
     *
     * @param other version to compare with
     * @return {@code true} if this version is equal to or higher than
     *      {@code other}, {@code false} otherwise.
     */
    public boolean atLeast(DerbyVersion other) {
        return compareTo(other) >= 0;
    }

    /**
     * Checks if this version is at the same or lower level as the other
     * version.
     *
     * @param other version to compare with
     * @return {@code true} if this version is equal to or lower than
     *      {@code other}, {@code false} otherwise.
     */
    public boolean atMost(DerbyVersion other) {
        return compareTo(other) <= 0;
    }

    /**
     * Checks if this version is at the same major and minor level as the other
     * version.
     *
     * @param major major level to compare with
     * @param minor minor level to compare with
     * @return {@code true} if major and minor level of the two versions are
     *      the same, {@code false} otherwise.
     */
    public boolean atMajorMinor(int major, int minor) {
        return this.major == major && this.minor == minor;
    }

    /**
     * Checks if this version is at the same major and minor level as the other
     * version.
     *
     * @param other version to compare with
     * @return {@code true} if major and minor level of the two versions are
     *      the same, {@code false} otherwise.
     */
    public boolean atMajorMinorOf(DerbyVersion other) {
        return major == other.major && minor == other.minor;
    }

    //@Override
    public String toString() {
        return major + "." + minor + "." + fixpack + "." + point;
    }

    //@Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final DerbyVersion other = (DerbyVersion) obj;
        if (this.major != other.major) {
            return false;
        }
        if (this.minor != other.minor) {
            return false;
        }
        if (this.fixpack != other.fixpack) {
            return false;
        }
        if (this.point != other.point) {
            return false;
        }
        return true;
    }

    //@Override
    public int hashCode() {
        int hash = 3;
        hash = 23 * hash + this.major;
        hash = 23 * hash + this.minor;
        hash = 23 * hash + this.fixpack;
        hash = 23 * hash + this.point;
        return hash;
    }

    public int compareTo(Object o) {
        return compareTo((DerbyVersion)o);
    }

    public int compareTo(DerbyVersion o) {
        if (major < o.major) {
            return -1;
        } else if (major > o.major) {
            return 1;
        }
        if (minor < o.minor) {
            return -1;
        } else if (minor > o.minor) {
            return 1;
        }
        if (fixpack < o.fixpack) {
            return -1;
        } else if (fixpack > o.fixpack) {
            return 1;
        }
        if (point < o.point) {
            return -1;
        } else if (point > o.point) {
            return 1;
        }
        return 0;
    }

    /**
     * Returns a simplified view of this version, where only the major and the
     * minor versions are included.
     * <p>
     * Introduced for compatibility with existing/older test code.
     *
     * @return A simplified version view.
     */
    public Version asSimpleVersion() {
        return this.simpleVersion;
    }

    /**
     * Checks if the major level of this version is the same as for the other
     * version.
     *
     * @param other version to compare with
     * @return {@code true} if the major levels match, {@code false} otherwise.
     */
    private boolean sameMajorAs(DerbyVersion other) {
        return major == other.major;
    }
}

