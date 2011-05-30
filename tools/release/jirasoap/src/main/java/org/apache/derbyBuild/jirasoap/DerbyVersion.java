/*

   Derby - Class org.apache.derbyBuild.jirasoap.DerbyVersion

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

package org.apache.derbyBuild.jirasoap;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Class representing a Derby version.
 * <p>
 * The format is major.minor.fixpack.point, for instance 10.6.2.1.
 */
//@Immutable
class DerbyVersion
        implements Comparable {

    /**
     * Shard static calendar used to format release date (this is not a
     * erformance critical class).
     */
    // GuardedBy("CAL")
    private static final Calendar CAL = GregorianCalendar.getInstance();

    /** Constant telling that a version hasn't been released. */
    public static final long NOT_RELEASED = -1;

    /** Derby version string, for instance "10.6.2.1". */
    private final String version;
    private final int major;
    private final int minor;
    private final int fixpack;
    private final int point;
    private final long releaseDate;
    private final String releaseDateStr;

    /**
     * Creates a new Derby version object.
     *
     * @param rv remote version object fetched from JIRA
     */
    public DerbyVersion(RemoteVersion rv) {
        this(rv.getName(), rv.isReleased()
                                ? rv.getReleaseDate().getTimeInMillis()
                                : NOT_RELEASED);
    }

    DerbyVersion(String version, long relDate) {
        this.version = version;
        String[] comp = version.split("\\.");
        if (comp.length != 4) {
            throw new IllegalArgumentException("invalid version: " + version);
        }
        major = Integer.parseInt(comp[0]);
        minor = Integer.parseInt(comp[1]);
        fixpack = Integer.parseInt(comp[2]);
        point = Integer.parseInt(comp[3]);
        this.releaseDate = relDate;
        if (relDate == NOT_RELEASED) {
            releaseDateStr = "n/a";
        } else {
            synchronized (CAL) {
                CAL.setTimeInMillis(relDate);
                releaseDateStr = CAL.get(Calendar.YEAR) + "-" +
                        padZero(CAL.get(Calendar.MONTH) +1) + "-" +
                        padZero(CAL.get(Calendar.DAY_OF_MONTH));
            }
        }
    }

    /**
     * Returns the Derby version string.
     *
     * @return Version string, for instance "10.6.2.1".
     */
    public String getVersion() {
        return version;
    }

    /**
     * Returns the Derby version string quoted for use in JQL.
     *
     * @return Quoted version string, for instance '"10.6.2.1"'.
     */
    public String getQuotedVersion() {
        return "\"" + getVersion() + "\"";
    }

    /**
     * Returns the release date in milliseconds since the Epoch.
     *
     * @return Release date as milliseconds since the Epoch.
     * @throws IllegalStateException if the version hasn't been released
     */
    public long getReleaseDateMillis() {
        if (!isReleased()) {
            throw new IllegalStateException("not released");
        }
        return releaseDate;
    }

    /**
     * Returns the release date formatted as a string (YYYY-MM-DD).
     *
     * @return The release date, or "n/a" if not released.
     */
    public String getFormattedReleaseDate() {
        return releaseDateStr;
    }

    /**
     * Tells if this version has been released.
     *
     * @return {@code true} if released, {@code false} if not.
     */
    public boolean isReleased() {
        return releaseDate != NOT_RELEASED;
    }

    /**
     * Tells if this version has the same fixpack as the other version.
     * <p>
     * This generally means that the two versions are release candidates for an
     * upcoming release.
     *
     * @param other other version
     * @return {@code true} if the fixpack component of the two versions are
     *      equal (in addition to the major and minor version), for instance
     *      the case for 10.6.2.1 and 10.6.2.2, {@code false} otherwise.
     *
     */
    public boolean isSameFixPack(DerbyVersion other) {
        return (major == other.major && minor == other.minor &&
                fixpack == other.fixpack);
    }

    /**
     * Compares this version to another version based on the Derby version
     * strings.
     * <p>
     * Note that this comparision doesn't take the release date into
     * consideration, but only the release string. This means that even though
     * 10.3.3.0 was released after 10.4.1.3, 10.4.1.3 will be considered
     * greater than 10.3.3.0.
     *
     * @param o other version
     * @return {@code 1} if this version is greater than the other version,
     *      {@code -1} if this version is smaller than the other version, and
     *      {@code 0} if the two versions are identical.
     */
    public int compareTo(Object o) {
        DerbyVersion other = (DerbyVersion) o;
        if (major > other.major) {
            return 1;
        }
        if (major < other.major) {
            return -1;
        }
        if (minor > other.minor) {
            return 1;
        }
        if (minor < other.minor) {
            return -1;
        }
        if (fixpack > other.fixpack) {
            return 1;
        }
        if (fixpack < other.fixpack) {
            return -1;
        }
        if (point > other.point) {
            return 1;
        }
        if (point < other.point) {
            return -1;
        }
        return 0;
    }

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

    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + this.major;
        hash = 83 * hash + this.minor;
        hash = 83 * hash + this.fixpack;
        hash = 83 * hash + this.point;
        return hash;
    }

    public String toString() {
        return version + " (" + releaseDateStr + ")";
    }

    /** Adds a leading zero if the value is less than ten. */
    private static String padZero(int val) {
        if (val < 10) {
            return "0" + Integer.toString(val);
        } else {
            return Integer.toString(val);
        }
    }
}
