/*

   Derby - Class org.apache.derbyTesting.junit.Version

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

import java.util.StringTokenizer;

/**
 * A generic class for storing a major and minor version number.
 * <p>
 * This class assumes that more capable versions compare greater than less
 * capable versions.
 *
 * @see DerbyVersion 
 */
public final class Version
        implements Comparable {
    private final int _major;
    private final int _minor;

    Version(int major, int minor) {
        this._major = major;
        this._minor = minor;
    }

    public Version(String desc)
            throws NumberFormatException {
        StringTokenizer tokens = new StringTokenizer( desc, "." );
        this._major = Integer.parseInt(tokens.nextToken());
        this._minor = Integer.parseInt(tokens.nextToken());
    }

    /**
     * Returns {@code true} if this version is at least as advanced
     * as the other version.
     */
    public boolean atLeast(Version that) {
        return this.compareTo( that ) > -1;
    }


    ////////////////////////////////////////////////////////
    //
    //    Comparable BEHAVIOR
    //
    ////////////////////////////////////////////////////////

    public int compareTo(Object o) {
        return compareTo((Version)o);
    }

    public int compareTo(Version that) {
        if ( this._major < that._major ) { return -1; }
        if ( this._major > that._major ) { return 1; }

        return this._minor - that._minor;
    }

    ////////////////////////////////////////////////////////
    //
    //    Object OVERLOADS
    //
    ////////////////////////////////////////////////////////

    public String toString() {
        return Integer.toString( _major ) + '.' + Integer.toString( _minor );
    }

    public boolean equals(Object other) {
        if (other instanceof Version) {
            return compareTo((Version)other) == 0;
        } else {
            return false;
        }
    }

    public int hashCode() {
        return _major ^ _minor;
    }
}
