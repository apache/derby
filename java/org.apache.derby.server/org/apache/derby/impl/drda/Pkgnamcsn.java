/*
 * Derby - class org.apache.derby.impl.drda.Pkgnamcsn
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.apache.derby.impl.drda;

/**
 * Class representing a PKGNAMCSN object (RDB Package Name,
 * Consistency Token, and Section Number).
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-212
final class Pkgnamcsn {
    /** Database name. */
    private final String rdbnam;
    /** RDB Package Collection Identifier. */
    private final String rdbcolid;
    /** RDB Package Identifier. */
    private final String pkgid;
    /** RDB Package Section Number. */
    private final int pkgsn;
    /** RDB Package Consistency Token. */
    private final ConsistencyToken pkgcnstkn;

    /** Object which can be used for hashing when the consistency
     * token can be ignored. */
    private Object statementKey = null;

    /**
     * Create a new <code>Pkgnamcsn</code> instance.
     *
     * @param rdbnam database name
     * @param rdbcolid RDB Package Collection Identifier
     * @param pkgid RDB Package Identifier
     * @param pkgsn RDB Package Section Number
     * @param pkgcnstkn RDB Package Consistency Token
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
    Pkgnamcsn(String rdbnam, String rdbcolid, String pkgid,
                     int pkgsn, ConsistencyToken pkgcnstkn) {
        this.rdbnam = rdbnam;
        this.rdbcolid = rdbcolid;
        this.pkgid = pkgid;
        this.pkgsn = pkgsn;
        this.pkgcnstkn = pkgcnstkn;
    }

    /**
     * Get RDBNAM.
     *
     * @return database name
     */
    public String getRdbnam() {
        return rdbnam;
    }

    /**
     * Get RDBCOLID.
     *
     * @return RDB Package Collection Identifier
     */
    public String getRdbcolid() {
        return rdbcolid;
    }

    /**
     * Get PKGID.
     *
     * @return RDB Package Identifier
     */
    public String getPkgid() {
        return pkgid;
    }

    /**
     * Get PKGSN.
     *
     * @return RDB Package Section Number
     */
    public int getPkgsn() {
        return pkgsn;
    }

    /**
     * Get PKGCNSTKN.
     *
     * @return RDB Package Consistency Token
     */
    public ConsistencyToken getPkgcnstkn() {
        return pkgcnstkn;
    }

    /**
     * Return string representation.
     *
     * @return a <code>String</code> value
     */
    public String toString() {
        return super.toString() + "(\"" + rdbnam + "\", \"" +
            rdbcolid + "\", \"" + pkgid + "\", " + pkgsn +
            ", " + pkgcnstkn + ")";
    }

    /**
     * Return an object which can be used as a key in a hash table
     * when the value of the consistency token can be ignored. The
     * object has <code>equals()</code> and <code>hashCode()</code>
     * methods which consider other objects returned from
     * <code>getStatementKey()</code> equal if RDBNAM, RDBCOLID, PKGID
     * and PKGSN are equal.
     *
     * @return an <code>Object</code> value
     * @see Database#getDRDAStatement(Pkgnamcsn)
     * @see Database#storeStatement(DRDAStatement)
     * @see Database#removeStatement(DRDAStatement)
     */
    public Object getStatementKey() {
        if (statementKey == null) {
            statementKey = new StatementKey();
        }
        return statementKey;
    }

    /**
     * Class for objects used as keys in the hash table
     * <code>stmtTable</code> found in the <code>Database</code>
     * class. The <code>equals()</code> and <code>hashCode()</code>
     * methods consider other <code>StatementKey</code> objects equal
     * to this object if they are associated with a
     * <code>Pkgnamcsn</code> object with the same values for RDBNAM,
     * RDBCOLID, PKGID and PKGSN.
     *
     * @see Database
     */
    private final class StatementKey {
        /** Cached hash code. */
        private int hash = 0;
        /**
         * Check whether RDBNAM, RDBCOLID, PKGID and PKGSN of another
         * <code>StatementKey</code> object matches this object.
         *
         * @param obj another object
         * @return true if the objects are equal
         */
        public boolean equals(Object obj) {
            if (StatementKey.this == obj) {
                return true;
            } else if (obj instanceof StatementKey) {
                return ((StatementKey) obj).isKeyFor(Pkgnamcsn.this);
            } else {
                return false;
            }
        }
        /**
         * Calculate hash code.
         *
         * @return hash code
         */
        public int hashCode() {
            if (hash == 0) {
                hash =
                    rdbnam.hashCode() ^
                    rdbcolid.hashCode() ^
                    pkgid.hashCode() ^
                    pkgsn;
            }
            return hash;
        }
        /**
         * Check whether this object can be used as a key for a
         * <code>Pkgnamcsn</code> object.
         *
         * @param p a <code>Pkgnamcsn</code> value
         * @return true if this object can be key for the
         * <code>Pkgnamcsn</code> object
         */
        private boolean isKeyFor(Pkgnamcsn p) {
            return
                rdbnam.equals(p.rdbnam) &&
                rdbcolid.equals(p.rdbcolid) &&
                pkgid.equals(p.pkgid) &&
                pkgsn == p.pkgsn;
        }
    }
}
