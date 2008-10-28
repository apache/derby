/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.RunTimeStatisticsParser
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
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.sql.Connection;
import java.util.HashSet;
import java.util.StringTokenizer;

public class RuntimeStatisticsParser {

    private int isolationLevel = Connection.TRANSACTION_NONE;
    private boolean distinctScan = false;
    private boolean eliminatedDuplicates = false;
    private boolean tableScan = false;
    private final boolean indexScan;
    private final boolean indexRowToBaseRow;
	private final boolean lastKeyIndexScan;
    private String statistics = "";
    private boolean scrollInsensitive = false;
    private final HashSet qualifiers;

    /**
     * Create a RuntimeStatistics object to parse the text and extract
     * information.
     * 
     * @param rts
     *            Runtime Statistics string
     * 
     */
    public RuntimeStatisticsParser(String rts) {
    	statistics = rts;
        if (rts.indexOf(" at serializable isolation level ") != -1)
            isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
        else if (rts.indexOf("at read uncommitted isolation level") != -1)
            isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
        else if (rts.indexOf("at read committed isolation level") != -1)
            isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        else if (rts.indexOf("at repeatable read isolation level") != -1)
            isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;

        if (rts.indexOf("Distinct Scan ResultSet") > 0) {
        	distinctScan = true;
        }
        
        if (rts.indexOf("Table Scan ResultSet") > 0) {
        	tableScan = true;
        }

        indexScan = (rts.indexOf("Index Scan ResultSet") >= 0);
        indexRowToBaseRow =
            (rts.indexOf("Index Row to Base Row ResultSet") >= 0);
        lastKeyIndexScan = (rts.indexOf("Last Key Index Scan ResultSet") >= 0);
        
        if (rts.indexOf("Eliminate duplicates = true") > 0) {
        	eliminatedDuplicates = true;
        }
        if (rts.indexOf("Scroll Insensitive ResultSet:") > 0)
            scrollInsensitive = true;

        qualifiers = findQualifiers();
    }

    /**
     * Class which represents a qualifier used in a scan.
     */
    private static class Qualifier {
        String operator;
        boolean negated;
        Qualifier(String operator, boolean negated) {
            this.operator = operator;
            this.negated = negated;
        }
        public int hashCode() {
            if (negated) {
                return ~(operator.hashCode());
            }
            return operator.hashCode();
        }
        public boolean equals(Object o) {
            if (o instanceof Qualifier) {
                Qualifier q = (Qualifier) o;
                return (negated == q.negated) && operator.equals(q.operator);
            }
            return false;
        }
        /**
         * Represent the qualifier as a string for debugging.
         */
        public String toString() {
            return (negated ? "\u00ac" : "" ) + operator;
        }
    }

    /**
     * Find all qualifiers in a query plan.
     *
     * @return set of <code>Qualifier</code>s
     */
    private HashSet findQualifiers() {
        HashSet set = new HashSet();
        int startPos = statistics.indexOf("qualifiers:\n");
        if (startPos >= 0) {
            // start search after "qualifiers:\n"
            String searchString = statistics.substring(startPos + 12);
            StringTokenizer t = new StringTokenizer(searchString, "\n");
            while (t.hasMoreTokens()) {
                String s = t.nextToken();
                if (s.startsWith("Operator: ")) {
                    String operator = s.substring(10);
                    t.nextToken();  // skip "Ordered nulls: ..."
                    t.nextToken();  // skip "Unknown return value: ..."
                    s = t.nextToken();
                    if (!s.startsWith("Negate comparison result: ")) {
                        throw new AssertionError(
                            "Expected to find \"Negate comparison result\"");
                    }
                    boolean negated =
                        Boolean.valueOf(s.substring(26)).booleanValue();
                    set.add(new Qualifier(operator, negated));
                }
            }
        }
        return set;
    }

    /**
     * @return Isolation level from parsed RuntimeStatistics
     */
    public int getIsolationLevel() {
        return isolationLevel;
    }
    
    /**
     * Return whether or not a Distinct Scan result set was used in the
     * query.
     */
    public boolean usedDistinctScan() {
    	return distinctScan;
    }
    
    /**
     * Return whether or not a Table Scan result set was used in the
     * query.
     */
    public boolean usedTableScan() {
    	return tableScan;
    }

    /**
     * Return whether or not an index scan result set was used in the query.
     */
    public boolean usedIndexScan() {
        return indexScan;
    }

    /**
     * Return whether or not a last key index scan result set was used
	 * in the query. A last key index scan is a special optimization for
	 * MIN and MAX queries against an indexed column (SELECT MAX(ID) FROM T).
     */
    public boolean usedLastKeyIndexScan() {
        return lastKeyIndexScan;
    }

    /**
     * Return whether or not an index row to base row result set was used in
     * the query.
     */
    public boolean usedIndexRowToBaseRow() {
        return indexRowToBaseRow;
    }
    
    /**
     * Return whether or not the query involved a sort that eliminated
     * duplicates
     */
    public boolean eliminatedDuplicates() {
    	return eliminatedDuplicates;
    }
    
    public boolean isScrollInsensitive(){
        return scrollInsensitive;
    }

    /**
     * Return whether or not the query used a &gt;= scan qualifier.
     */
    public boolean hasGreaterThanOrEqualQualifier() {
        // < negated is equivalent to >=
        return qualifiers.contains(new Qualifier("<", true));
    }

    /**
     * Return whether or not the query used a &lt; scan qualifier.
     */
    public boolean hasLessThanQualifier() {
        return qualifiers.contains(new Qualifier("<", false));
    }

    /**
     * Return whether or not the query plan includes a line of the form
     *
     *   "Number of rows qualified=n"
     *
     * where "n" is the received qualRows argument.  Note that this
     * method will return true if the above string is found anywhere
     * in the query plan.  For queries which specifying more than one
     * table, more advanced parsing will be required to associate the
     * number of rows qualified with the correct table.
     */
    public boolean rowsQualifiedEquals(int qualRows)
    {
        return (statistics.indexOf("Number of rows qualified=" +
            qualRows + "\n") != -1);
    }
}
