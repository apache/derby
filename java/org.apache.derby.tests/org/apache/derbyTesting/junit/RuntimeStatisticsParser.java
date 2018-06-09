/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.RuntimeStatisticsParser
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
    private final HashSet<Qualifier> qualifiers;
    private String [] startPosition = {"None"};
    private String [] stopPosition = {"None"};

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
        
        startPosition = getStartPosition();
        stopPosition = getStopPosition();
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
    private HashSet<Qualifier> findQualifiers() {
        HashSet<Qualifier> set = new HashSet<Qualifier>();
        int startPos = statistics.indexOf("qualifiers:\n");
        if (startPos >= 0) {
            // start search after "qualifiers:\n"
            String searchString = statistics.substring(startPos + 12);
            StringTokenizer t = new StringTokenizer(searchString, "\n");

            while (t.hasMoreTokens()) {
                String s = t.nextToken();
                StringTokenizer t2 = new StringTokenizer(s, "\t ");

                if (t2.nextToken().equals("Operator:")) {
                    String operator = t2.nextToken();

                    t.nextToken();  // skip "Ordered nulls: ..."
                    t.nextToken();  // skip "Unknown return value: ..."
                    s = t.nextToken();

                    t2 = new StringTokenizer(s, "\t ");
                    String neg = t2.nextToken();

                    if (!neg.equals("Negate")) {
                        throw new AssertionError(
                            "Expected to find \"Negate comparison result\", " +
                            "found: " + neg);
                    }
                    t2.nextToken(); // skip "comparison"
                    t2.nextToken(); // skip "result:"

                    boolean negated =
                        Boolean.valueOf(t2.nextToken()).booleanValue();
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
     * @param tableName
     * @return true if a Table Scan ResultSet was used for tableName
     */
    public boolean usedTableScan(String tableName){
        return (statistics.indexOf("Table Scan ResultSet for " + 
                    tableName + " ")!= -1);
    }

    /**
     * @param tableName
     * @param indexName
     * @return true if passed indexName was used for Index Scan ResultSet 
     *     for the passed tableName
     */
    public boolean usedSpecificIndexForIndexScan(
    		String tableName, String indexName){
        return (statistics.indexOf("Index Scan ResultSet for " + 
                    tableName + " using index " + indexName + " ")!= -1);
    }

    /**
     * @param tableName
     * @return true if an Index Scan ResultSet was used for tableName
     */
    public boolean usedIndexScan(String tableName){
        return (statistics.indexOf("Index Scan ResultSet for " + 
                    tableName + " ")!= -1);
    }
    
    /**
     * @param tableName
     * @return true if passed indexName was used for Index Scan ResultSet 
     *     for the passed tableName
     */
    public boolean usedConstraintForIndexScan(String tableName){
        return (statistics.indexOf("Index Scan ResultSet for " + 
                    tableName + " using constraint")!= -1);
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
     * @param tableName
     * @return true if Index Row to Base Row ResultSet was used for tableName
     */
    public boolean usedIndexRowToBaseRow(String tableName) {
       
            return (statistics.indexOf("Index Row to Base Row ResultSet for " + 
                        tableName + ":")!= -1);       
    }
        
    /**
     * @param tableName
     * @return true if Used Distinct Scan ResultSet for tablenName
     */
    public boolean usedDistinctScan(String tableName) {
        return (statistics.indexOf("Distinct Scan ResultSet for " + 
                tableName + " ")!= -1);

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
     * Return whether or not the query used an equals scan qualifier.
     */
    public boolean hasEqualsQualifier() {
        return qualifiers.contains(new Qualifier("=", false));
    }
    
    /**
     * Return whether there are no qualifiers (i.e. qualifiers: None)
     */
    public boolean hasNoQualifiers() {
        int startPos = statistics.indexOf("qualifiers:\n");
        if (startPos >= 0) {
            // start search after "qualifiers:\n"
            String searchString = statistics.substring(startPos + 12);
            if (searchString.indexOf("None")>1)
                return true;
            else
            {
                System.out.println("statistics.substring: " + searchString);
                return false;
            }
        }
        else {
            throw new AssertionError(
                    "Expected to find \"qualifiers: None\", " +
                    "but didn't even find 'qualifiers'");
        }
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
    
    /**
     * @return true if a hash join was used
     */
    public boolean usedHashJoin()
    {
        return (statistics.indexOf("Hash Join ResultSet") != -1);
    }

    /**
     * @return true if a nested loop left outer join was used
     */
    public boolean usedNLLeftOuterJoin()
    {
        return (statistics.indexOf("Nested Loop Left Outer Join") != -1);
    }

    /**
     * Check if an exists join (or a not exists join) was used.
     *
     * @return {@code true} if the query used a (not) exists join
     */
    public boolean usedExistsJoin() {
        return statistics.indexOf("Exists Join ResultSet") != -1;
    }

    /**
     * Search the RuntimeStatistics for a string.  It must occur
     * at least instances times.
     * @param stringToFind the string to search for
     * @param instances the minimum number of occurrences of the string
     * @return true if stringToFind is found at least {@code instances} times
     */
    public boolean findString(String stringToFind, int instances)
    {
        int foundCount=0;
        int currentOffset=0;
        String stat = statistics;
        for (int i = 0; i < instances; i++) {
            currentOffset = stat.indexOf(stringToFind);
            if (currentOffset != -1) {
                foundCount++;
                stat = stat.substring(currentOffset + stringToFind.length());
            } else {
                break;
            }
        }
        return (foundCount >= instances);
    }

    /**
     * Check if sorting node was added for the query.
     * @return true if sorting node was required
     */
    public boolean whatSortingRequired() {
        return (statistics.indexOf("Sort information: ") != -1 );
    }

    public boolean usedExternalSort() {
        return (statistics.indexOf("Sort type=external") != -1 );
    }

    public String toString() {
        return statistics;
    }
    
    /**
     * Find the start position ; sometimes using a scan start / stop is
     * a way of doing qualifiers using an index
     * @return the String array following start position:
     */
    public String [] getStartPosition() {
        int startStartIndex = statistics.indexOf("start position:");
        int endStartIndex = statistics.indexOf("stop position:");
        if (startStartIndex >= 0 && endStartIndex >= 0)
        {
            String positionLines = statistics.substring(startStartIndex, endStartIndex);
            
            return Utilities.split(positionLines, '\n');
        }
        else 
            return null;
        
    }

    /**
     * Find the stop position ; sometimes using a scan start / stop is
     * a way of doing qualifiers using an index
     * @return the String array following start position:
     */
    public String [] getStopPosition() {
        int startStopIndex = statistics.indexOf("stop position:");
        int endStopIndex = statistics.indexOf("qualifiers:");
        if (startStopIndex >= 0 && endStopIndex >= 0)
        {
            String positionLines = statistics.substring(startStopIndex, endStopIndex);
            
            return Utilities.split(positionLines, '\n');
        }
        else 
            return null;
    }

    /**
     * Assert that a sequence of string exists in the statistics.
     * <p>/
     * The strings in the argument are each assumed to start a line. Leading
     * underscores are converted to tab characters before comparing.
     *
     * @param strings The sequence of string expected to be found.
     */
    public void assertSequence(String[] strings) {

        // Make strings ready for comparison:
        for (int i=0; i < strings.length; i++) {
            StringBuffer sb = new StringBuffer();

            sb.append('\n');
            
            for (int j=0; j < strings[i].length(); j++) {
                if (strings[i].charAt(j) == '_') {
                    // this would mess up if the string has an _ somewhere in
                    // the middle, e.g. if a table name has an _ in it. So, 
                    // only do this for the first 15 characters.
                    if (j < 15)
                        sb.append('\t');
                    else
                        sb.append(strings[i].substring(j));                        
                } else {
                    sb.append(strings[i].substring(j));
                    break;
                }
            }
            strings[i] = sb.toString();
        }

        String window = statistics;
        for (int i = 0; i < strings.length; i++) {
            int pos = window.indexOf(strings[i]);

            if (pos == -1) {
                throw new AssertionError(
                    "Sequence " + strings[i] + "not found in statistics");
            }

            window = window.substring(pos + 1);
        }
    }     
}
    
