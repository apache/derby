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

public class RuntimeStatisticsParser {

    private int isolationLevel = Connection.TRANSACTION_NONE;
    private boolean distinctScan = false;
    private boolean eliminatedDuplicates = false;
    private boolean tableScan = false;
    private String statistics = "";
    private boolean scrollInsensitive = false;

    /**
     * Create a RuntimeStatistics object to parse the text and extract
     * information. Currently only isolation level is the only thing extracted.
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
        
        if (rts.indexOf("Eliminate duplicates = true") > 0) {
        	eliminatedDuplicates = true;
        }
        if (rts.indexOf("Scroll Insensitive ResultSet:") > 0)
            scrollInsensitive = true;
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
     * Return whether or not the query involved a sort that eliminated
     * duplicates
     */
    public boolean eliminatedDuplicates() {
    	return eliminatedDuplicates;
    }
    
    public boolean isScrollInsensitive(){
        return scrollInsensitive;
    }
    
}
