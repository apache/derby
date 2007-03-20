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

    /**
     * Create a RuntimeStatistics object to parse the text and extract
     * information. Currently only isolation level is the only thing extracted.
     * 
     * @param rts
     *            Runtime Statistics string
     * 
     */
    public RuntimeStatisticsParser(String rts) {
        if (rts.indexOf(" at serializable isolation level ") != -1)
            isolationLevel = Connection.TRANSACTION_SERIALIZABLE;
        else if (rts.indexOf("at read uncommitted isolation level") != -1)
            isolationLevel = Connection.TRANSACTION_READ_UNCOMMITTED;
        else if (rts.indexOf("at read committed isolation level") != -1)
            isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        else if (rts.indexOf("at repeatable read isolation level") != -1)
            isolationLevel = Connection.TRANSACTION_REPEATABLE_READ;

    }

    /**
     * @return Isolation level from parsed RuntimeStatistics
     */
    public int getIsolationLevel() {
        return isolationLevel;
    }
    
}
