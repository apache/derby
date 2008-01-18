/*
 
 Derby - Class org.apache.derby.iapi.jdbc.EngineResultSet
 
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
package org.apache.derby.iapi.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.derby.iapi.error.StandardException;
/**
 * Additional methods the embedded engine exposes on its ResultSet object
 * implementations. An internal api only, mainly for the network
 * server
 */
public interface EngineResultSet extends ResultSet {
    
    /**
     * Is this result set from a select for update statement?
     */
    public boolean isForUpdate();
    
    /**
     * Is the designated columnIndex a null data value?
     * This is used by EXTDTAInputStream to get the null value without 
     * retrieving the underlying data value.
     * @param columnIndex
     * @return true if the data value at columnIndex for the current row is null 
     * @throws SQLException 
     */
    public boolean isNull(int columnIndex) throws SQLException;
    
    /**
     * Return the length of the designated columnIndex data value.
     * Implementation is type dependent.
     * 
     * @param columnIndex  column to access
     * @return length of data value
     * @throws SQLException
     * @see org.apache.derby.iapi.types.DataValueDescriptor#getLength() 
     */
    public int getLength(int columnIndex) throws SQLException;
    
    /**
     * Fetch the holdability of this ResultSet which may be different
     * from the holdability of its Statement.
     * @return HOLD_CURSORS_OVER_COMMIT or CLOSE_CURSORS_AT_COMMIT
     * @throws SQLException Error.
     */
    public int getHoldability() throws SQLException;
    
}
