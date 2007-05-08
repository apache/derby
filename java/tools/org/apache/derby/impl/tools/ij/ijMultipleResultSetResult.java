/*

Derby - Class org.apache.derby.impl.tools.ij.ijResultSetResult

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derby.impl.tools.ij;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;

import java.util.List;
import java.util.ArrayList;

/**
 * This impl is intended to be used with multiple resultsets, where
 * the execution of the statement is already complete.
 */
public class ijMultipleResultSetResult extends ijResultImpl {

    List resultSets = null;

    int[] displayColumns = null;
    int[] columnWidths = null;

    /**
     * Create a ijResultImpl that represents multiple result set.
     */
    public ijMultipleResultSetResult(List resultSets) throws SQLException {
        resultSets = new ArrayList();
        resultSets.addAll(resultSets);
    }

    /**
     * Create a ijResultImpl that represents multiple result sets, only
     * displaying a subset of the columns, using specified column widths.
     * 
     * @param resultSets The result sets to display
     * @param display Which column numbers to display, or null to display
     *                all columns.
     * @param widths  The widths of the columns specified in 'display', or
     *                null to display using default column sizes.
     */
    public ijMultipleResultSetResult(List resultSets, int[] display,
                                     int[] widths) throws SQLException {
        this.resultSets = new ArrayList();
        this.resultSets.addAll(resultSets);

        displayColumns = display;
        columnWidths   = widths;
    }


    public void addResultSet(ResultSet rs){
        resultSets.add(rs);
    }

    public boolean isMultipleResultSetResult(){
        return true;
    }

    public List getMultipleResultSets() {
        return resultSets;
    }

    public void closeStatement() throws SQLException {
        if (resultSets != null) {
            ResultSet rs = null;
            for (int i = 0; i<resultSets.size(); i++){
                rs = (ResultSet)resultSets.get(i);
                if(rs.getStatement() != null) rs.getStatement().close();
                else rs.close(); 
            }
        }
    }

    public int[] getColumnDisplayList() { return displayColumns; }
    public int[] getColumnWidthList() { return columnWidths; }

    /**
     * @return the warnings from all resultsets as one SQLWarning chain
     */
    public SQLWarning getSQLWarnings() throws SQLException { 
        SQLWarning warning = null;
        ResultSet rs = null;
        for (int i=0; i<resultSets.size(); i++){
            rs = (ResultSet)resultSets.get(i);
            if (rs.getWarnings() != null) {
                if (warning == null) warning = rs.getWarnings();
                else                 warning.setNextWarning(rs.getWarnings());
            }
        }
        return warning;
    }
    
    /**
     * Clears the warnings in all resultsets
     */
    public void clearSQLWarnings() throws SQLException {
        for (int i=0; i<resultSets.size(); i++){
            ((ResultSet)resultSets.get(i)).clearWarnings();
        }
    }
}
