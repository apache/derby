/*

   Derby - Class org.apache.derby.client.am.MaterialStatement

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;

import java.util.ArrayList;



public interface MaterialStatement {
    public abstract void writeExecuteImmediate_(String sql, Section section) throws SqlException;

    public abstract void readExecuteImmediate_() throws SqlException;

    // The sql parameter is supplied in the read method for drivers that
    // process all commands on the "read-side" and do little/nothing on the "write-side".
    // Drivers that follow the write/read paradigm (e.g. NET) will likely ignore the sql parameter.
    public abstract void readExecuteImmediateForBatch_(String sql) throws SqlException;

    public abstract void writePrepareDescribeOutput_(String sql, Section section) throws SqlException;

    public abstract void readPrepareDescribeOutput_() throws SqlException;

    public abstract void writeOpenQuery_(Section section,
                                         int fetchSize,
                                         int resultSetType) throws SqlException;

    public abstract void readOpenQuery_() throws SqlException;

    public abstract void writeExecuteCall_(boolean outputExpected,
                                           String procedureName,
                                           Section section,
                                           int fetchSize,
                                           boolean suppressResultSets, // for batch updates set to true, otherwise to false
                                           int resultSetType,
                                           ColumnMetaData parameterMetaData,
                                           Object[] inputs) throws SqlException;

    public abstract void readExecuteCall_() throws SqlException;

    // Used for re-prepares across commit and other places as well
    public abstract void writePrepare_(String sql, Section section) throws SqlException;

    public abstract void readPrepare_() throws SqlException;

    public abstract void writeSetSpecialRegister_(
        Section section, ArrayList sqlsttList) throws SqlException;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

    public abstract void readSetSpecialRegister_() throws SqlException;

    public abstract void reset_();

}

