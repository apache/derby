/*

   Derby - Class org.apache.derby.client.am.StatementCallbackInterface

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



// Methods implemented by the common Statement class to handle
// certain events that may originate from the material or common layers.
//
// Reply implementations may update statement state via this interface.
//

public interface StatementCallbackInterface extends UnitOfWorkListener {
    // A query has been opened on the server.
    public void completeOpenQuery(Sqlca sqlca, ClientResultSet resultSet)
            throws DisconnectException;
//IC see: https://issues.apache.org/jira/browse/DERBY-6125

    public void completeExecuteCallOpenQuery(
        Sqlca sqlca,
        ClientResultSet resultSet,
        ColumnMetaData resultSetMetaData,
        Section generatedSection);

    // Chains a warning onto the statement.
    public void accumulateWarning(SqlWarning e);

    public void completePrepare(Sqlca sqlca);

    public void completePrepareDescribeOutput(ColumnMetaData columnMetaData, Sqlca sqlca);

    public void completeExecuteImmediate(Sqlca sqlca);

    public void completeExecuteSetStatement(Sqlca sqlca);


    public void completeExecute(Sqlca sqlca);

    public void completeExecuteCall(
//IC see: https://issues.apache.org/jira/browse/DERBY-6125
        Sqlca sqlca,
        Cursor params,
        ClientResultSet[] resultSets);

    public void completeExecuteCall(Sqlca sqlca, Cursor params);

    public int completeSqlca(Sqlca sqlca);

    public ConnectionCallbackInterface getConnectionCallbackInterface();

    public ColumnMetaData getGuessedResultSetMetaData();
}
