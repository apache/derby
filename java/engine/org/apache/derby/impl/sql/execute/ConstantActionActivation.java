/*

   Derby - Class org.apache.derby.impl.sql.execute.ConstantActionActivation

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.ResultSet;

/**
	A pre-compiled activation that supports a single ResultSet with
	a single constant action. All the execution logic is contained
	in the constant action.
    <P>
    At compile time for DDL statements this class will be picked
    as the implementation of Activation. The language PreparedStatement
    will contain the ConstantAction created at compiled time.
    At execute time this class then fetches a language ResultSet using
    ResultSetFactory.getDDLResultSet and executing the ResultSet
    will invoke the execute on the ConstantAction.

 */
public final class ConstantActionActivation extends BaseActivation
{
    /**
     * Always return false since constant actions don't need recompilation
     * when the row counts change.
     */
    protected boolean shouldWeCheckRowCounts() {
        return false;
    }

    protected ResultSet createResultSet() throws StandardException {
        return getResultSetFactory().getDDLResultSet(this);
    }

	public void postConstructor(){}
}
