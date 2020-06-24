/*

   Derby - Class org.apache.derby.iapi.sql.execute.TargetResultSet

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

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.types.RowLocation;

/**
 * The TargetResultSet interface is used to provide additional
 * operations on result sets that are the target of a bulk insert 
 * or update.  This is useful because bulk insert is upside down -
 * the insert is done via the store.
 *
 */
public interface TargetResultSet extends ResultSet
{
	/**
	 * Pass a changed row and the row location for that row
	 * to the target result set.
	 *
	 * @param execRow		The changed row.
	 * @param rowLocation	The row location of the row.
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public void changedRow(ExecRow execRow, RowLocation rowLocation) throws StandardException;

    public void offendingRowLocation(
//IC see: https://issues.apache.org/jira/browse/DERBY-532
            RowLocation rl, long constainerId) throws StandardException;
	/**
	 * Preprocess the source row prior to getting it back from the source.
	 * This is useful for bulk insert where the store stands between the target and 
	 * the source.
	 *
	 * @param sourceRow	The source row.
	 *
	 * @return The preprocessed source row.
	 *
	 * @exception StandardException thrown if cursor finished.
	 */
	public ExecRow preprocessSourceRow(ExecRow sourceRow) throws StandardException;
}
