/*

   Derby - Class org.apache.derbyTesting.unitTests.store.T_ColumnOrderingImpl

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

package org.apache.derbyTesting.unitTests.store;

import org.apache.derbyTesting.unitTests.harness.T_MultiThreadedIterations;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.store.access.*;

// used by unit tests, that needs  to simulate
//  ColumnOrdering  data type parameter from the language layer.

public class T_ColumnOrderingImpl implements ColumnOrdering
{
	int columnId;
	boolean isAscending;

	public	T_ColumnOrderingImpl(int columnId, boolean isAscending)
	{
		this.columnId = columnId;
		this.isAscending = isAscending;
	}

	/*
	 * Methods of ColumnOrdering
	 */

	/**
	@see ColumnOrdering#getColumnId
	**/
	public int getColumnId()
	{
		return this.columnId;
	}

	/**
	@see ColumnOrdering#getIsAscending
	**/
	public boolean getIsAscending()
	{
		return this.isAscending;
	}

	/**
	@see ColumnOrdering#getIsNullsOrderedLow
	**/
	public boolean getIsNullsOrderedLow()
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-2887
		return false;
	}
}

