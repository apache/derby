/*

   Derby - Class org.apache.derbyTesting.functionTests.util.T_Access

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.util;

import org.apache.derby.iapi.error.StandardException; 
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.error.PublicAPI;
import java.sql.SQLException;

/**
   This class provides mechanism to call access Factory methods  from sql-j.
  */

public class T_Access
{

	public static AccessFactory getAccessFactory() throws SQLException
	{
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		LanguageConnectionFactory lcf = lcc.getLanguageConnectionFactory();
		return (AccessFactory)lcf.getAccessFactory();
	}

	/*
	 *Followng call waits until post commit thread queue is empty.
	 *This call is useful for tests which checks for the following type
	 *of cases:
	 *  1) Checking for space usage after delete statements
	 *  2) Checking for locks when delete statements are involved,
	 *     because post commit thread might be holding locks when
	 *     checking for snap shot of locks, so best thing to do
	 *     to get consistent results is to call the following function
	 *     before checking for locks (eg: store/updatelocks.sql)
	 *  3) Depending on whethere the  space is not released yet by the post commit thread
	 *     for commited deletes or not can change the order of rows in the heap.
	 *     In such cases , it is good idea to call this method before doing
	 *     inserts(Even adding/dropping constraints can have effect because they
	 *     do inderectly deletes/inserts on system tables.) eg: lang/fk_nonsps.sql
	 */
	public static void waitForPostCommitToFinish() throws SQLException
	{
			AccessFactory af = getAccessFactory();
			af.waitForPostCommitToFinishWork();
	}
}

