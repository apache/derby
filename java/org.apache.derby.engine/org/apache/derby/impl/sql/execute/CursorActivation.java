/*

   Derby - Class org.apache.derby.impl.sql.execute.CursorActivation

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

/**
 *
 * In the family of activation support classes,
 * this one provides an activation with a cursor name.
 *
 */
public abstract class CursorActivation 
	extends BaseActivation
{
	/**
	 * remember the cursor name
	 */
	public void	setCursorName(String cursorName) 
	{
		if (!isClosed())
			super.setCursorName(cursorName);
	}

	/**
	 * @see org.apache.derby.iapi.sql.Activation#isCursorActivation
	 */
	public boolean isCursorActivation()
	{
		return true;
	}

    /** @see BaseActivation#decorateResultSet */
    ResultSet decorateResultSet() throws StandardException {
        // CursorActivation means it's a query that returns rows. Check that
        // the caller is authorized to run SQL read operations.
        getLanguageConnectionContext().getAuthorizer().authorize(
                this, Authorizer.SQL_SELECT_OP);

        // The top-level result set should be marked as such.
        NoPutResultSet rs = (NoPutResultSet) createResultSet();
        rs.markAsTopResultSet();
        return rs;
    }
}
