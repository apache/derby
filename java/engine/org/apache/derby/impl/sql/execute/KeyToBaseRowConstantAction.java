/*

   Derby - Class org.apache.derby.impl.sql.execute.KeyToBaseRowConstantAction

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute; 

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.io.FormatableBitSet;

public interface KeyToBaseRowConstantAction
extends ConstantAction
{
	public ExecRow getEmptyHeapRow(LanguageConnectionContext lcc)
		 throws StandardException;
	public ExecRow getEmptyKeyRow()
		 throws StandardException;
	public long getKeyConglomId()
		 throws StandardException;
	public long getBaseTableConglomId()
		 throws StandardException;
	public UUID getTableId() 
		 throws StandardException;
	public FormatableBitSet getBaseRowReadList()
		 throws StandardException;
}
