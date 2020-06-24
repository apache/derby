/*

   Derby - Class org.apache.derby.impl.sql.compile.AllResultColumn

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * An AllResultColumn represents a "*" result column in a SELECT
 * statement.  It gets replaced with the appropriate set of columns
 * at bind time.
 *
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class AllResultColumn extends ResultColumn
{
	private TableName		tableName;

    AllResultColumn(TableName tableName, ContextManager cm) {
        super(cm);
        this.tableName = tableName;
    }
	/** 
	 * Return the full table name qualification for this node
	 *
	 * @return Full table name qualification as a String
	 */
    String getFullTableName()
	{
		if (tableName == null)
		{
			return null;
		}
		else
		{
			return tableName.getFullTableName();
		}
	}

	/**
	 * Make a copy of this ResultColumn in a new ResultColumn
	 *
	 * @return	A new ResultColumn with the same contents as this one
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	ResultColumn cloneMe() throws StandardException
	{
		if (SanityManager.DEBUG)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
			SanityManager.ASSERT(getColumnDescriptor() == null,
					"columnDescriptor is expected to be non-null");
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        return new AllResultColumn(tableName, getContextManager());
	}


    @Override
    public TableName getTableNameObject() {
//IC see: https://issues.apache.org/jira/browse/DERBY-13
        return tableName;
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (tableName != null) {
            tableName = (TableName) tableName.accept(v);
        }
    }
}
