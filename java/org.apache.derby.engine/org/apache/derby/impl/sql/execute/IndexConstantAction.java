/*

   Derby - Class org.apache.derby.impl.sql.execute.IndexConstantAction

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

import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 *	This class is the superclass for the classes that describe actions 
 *  that are ALWAYS performed for a CREATE/DROP INDEX Statement at Execution time.
 *
 */

public abstract class IndexConstantAction extends DDLSingleTableConstantAction
{

	String				indexName;
	String				tableName;
	String				schemaName;

    /** Set by CreateConstraintConstantAction */
    protected transient   UUID    constraintID;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a CREATE/DROP INDEX statement.
	 *
	 *	@param	tableId				The table uuid
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName					Schema that index lives in.
	 *
	 */
	protected	IndexConstantAction(
								UUID				tableId,
								String				indexName,
								String				tableName,
								String schemaName)
	{
		super(tableId);
		this.indexName = indexName;
		this.tableName = tableName;
		this.schemaName = schemaName;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(schemaName != null, "Schema name is null");
		}
	}

    // CLASS METHODS

	/**
	  *	Get the index name.
	  *
	  *	@return	the name of the index
	  */
    public	String	getIndexName() { return indexName; }

	/**
	 * Set the index name at execution time.
	 * Useful for unnamed constraints which have a backing index.
	 *
	 * @param indexName		The (generated) index name.
	 */
	public void setIndexName(String indexName)
	{
		this.indexName = indexName;
	}

    /**
     * Set the id for the constraint which may be driving this index action.
     * This is called by CreateConstraintConstantAction.
     * @param constraintID The id of the constraint
     */
    public void setConstraintID(UUID constraintID) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6670
//IC see: https://issues.apache.org/jira/browse/DERBY-6665
        this.constraintID = constraintID;
    }

}
