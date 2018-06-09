/*

   Derby - Class org.apache.derby.impl.sql.compile.SubqueryList

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
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * A SubqueryList represents a list of subqueries within a specific clause
 * (select, where or having) in a DML statement.  It extends QueryTreeNodeVector.
 *
 */

class SubqueryList extends QueryTreeNodeVector<SubqueryNode>
{
    SubqueryList(ContextManager cm) {
        super(SubqueryNode.class, cm);
    }

	/**
	 * Add a subquery to the list.
	 *
	 * @param subqueryNode	A SubqueryNode to add to the list
	 *
	 */

    void addSubqueryNode(SubqueryNode subqueryNode) throws StandardException
	{
		addElement(subqueryNode);
	}

	/**
	 * Optimize the subqueries in this list.  
	 *
	 * @param dataDictionary	The data dictionary to use for optimization
	 * @param outerRows			The optimizer's estimate of the number of
	 *							times this subquery will be executed.
	 *
	 * @exception StandardException		Thrown on error
	 */

    void optimize(DataDictionary dataDictionary, double outerRows)
			throws StandardException
	{
        for (SubqueryNode sqn : this)
		{
            sqn.optimize(dataDictionary, outerRows);
		}
	}

	/**
	 * Modify the access paths for all subqueries in this list.
	 *
	 * @see ResultSetNode#modifyAccessPaths
	 *
	 * @exception StandardException		Thrown on error
	 */
    void modifyAccessPaths()
			throws StandardException
	{
        for (SubqueryNode sqn : this)
		{
            sqn.modifyAccessPaths();
		}
	}

	/**
	 * Search to see if a query references the specifed table name.
	 *
	 * @param name		Table name (String) to search for.
	 * @param baseTable	Whether or not name is for a base table
	 *
	 * @return	true if found, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
    boolean referencesTarget(String name, boolean baseTable)
		throws StandardException
	{
        for (SubqueryNode sqn : this)
		{
            if (sqn.isMaterializable())
			{
				continue;
			}

            if (sqn.getResultSet().referencesTarget(name, baseTable))
			{
				return true;
			}
		}

		return false;
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean referencesSessionSchema()
		throws StandardException
	{
        for (SubqueryNode sqn : this)
		{
            if (sqn.referencesSessionSchema())
			{
				return true;
			}
		}

		return false;
	}

	/**
	 * Set the point of attachment in all subqueries in this list.
	 *
	 * @param pointOfAttachment		The point of attachment
	 *
	 * @exception StandardException			Thrown on error
	 */
    void setPointOfAttachment(int pointOfAttachment)
		throws StandardException
	{
        for (SubqueryNode sqn : this)
		{
            sqn.setPointOfAttachment(pointOfAttachment);
		}
	}

	/**
	 * Decrement (query block) level (0-based) for 
	 * all of the tables in this subquery list.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
	void decrementLevel(int decrement)
	{
        for (SubqueryNode sqn : this)
		{
            sqn.getResultSet().decrementLevel(decrement);
		}
	}

	/**
     * Mark all of the subqueries in this 
     * list as being part of a having clause,
     * so we can avoid flattening later.
	 * 
	 */
    void markHavingSubqueries() {
        for (SubqueryNode sqn : this)
	    {
            sqn.setHavingSubquery(true);
	    }
	}

	/**
	 * Mark all of the subqueries in this list as being part of a where clause
	 * so we can avoid flattening later if needed.
	 */
    void markWhereSubqueries() {
        for (SubqueryNode sqn : this)
		{
            sqn.setWhereSubquery(true);
		}
	}
}

