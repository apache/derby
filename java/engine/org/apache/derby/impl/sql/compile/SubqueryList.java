/*

   Derby - Class org.apache.derby.impl.sql.compile.SubqueryList

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A SubqueryList represents a list of subquerys within a specific clause 
 * (select, where or having) in a DML statement.  It extends QueryTreeNodeVector.
 *
 * @author Jerry Brenner
 */

public class SubqueryList extends QueryTreeNodeVector
{

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			SubqueryNode	subqueryNode;

			super.printSubNodes(depth);

			for (int index = 0; index < size(); index++)
			{
				subqueryNode = (SubqueryNode) elementAt(index);
				subqueryNode.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Add a subquery to the list.
	 *
	 * @param subqueryNode	A SubqueryNode to add to the list
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void addSubqueryNode(SubqueryNode subqueryNode) throws StandardException
	{
		addElement(subqueryNode);
	}

	/**
	 * Preprocess a SubqueryList.  For now, we just preprocess each SubqueryNode
	 * in the list.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return None.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void preprocess(int numTables,
							FromList outerFromList,
							SubqueryList outerSubqueryList,
							PredicateList outerPredicateList) 
				throws StandardException
	{
		SubqueryNode	subqueryNode;

		int size = size();
		for (int index = 0; index < size; index++)
		{
			subqueryNode = (SubqueryNode) elementAt(index);
			subqueryNode.preprocess(numTables, outerFromList,
									outerSubqueryList,
									outerPredicateList);
		}
	}

	/**
	 * Optimize the subqueries in this list.  
	 *
	 * @param dataDictionary	The data dictionary to use for optimization
	 * @param outerRows			The optimizer's estimate of the number of
	 *							times this subquery will be executed.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void optimize(DataDictionary dataDictionary, double outerRows)
			throws StandardException
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			SubqueryNode	subqueryNode;
			subqueryNode = (SubqueryNode) elementAt(index);
			subqueryNode.optimize(dataDictionary, outerRows);
		}
	}

	/**
	 * Modify the access paths for all subqueries in this list.
	 *
	 * @see ResultSetNode#modifyAccessPaths
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void modifyAccessPaths()
			throws StandardException
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			SubqueryNode	subqueryNode;
			subqueryNode = (SubqueryNode) elementAt(index);
			subqueryNode.modifyAccessPaths();
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
	public boolean referencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			SubqueryNode	subqueryNode;

			subqueryNode = (SubqueryNode) elementAt(index);
			if (subqueryNode.isMaterializable())
			{
				continue;
			}

			if (subqueryNode.getResultSet().referencesTarget(name, baseTable))
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
	public boolean referencesSessionSchema()
		throws StandardException
	{
		int size = size();
		for (int index = 0; index < size; index++)
		{
			SubqueryNode	subqueryNode;

			subqueryNode = (SubqueryNode) elementAt(index);

			if (subqueryNode.getResultSet().referencesSessionSchema())
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
	 * @return Nothing.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public void setPointOfAttachment(int pointOfAttachment)
		throws StandardException
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
			SubqueryNode	subqueryNode;

			subqueryNode = (SubqueryNode) elementAt(index);
			subqueryNode.setPointOfAttachment(pointOfAttachment);
		}
	}

	/**
	 * Decrement (query block) level (0-based) for 
	 * all of the tables in this subquery list.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 *
	 * @return Nothing;
	 */
	void decrementLevel(int decrement)
	{
		int size = size();

		for (int index = 0; index < size; index++)
		{
			((SubqueryNode) elementAt(index)).getResultSet().decrementLevel(decrement);
		}
	}
}

