/*

   Derby - Class org.apache.derby.impl.sql.compile.ReferencedTablesVisitor

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.util.JBitSet;

/**
 * Build a JBitSet of all of the referenced tables in the tree.
 *
 * @author jerry
 */
public class ReferencedTablesVisitor implements Visitor 
{
	private JBitSet tableMap;

	public ReferencedTablesVisitor(JBitSet tableMap)
	{
		this.tableMap = tableMap;
	}


	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * Don't do anything unless we have a ColumnReference,
	 * Predicate or ResultSetNode node.
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 *
	 * @exception StandardException on error
	 */
	public Visitable visit(Visitable node)
		throws StandardException
	{
		if (node instanceof ColumnReference)
		{
			((ColumnReference)node).getTablesReferenced(tableMap);
		}
		else if (node instanceof Predicate)
		{
			Predicate pred = (Predicate) node;
			tableMap.or(pred.getReferencedSet());
		}
		else if (node instanceof ResultSetNode)
		{
			ResultSetNode rs = (ResultSetNode) node;
			tableMap.or(rs.getReferencedTableMap());
		}

		return node;
	}

	/**
	 * No need to go below a Predicate or ResultSet.
	 *
	 * @return Whether or not to go below the node.
	 */
	public boolean skipChildren(Visitable node)
	{
		return (node instanceof Predicate ||
			    node instanceof ResultSetNode);
	}

	public boolean stopTraversal()
	{
		return false;
	}
	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////
	JBitSet getTableMap()
	{
		return tableMap;
	}
}	
