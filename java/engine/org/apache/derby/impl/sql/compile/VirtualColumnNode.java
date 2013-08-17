/*

   Derby - Class org.apache.derby.impl.sql.compile.VirtualColumnNode

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * A VirtualColumnNode represents a virtual column reference to a column in
 * a row returned by an underlying ResultSetNode. The underlying column could
 * be in a base table,  view (which could expand into a complex
 * expression), subquery in the FROM clause, temp table, expression result, etc.  
 * By the time we get to code generation, all VirtualColumnNodes should stand only 
 * for references to columns in a base table within a FromBaseTable.
 *
 */

class VirtualColumnNode extends ValueNode
{
	/* A VirtualColumnNode contains a pointer to the immediate child result
	 * that is materializing the virtual column and the ResultColumn
	 * that represents that materialization.
	 */
	private ResultSetNode	sourceResultSet;
	private ResultColumn	sourceColumn;

	/* columnId is redundant since a ResultColumn also has one, but
	 * we need it here for generate()
	 */
	int columnId;

	private boolean correlated = false;


	/**
     * Constructor for a VirtualColumnNode.
	 *
	 * @param sourceResultSet	The ResultSetNode where the value is originating
	 * @param sourceColumn		The ResultColumn where the value is originating
	 * @param columnId			The columnId within the current Row
     * @param cm                The context manager
	 */
    VirtualColumnNode(ResultSetNode sourceResultSet,
                      ResultColumn sourceColumn,
                      int columnId,
                      ContextManager cm) throws StandardException
	{
        super(cm);
        this.sourceResultSet = sourceResultSet;
        this.sourceColumn = sourceColumn;
        this.columnId = columnId;
        setType(sourceColumn.getTypeServices());
	}


	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
    void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			printLabel(depth, "sourceColumn: ");
		    sourceColumn.treePrint(depth + 1);
            printLabel(depth, "sourceResultSet: ");
            sourceResultSet.treePrint(depth + 1);
		}
	}

	/**
	 * Return the ResultSetNode that is the source of this VirtualColumnNode.
	 *
	 * @return ResultSetNode	
	 */
    ResultSetNode getSourceResultSet()
	{
		return sourceResultSet;
	}

	/**
	 * Return the ResultColumn that is the source of this VirtualColumnNode.
	 *
	 * @return ResultSetNode	
	 */
    ResultColumn getSourceColumn()
	{
		return sourceColumn;
	}

	/**
	 * Get the name of the table the ResultColumn is in, if any.  This will be null
	 * if the user did not supply a name (for example, select a from t).
	 * The method will return B for this example, select b.a from t as b
	 * The method will return T for this example, select t.a from t
	 *
	 * @return	A String containing the name of the table the Column
	 *		is in. If the column is not in a table (i.e. is a
	 * 		derived column), it returns NULL.
	 */
    @Override
    String getTableName()
	{
		return sourceColumn.getTableName();
	}

	/**
	 * Get the name of the schema the ResultColumn's table is in, if any.
	 * The return value will be null if the user did not supply a schema name
	 * (for example, select t.a from t).
	 * Another example for null return value (for example, select b.a from t as b).
	 * But for following query select app.t.a from t, this will return APP
	 *
	 * @return	A String containing the name of the schema for the Column's table.
	 *		If the column is not in a schema (i.e. derived column), it returns NULL.
	 */
    @Override
    String getSchemaName() throws StandardException
	{
		return sourceColumn.getSchemaName();
	}

	/**
	 * Return whether or not the ResultColumn is wirtable by a positioned update.
	 *
	 * @return TRUE, if the column is a base column of a table and is 
	 * writable by a positioned update.
	 */
    @Override
	public boolean updatableByCursor()
	{
		return sourceColumn.updatableByCursor();
	}

	/**
	 * Return the ResultColumn that is the source of this VirtualColumnNode.
	 *
	 * @return ResultSetNode	
	 */
    @Override
    ResultColumn getSourceResultColumn()
	{
		return sourceColumn;
	}

	/**
	 * Mark this VCN as a reference to a correlated column.
	 * (It's source resultSet is an outer ResultSet.
	 */
	void setCorrelated()
	{
		correlated = true;
	}

	/**
	 * Return whether or not this VCN is a correlated reference.
	 * 
	 * @return Whether or not this VCN is a correlated reference.
	 */
	boolean getCorrelated()
	{
		return correlated;
	}

	/**
	 * Return whether or not this expression tree is cloneable.
	 *
	 * @return boolean	Whether or not this expression tree is cloneable.
	 */
    @Override
    boolean isCloneable()
	{
		return true;
	}

	/**
	 * ColumnNode's are against the current row in the system.
	 * This lets us generate
	 * a faster get that simply returns the column from the
	 * current row, rather than getting the value out and
	 * returning that, only to have the caller (in the situations
	 * needed) stuffing it back into a new column holder object.
	 * We will assume the general generate() path is for getting
	 * the value out, and use generateColumn() when we want to
	 * keep the column wrapped.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
		int sourceResultSetNumber = sourceColumn.getResultSetNumber();

		/* If the source is marked as redundant, then continue down
		 * the RC/VirtualColumnNode chain.
		 */
		if (sourceColumn.isRedundant())
		{
			sourceColumn.getExpression().generateExpression(acb, mb);
			return;
		}

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(sourceResultSetNumber >= 0,
			"sourceResultSetNumber expected to be >= 0 for virtual column "+sourceColumn.getName());

		/* The ColumnReference is from an immediately underlying ResultSet.
		 * The Row for that ResultSet is Activation.row[sourceResultSetNumber], 
		 * where sourceResultSetNumber is the resultSetNumber for that ResultSet.
		 *
		 * The generated java is the expression:
		 *	(<Datatype interface>) this.row[sourceResultSetNumber].
		 *												getColumn(#columnId);
		 * where <Datatype interface> is the appropriate interface for the
		 * column from the Datatype protocol.
		 */
		acb.pushColumnReference(mb, 
									sourceResultSetNumber,
	    							sourceColumn.getVirtualColumnId());

		mb.cast(sourceColumn.getTypeCompiler().interfaceName());
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 * @exception StandardException	thrown on error
	 */
    @Override
	protected int getOrderableVariantType() throws StandardException
	{
		/*
		** Delegate to the source column
		*/
		return sourceColumn.getOrderableVariantType();
	}

	/**
	 * Get the DataTypeServices from this Node.
	 *
	 * @return	The DataTypeServices from this Node.  This
	 *		may be null if the node isn't bound yet.
	 */
    @Override
    final public DataTypeDescriptor getTypeServices()
	{
        return sourceColumn.getTypeServices();
    }
    
    @Override
    final public void setType(DataTypeDescriptor dtd) throws StandardException
    {
        sourceColumn.setType(dtd);
    }
    
    boolean isEquivalent(ValueNode o) throws StandardException
    {
        if (isSameNodeKind(o)) {
    		VirtualColumnNode other = (VirtualColumnNode)o;
    		return sourceColumn.isEquivalent(other.sourceColumn);
    	}

    	return false;
    }
}
