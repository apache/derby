/*

   Derby - Class org.apache.derby.impl.sql.compile.DefaultNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.depend.ProviderList;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.catalog.types.DefaultInfoImpl;

import java.util.Vector;

/**
 * DefaultNode represents a column/parameter default.
 */
public  class DefaultNode extends ValueNode
{
	private ProviderList apl;
	private String		columnName;
	private String		defaultText;
	private ValueNode	defaultTree;

	/**
	 * Initializer for a column/parameter default.
	 *
	 * @param defaultTree			Query tree for default
	 * @param defaultText	The text of the default.
	 */
	public void init(
					Object defaultTree,
					Object defaultText)
	{
		this.defaultTree = (ValueNode) defaultTree;
		this.defaultText = (String) defaultText;
	}

	/**
	 * Initializer for insert/update
	 *
	 */
	public void init(Object columnName)
	{
		this.columnName = (String) columnName;
	}

	/**
	  * Get the text of the default.
	  */
	public	String	getDefaultText()
	{
		return	defaultText;
	}

	/**
	 * Get the query tree for the default.
	 *
	 * @return The query tree for the default.
	 */
	ValueNode getDefaultTree()
	{
		return defaultTree;
	}

	void setAuxiliaryProviderList(ProviderList apl)
	{
		this.apl = apl;
	}

	public ProviderList getAuxiliaryProviderList()
	{
		return apl;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "defaultTree: " + defaultTree + "\n" +
				   "defaultText: " + defaultText + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 * In this case, there are no sub-expressions, and the return type
	 * is already known, so this is just a stub.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
			Vector	aggregateVector)
		throws StandardException
	{
		ColumnDescriptor	cd;
		TableDescriptor		td;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList.size() != 0,
				"fromList expected to be non-empty");
			if (! (fromList.elementAt(0) instanceof FromBaseTable))
			{
				SanityManager.THROWASSERT(
					"fromList.elementAt(0) expected to be instanceof FromBaseTable, not " +
					fromList.elementAt(0).getClass().getName());
			}

		}
		// Get the TableDescriptor for the target table
		td = ((FromBaseTable) fromList.elementAt(0)).getTableDescriptor();

		// Get the ColumnDescriptor for the column
		cd = td.getColumnDescriptor(columnName);
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(cd != null,
				"cd expected to be non-null");
		}

		/* If we have the default text, then parse and bind it and
		 * return the tree.
		 */
		DefaultInfoImpl defaultInfo = (DefaultInfoImpl) cd.getDefaultInfo();
		if (defaultInfo != null)
		{
			String		defaultText = defaultInfo.getDefaultText();
			ValueNode	defaultTree = parseDefault(defaultText, getLanguageConnectionContext(),
												   getCompilerContext());

			/* Query is dependent on the DefaultDescriptor */
			DefaultDescriptor defaultDescriptor = cd.getDefaultDescriptor(
													getDataDictionary());
			getCompilerContext().createDependency(defaultDescriptor);

			return defaultTree.bindExpression(
									fromList, 
									subqueryList,
									aggregateVector);
		}
		else
		{
			// Default is null
			ValueNode nullNode = (ValueNode) getNodeFactory().getNode(
										C_NodeTypes.UNTYPED_NULL_CONSTANT_NODE,
										getContextManager());
			return nullNode;
		}
	}

	/**
	  *	Parse a default and turn it into a query tree.
	  *
	  *	@param	defaultText			Text of Default.
	  * @param	lcc					LanguageConnectionContext
	  * @param	cc					CompilerContext
	  *
	  * @return	The parsed default as a query tree.
	  *
	  * @exception StandardException		Thrown on failure
	  */
	public	static ValueNode	parseDefault
	(
		String						defaultText,
		LanguageConnectionContext	lcc,
		CompilerContext				cc
    )
		throws StandardException
	{
		Parser						p;
		ValueNode					defaultTree;

		/* Get a Statement to pass to the parser */

		/* We're all set up to parse. We have to build a compilable SQL statement
		 * before we can parse -  So, we goober up a VALUES defaultText.
		 */
		String values = "VALUES " + defaultText;
		
		/*
		** Get a new compiler context, so the parsing of the select statement
		** doesn't mess up anything in the current context (it could clobber
		** the ParameterValueSet, for example).
		*/
		CompilerContext newCC = lcc.pushCompilerContext();

		p = newCC.getParser();

		
		/* Finally, we can call the parser */
		// Since this is always nested inside another SQL statement, so topLevel flag
		// should be false
		QueryTreeNode qt = p.parseStatement(values);
		if (SanityManager.DEBUG)
		{
			if (! (qt instanceof CursorNode))
			{
				SanityManager.THROWASSERT(
					"qt expected to be instanceof CursorNode, not " +
					qt.getClass().getName());
			}
			CursorNode cn = (CursorNode) qt;
			if (! (cn.getResultSetNode() instanceof RowResultSetNode))
			{
				SanityManager.THROWASSERT(
					"cn.getResultSetNode() expected to be instanceof RowResultSetNode, not " +
					cn.getResultSetNode().getClass().getName());
			}
		}

		defaultTree = ((ResultColumn) 
							((CursorNode) qt).getResultSetNode().getResultColumns().elementAt(0)).
									getExpression();

		lcc.popCompilerContext(newCC);

		return	defaultTree;
	}

	/**
	 * @exception StandardException		Thrown on failure
	 */
	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb) 
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT(
				"generateExpression not expected to be called");
		}
	}
}
