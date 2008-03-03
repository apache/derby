/*

   Derby - Class org.apache.derby.impl.sql.compile.WindowFunctionColumnNode

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

package org.apache.derby.impl.sql.compile;

import java.util.Vector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Abstract WindowFunctionColumnNode for window function columns in a 
 * ResultColumnList. 
 * <p>
 * This abstract class contains the common code for window function columns, 
 * and simplifies checks in the code for compilation and execution.
 * <p>
 */
abstract class WindowFunctionColumnNode extends ResultColumn
{
	/* 
	 * WindowNode containing the window definition 
	 * for this window function column
	 */
	private WindowNode windowNode;
	
	
	/**
	 * Initializer for a WindowFunctionColumnNode
	 *
	 * @exception StandardException
	 */
	public void init()
		throws StandardException
	{		
		this.windowNode = null;
		this.isGenerated = true;		
	}	
	
	/**
	 * getWindowNode
	 *
	 * @return the WindowNode for this window function column 
	 */
	public WindowNode getWindowNode()
	{
		return this.windowNode;
	}
	
	/**
	 * setWindowNode
	 *
	 * @param wn The WindowNode with the window definition for this 
	 *			window function column 
	 */	
	public void setWindowNode(WindowNode wn)
	{
		this.windowNode = wn;
	}
	
	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		/* 
		 * Preprocess our WindowNode
		 */
		windowNode = (WindowNode)windowNode.preprocess(numTables, 
			outerFromList,
			outerSubqueryList,
			outerPredicateList);
		return this;
	}
	
	
	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 * In this case, there are no sub-expressions, and the return type
	 * is already known, so this is just a stub.
	 *
	 * @param fromList		The FROM list for the query this
	 *						expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error. Although this class
	 * doesn't throw this exception, it's subclasses do and hence this method
	 * signature here needs to have throws StandardException 
	 */
	public ValueNode bindExpression(
			FromList fromList, 
			SubqueryList subqueryList,
			Vector	aggregateVector)
		throws StandardException
	{
		/*
		 * Call into the windows bind method
		 */		
		windowNode.bind(fromList, subqueryList, aggregateVector);		
		return this;
	}
        
	/**
	 * Do code generation for this window function
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpression(ExpressionClassBuilder acb,
                                       MethodBuilder mb)                                       
		throws StandardException
	{
		/* 
		 * Window function columns are added by the WindowResultSet, so we 
		 * should never call into here.
		 */
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(false, "Code generation for WindowFunctionColumnNode is unimplemented");
	}        

	/**
	 * Window functions do not have a (base)tablename, so we return null.
	 * Overrides method from parent class.
	 */
	public String getTableName()
	{
		return null;
	}
}
