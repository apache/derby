/*

   Derby - Class org.apache.derby.impl.sql.compile.AggregateNode

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.iapi.services.loader.ClassFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.execute.ExecAggregator;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.impl.sql.compile.CountAggregateDefinition;
import org.apache.derby.impl.sql.compile.MaxMinAggregateDefinition;
import org.apache.derby.impl.sql.compile.SumAvgAggregateDefinition;

import java.util.Vector;

/**
 * An Aggregate Node is a node that reprsents a set function/aggregate.
 * It used for all system aggregates as well as user defined aggregates.
 *
 * @author jamie
 */

public class AggregateNode extends UnaryOperatorNode
{
	private boolean					distinct;

	private AggregateDefinition		uad;
	private StringBuffer			aggregatorClassName;
	private String					aggregateDefinitionClassName;
	private Class					aggregateDefinitionClass;
	private ClassInspector			classInspector;
	private String					aggregateName;

	/*
	** We wind up pushing all aggregates into a different
	** resultColumnList.  When we do this (in 
	** replaceAggregateWithColumnReference), we return a
	** column reference and create a new result column.
	** This is used to store that result column.
	*/
	private ResultColumn			generatedRC;
	private ColumnReference			generatedRef;

	/**
	 * Intializer.  Used for user defined and internally defined aggregates.
	 * Called when binding a StaticMethodNode that we realize is an aggregate.
	 *
	 * @param operand	the value expression for the aggregate
	 * @param uadClass	the class name for user aggregate definition for the aggregate
	 *					or the Class for the internal aggregate type.
	 * @param distinct	boolean indicating whether this is distinct
	 *					or not.
	 * @param aggregateName	the name of the aggregate from the user's perspective,
	 *					e.g. MAX
	 *
	 * @exception StandardException on error
	 */
	public void init
	(
		Object	operand,
		Object		uadClass,
		Object		distinct,
		Object		aggregateName
	) throws StandardException
	{
		super.init(operand);
		this.aggregateName = (String) aggregateName;

		if (uadClass instanceof String)
		{
			this.aggregateDefinitionClassName = (String) uadClass;
			this.distinct = ((Boolean) distinct).booleanValue();
		}
		else
		{
			this.aggregateDefinitionClass = (Class) uadClass;
			this.aggregateDefinitionClassName =
										aggregateDefinitionClass.getName();

			// Distinct is meaningless for min and max
			if (!aggregateDefinitionClass.equals(MaxMinAggregateDefinition.class))
			{
				this.distinct = ((Boolean) distinct).booleanValue();
			}
		}
	}

	/**
	 * Replace aggregates in the expression tree with a ColumnReference to
	 * that aggregate, append the aggregate to the supplied RCL (assumed to
	 * be from the child ResultSetNode) and return the ColumnReference.
	 * This is useful for pushing aggregates in the Having clause down to
	 * the user's select at parse time.  It is also used for moving around 
	 * Aggregates in the select list when creating the Group By node.  In 
	 * that case it is called <B> after </B> bind time, so we need to create
	 * the column differently.
	 *
	 * @param childRCL	The RCL to append to.
	 * @param tableNumber	The tableNumber for the new ColumnReference
	 *
	 * @return ValueNode	The (potentially) modified tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public ValueNode replaceAggregatesWithColumnReferences(ResultColumnList rcl, int tableNumber)
		throws StandardException
	{

		/*
		** This call is idempotent.  Do
		** the right thing if we have already
		** replaced ourselves.
		*/
		if (generatedRef == null)
		{
			String					generatedColName;
			CompilerContext 		cc = getCompilerContext();
			generatedColName ="SQLCol" + cc.getNextColumnNumber();
			generatedRC = (ResultColumn) getNodeFactory().getNode(
											C_NodeTypes.RESULT_COLUMN,
											generatedColName,
											this,
											getContextManager());
			generatedRC.markGenerated();
	
			/*
			** Parse time.	
			*/
			if (getTypeServices() == null)
			{
				generatedRef = (ColumnReference) getNodeFactory().getNode(
												C_NodeTypes.COLUMN_REFERENCE,
												generatedColName,
												null,
												getContextManager());
			}
			else
			{
				generatedRef = (ColumnReference) getNodeFactory().getNode(
												C_NodeTypes.COLUMN_REFERENCE,
												generatedRC.getName(),
												null,
												getContextManager());
				generatedRef.setType(this.getTypeServices());
			}
			// RESOLVE - unknown nesting level, but not correlated, so nesting levels must be 0
			generatedRef.setNestingLevel(0);
			generatedRef.setSourceLevel(0);
			if (tableNumber != -1)
			{
				generatedRef.setTableNumber(tableNumber);
			}

			rcl.addResultColumn(generatedRC);

			/* 
			** Mark the ColumnReference as being generated to replace
			** an aggregate
			*/
			generatedRef.markGeneratedToReplaceAggregate();
		}
		else
		{
			rcl.addResultColumn(generatedRC);
		}

		return generatedRef;
	}

	/**
	 * Get the AggregateDefinition.
	 *
	 * @return The AggregateDefinition
	 */
	AggregateDefinition getAggregateDefinition()
	{
		return uad;
	}

	/**
	 * Get the generated ResultColumn where this
	 * aggregate now resides after a call to 
	 * replaceAggregatesWithColumnReference().
	 *
	 * @return the result column
	 */
	public ResultColumn getGeneratedRC()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(generatedRC != null, 
				"generatedRC is null.  replaceAggregateWithColumnReference() "+
				"has not been called on this AggergateNode.  Make sure "+
				"the node is under a ResultColumn as expected.");
		}
					
		return generatedRC;
	}

	/**
	 * Get the generated ColumnReference to this
	 * aggregate after the parent called
	 * replaceAggregatesWithColumnReference().
	 *
	 * @return the column reference
	 */
	public ColumnReference getGeneratedRef()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(generatedRef != null, 
				"generatedRef is null.  replaceAggregateWithColumnReference() "+
				"has not been called on this AggergateNode.  Make sure "+
				"the node is under a ResultColumn as expected.");
		}
		return generatedRef;
	}

	/**
	 * Bind this operator.  Determine the type of the subexpression,
	 * and pass that into the UserAggregate.
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode bindExpression(
					FromList			fromList,
					SubqueryList		subqueryList,
					Vector				aggregateVector)
			throws StandardException
	{
		TypeId	outType;
		TypeId	inputType = null;
		Class				inputClass = null;
		String				inputTypeName = null;
		Class				inputInterfaceClass = null;
		String				inputInterfaceName = null;
		DataTypeDescriptor 	dts = null;
		TypeDescriptor 		resultType = null;
		ClassFactory		cf;

		cf = getClassFactory();
		classInspector = cf.getClassInspector();

		instantiateAggDef();

		/* Add ourselves to the aggregateVector before we do anything else */
		aggregateVector.addElement(this);

		super.bindExpression(
				fromList, subqueryList,
				aggregateVector);

		if (operand != null)
		{
			/*
			** Make sure that we don't have an aggregate 
			** IMMEDIATELY below us.  Don't search below
			** any ResultSetNodes.
			*/
			HasNodeVisitor visitor = new HasNodeVisitor(this.getClass(), ResultSetNode.class);
			operand.accept(visitor);
			if (visitor.hasNode())
			{
				throw StandardException.newException(SQLState.LANG_USER_AGGREGATE_CONTAINS_AGGREGATE, 
						aggregateName);
			}

			/*
			** Check the type of the operand.  Make sure that the user
			** defined aggregate can handle the operand datatype.
			*/
			dts = operand.getTypeServices();

			/* Convert count(nonNullableColumn) to count(*)	*/
			if (uad instanceof CountAggregateDefinition &&
				!dts.isNullable())
			{
				setOperator(aggregateName);
				setMethodName(aggregateName);
			}

			/*
			** If we have a distinct, then the value expression
			** MUST implement Orderable because we are going
			** to process it using it as part of a sort.
			*/
			if (distinct)
			{
				/*
				** For now, we check to see if orderable() returns
				** true for this type.  In the future we may need
				** to check to see if the type implements Orderable
				**
				*/
				if (!operand.getTypeId().orderable(cf))
				{
					throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, 
							dts.getTypeId().getSQLTypeName());
				}

			}

			/*
			** Don't allow an untyped null
			*/
			if (operand instanceof UntypedNullConstantNode)
			{
				throw StandardException.newException(SQLState.LANG_USER_AGGREGATE_BAD_TYPE_NULL, aggregateName);
			}
		}

		/*
		** Ask the aggregate definition whether it can handle
	 	** the input datatype.  If an exception is thrown,
		** barf.
	 	*/
		try
		{
			aggregatorClassName = new StringBuffer();
			resultType = uad.getAggregator(dts, aggregatorClassName);
		} catch (Exception e)
		{
			//RESOLVE: would be a good idea to add some additional text to
			// this error, like during getResultDataType on aggregate x
			// maybe enhance this error everywhere (seems like execution
			// should also add some text, at the very least saying during
			// execution.  see Compiltion/Generator/UserExpressionBuilder.java
			throw StandardException.unexpectedUserException(e);
		}

		if (resultType == null)
		{
			throw StandardException.newException(SQLState.LANG_USER_AGGREGATE_BAD_TYPE, 
						aggregateName, 
						operand.getTypeId().getSQLTypeName());
		}

		checkAggregatorClassName(aggregatorClassName.toString());

		/*
		** Try for a built in type matching the
		** type name.  
		*/
		TypeId compTypeId = TypeId.getBuiltInTypeId(resultType.getTypeName());
		/*
		** If not built in, it is probably a java type.
		** Get the sql type descriptor for that.  
		*/
		if (compTypeId == null)
		{
			compTypeId = TypeId.getSQLTypeForJavaType(resultType.getTypeName());
		}

		/*
		** Now set the type.  Get a new descriptor
		** in case the user returned the same descriptor
		** as was passed in.
		*/
		setType(new DataTypeDescriptor(
							compTypeId,
							resultType.getPrecision(),
							resultType.getScale(),
							resultType.isNullable(),
							resultType.getMaximumWidth()
						)
				);

		return this;
	}

	/*
	** Make sure the aggregator class is ok
	*/
	private void checkAggregatorClassName(String className) throws StandardException
	{
		className = verifyClassExist(className, false);

		if (!classInspector.assignableTo(className, "org.apache.derby.iapi.sql.execute.ExecAggregator"))
		{
			throw StandardException.newException(SQLState.LANG_BAD_AGGREGATOR_CLASS2, 
													className, 
													aggregateName,
													operand.getTypeId().getSQLTypeName());
		}
	}

		
	/*
	** Instantiate the aggregate definition.
	*/
	private void instantiateAggDef() throws StandardException
	{
		Class theClass = aggregateDefinitionClass;

		// get the class
		if (theClass == null)
		{
			String aggClassName = aggregateDefinitionClassName;
			aggClassName = verifyClassExist(aggClassName, false);

			try
			{
				theClass = classInspector.getClass(aggClassName);
			}
			catch (Throwable t)
			{
				throw StandardException.unexpectedUserException(t);
			}
		}

		// get an instance
		Object instance = null;
		try
		{
			instance = theClass.newInstance();
		}
		catch (Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}

		if (!(instance instanceof AggregateDefinition))
		{
			throw StandardException.newException(SQLState.LANG_INVALID_USER_AGGREGATE_DEFINITION2, aggregateDefinitionClassName);
		}

		if (instance instanceof MaxMinAggregateDefinition)
		{
			MaxMinAggregateDefinition temp = (MaxMinAggregateDefinition)instance;
			if (aggregateName.equals("MAX"))
				temp.setMaxOrMin(true);
			else
				temp.setMaxOrMin(false);
		}

		if (instance instanceof SumAvgAggregateDefinition)
		{
			SumAvgAggregateDefinition temp1 = (SumAvgAggregateDefinition)instance;
			if (aggregateName.equals("SUM"))
				temp1.setSumOrAvg(true);
			else
				temp1.setSumOrAvg(false);
		}

		this.uad = (AggregateDefinition)instance;
	
		setOperator(aggregateName);
		setMethodName(aggregateDefinitionClassName);

	}

	/**
	 * Indicate whether this aggregate is distinct or not.
	 *
	 * @return 	true/false
	 */
	public boolean isDistinct()
	{
		return distinct;
	}

	/**
	 * Get the class that implements that aggregator for this
	 * node.
	 *
	 * @return the class name
	 */
	public String	getAggregatorClassName()
	{
		return aggregatorClassName.toString();
	}

	/**
	 * Get the class that implements that aggregator for this
	 * node.
	 *
	 * @return the class name
	 */
	public String	getAggregateName()
	{
		return aggregateName;
	}

	/**
	 * Get the result column that has a new aggregator.
	 * This aggregator will be fed into the sorter.
	 *
	 * @param the data dictionary
	 *
	 * @return the result column.  WARNING: it still needs to be bound
	 *
	 * @exception StandardException on error
	 */
	public ResultColumn	getNewAggregatorResultColumn(DataDictionary	dd)
		throws StandardException
	{
		String	className = aggregatorClassName.toString();

		TypeId compTypeId = TypeId.getSQLTypeForJavaType(className);

		/*
		** Create a null of the right type.  The proper aggregators
		** are created dynamically by the SortObservers
		*/
		ConstantNode nullNode = getNullNode(
							compTypeId,
							getContextManager());		// no params

		nullNode.bindExpression(
						null,	// from
						null,	// subquery
						null);	// aggregate

		/*
		** Create a result column with this new node below
		** it.
		*/
		return (ResultColumn) getNodeFactory().getNode(
									C_NodeTypes.RESULT_COLUMN,
									aggregateName,
									nullNode, 
									getContextManager());
	}


	/**
	 * Get the aggregate expression in a new result
	 * column.
	 *
	 * @param the data dictionary
	 *
	 * @return the result column.  WARNING: it still needs to be bound
	 *
	 * @exception StandardException on error
	 */
	public ResultColumn	getNewExpressionResultColumn(DataDictionary	dd)
		throws StandardException
	{
		ValueNode		node;
		/*
		** Create a result column with the aggrergate operand
		** it.  If there is no operand, then we have a COUNT(*),
		** so we'll have to create a new null node and put
		** that in place.
		*/
		node = (operand == null) ?
			this.getNewNullResultExpression() :
			operand;

		return (ResultColumn) getNodeFactory().getNode(
								C_NodeTypes.RESULT_COLUMN,
								"##aggregate expression",
								node,
								getContextManager());
	}

	/**
	 * Get the null aggregate result expression
	 * column.
	 *
	 * @return the value node
	 *
	 * @exception StandardException on error
	 */
	public ValueNode	getNewNullResultExpression()
		throws StandardException
	{
		/*
		** Create a result column with the aggrergate operand
		** it.
		*/
		return getNullNode(this.getTypeId(),
							getContextManager());
	}

	/**
	 * Do code generation for this unary operator.  Should
	 * never be called for an aggregate -- it should be converted
	 * into something else by code generation time.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 * @return	An expression to evaluate this operator
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("generateExpression() should never "+
					"be called on an AggregateNode.  "+
					"replaceAggregatesWithColumnReferences should have " +
					"been called prior to generateExpression");
		}
	}

	/**
	 * Print a string ref of this node.
	 *
	 * @return a string representation of this node 
	 */
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "Aggregate: "+aggregateName+
				"\ndistinct: "+distinct+
				super.toString();
		}
		else
		{
			return "";
		}
	}
}
