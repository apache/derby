/*

   Derby - Class org.apache.derby.impl.sql.compile.AggregateNode

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

import java.util.List;
import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.types.AggregateAliasInfo;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.AliasDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;

/**
 * An Aggregate Node is a node that represents a set function/aggregate.
 * It used for all system aggregates as well as user defined aggregates.
 */

class AggregateNode extends UnaryOperatorNode
{
    static final class BuiltinAggDescriptor
    {
        public  final   String  aggName;
        public  final   String  aggClassName;
        public  final   TypeDescriptor  argType;
        public  final   TypeDescriptor  returnType;

        public BuiltinAggDescriptor
            (
             String aggName,
             String aggClassName,
             TypeDescriptor argType,
             TypeDescriptor returnType
             )
        {
            this.aggName = aggName;
            this.aggClassName = aggClassName;
            this.argType = argType;
            this.returnType = returnType;
        }
    }
    
    //
    // Builtin aggregates which implement org.apache.derby.agg.Aggregator.
    //
    private static  BuiltinAggDescriptor[]  BUILTIN_MODERN_AGGS =
    {
        new BuiltinAggDescriptor
        (
         "VAR_POP",
         "org.apache.derby.impl.sql.execute.VarPAggregator",
         TypeDescriptor.DOUBLE,
         TypeDescriptor.DOUBLE
         ),
        new BuiltinAggDescriptor
        (
         "VAR_SAMP",
         "org.apache.derby.impl.sql.execute.VarSAggregator",
         TypeDescriptor.DOUBLE,
         TypeDescriptor.DOUBLE
         ),
        new BuiltinAggDescriptor
        (
         "STDDEV_POP",
         "org.apache.derby.impl.sql.execute.StdDevPAggregator",
         TypeDescriptor.DOUBLE,
         TypeDescriptor.DOUBLE
         ),
        new BuiltinAggDescriptor
        (
         "STDDEV_SAMP",
         "org.apache.derby.impl.sql.execute.StdDevSAggregator",
         TypeDescriptor.DOUBLE,
         TypeDescriptor.DOUBLE
         ),
    };
    
	private boolean					distinct;

	private AggregateDefinition		uad;
    private TableName           userAggregateName;
	private StringBuffer			aggregatorClassName;
	private String					aggregateDefinitionClassName;
    private Class<?>                aggregateDefinitionClass;
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
     * Constructed when binding a StaticMethodNode that we realize is
     * an aggregate.
     *
     * @param operand the value expression for the aggregate
     * @param uadClass the class of the user aggregate definition
     * @param alias the name by which the aggregate was called
     * @param distinct boolean indicating whether this is distinct
	 *					or not.
     * @param aggregateName the name of the aggregate from the user's
     *                  perspective, e.g. MAX
     * @param cm context manager
     * @throws StandardException
     */
     AggregateNode(
            ValueNode operand,
            UserAggregateDefinition uadClass,
            TableName alias,
            boolean distinct,
            String aggregateName,
            ContextManager cm) throws StandardException {
        this(operand, alias, distinct, aggregateName, cm);
        setUserDefinedAggregate(uadClass);
    }

    /**
     * @param operand the value expression for the aggregate
     * @param uadClass the class name for user aggregate definition
     * for the aggregate
     * @param distinct boolean indicating whether this is distinct
     *                  or not.
     * @param aggregateName the name of the aggregate from the user's
     *                  perspective, e.g. MAX
     * @param cm context manager
     * @throws StandardException
     */
    AggregateNode(
            ValueNode operand,
            TableName uadClass,
            boolean distinct,
            String aggregateName,
            ContextManager cm) throws StandardException {
        super(operand, cm);
        this.aggregateName = aggregateName;
        this.userAggregateName = uadClass;
        this.distinct = distinct;
    }

    /**
     * @param operand the value expression for the aggregate
     * @param uadClass Class for the internal aggregate type
     * @param distinct boolean indicating whether this is distinct
     *                  or not.
     * @param aggregateName the name of the aggregate from the user's
     *                  perspective, e.g. MAX
     * @param cm context manager
     * @throws StandardException
     */
    AggregateNode(
            ValueNode operand,
            Class<?> uadClass,
            boolean distinct,
            String aggregateName,
            ContextManager cm) throws StandardException {
        super(operand, cm);
        this.aggregateName = aggregateName;
        this.aggregateDefinitionClass = uadClass;

        // Distinct is meaningless for min and max
        if (!aggregateDefinitionClass.equals(MaxMinAggregateDefinition.class)) {
            this.distinct = distinct;
        }

        this.aggregateDefinitionClassName = aggregateDefinitionClass.getName();
    }


    /** initialize fields for user defined aggregate */
    private void setUserDefinedAggregate( UserAggregateDefinition userAgg )
    {
        this.uad = userAgg;
        this.aggregateDefinitionClass = uad.getClass();
        this.aggregateDefinitionClassName = aggregateDefinitionClass.getName();
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
	 * @param rcl	The RCL to append to.
	 * @param tableNumber	The tableNumber for the new ColumnReference
	 *
	 * @return ValueNode	The (potentially) modified tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
    ValueNode replaceAggregatesWithColumnReferences(
        ResultColumnList rcl, int tableNumber) throws StandardException
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
            generatedRC =
                new ResultColumn(generatedColName, this, getContextManager());
			generatedRC.markGenerated();
	
			/*
			** Parse time.	
			*/
            generatedRef = new ColumnReference(generatedRC.getName(),
                                               null,
                                               getContextManager());

			// RESOLVE - unknown nesting level, but not correlated, so nesting levels must be 0
            generatedRef.setSource(generatedRC);
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
    ResultColumn getGeneratedRC()
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
    ColumnReference getGeneratedRef()
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
     * @param aggregates        The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ValueNode bindExpression(
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
        DataDictionary  dd = getDataDictionary();
		DataTypeDescriptor 	dts = null;
		ClassFactory		cf;

		cf = getClassFactory();
		classInspector = cf.getClassInspector();

        boolean noSchema = true;
        if ( userAggregateName != null )
        {
            noSchema = (userAggregateName.getSchemaName() == null );
            userAggregateName.bind();
        }

        // If this is a user-defined aggregate that hasn't been bound yet,
        // bind it now.
        if (userAggregateName != null && uad == null)
        {
            String  schemaName = userAggregateName.getSchemaName();
            AliasDescriptor ad = resolveAggregate
                (
                 dd,
                 getSchemaDescriptor( schemaName, true ),
                 userAggregateName.getTableName(),
                 noSchema
                 );

            if ( ad == null )
            {
                throw StandardException.newException
                    (
                     SQLState.LANG_OBJECT_NOT_FOUND,
                     AliasDescriptor.getAliasType( AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR ),
                     userAggregateName.getTableName()
                     );
            }
            
            setUserDefinedAggregate( new UserAggregateDefinition( ad ) );
            aggregateName = ad.getJavaClassName();
         }

		instantiateAggDef();

        // if this is a user-defined aggregate
        if ( isUserDefinedAggregate() )
        {
            AliasDescriptor ad = ((UserAggregateDefinition) uad).getAliasDescriptor();
            boolean         isModernBuiltinAggregate =
                SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME.equals( ad.getSchemaName() );

            if ( distinct && isModernBuiltinAggregate )
            {
                throw StandardException.newException( SQLState.LANG_BAD_DISTINCT_AGG );
            }

            // set up dependency on the user-defined aggregate and compile a check for USAGE
            // priv if needed. no need for a dependency if this is a builtin, system-supplied
            // aggregate
            if ( !isModernBuiltinAggregate )
            {
                getCompilerContext().createDependency( ad );
            }

            if ( isPrivilegeCollectionRequired() )
            {
                //
                // Don't need a privilege check for modern, builtin (system)
                // aggregates. They are tricky. They masquerade as user-defined
                // aggregates because they implement org.apache.derby.agg.Aggregator
                //
                if ( !isModernBuiltinAggregate )
                {
                    getCompilerContext().addRequiredUsagePriv( ad );
                }
            }
        }

        // Add ourselves to the list of aggregates before we do anything else.
        aggregates.add(this);

        CompilerContext cc = getCompilerContext();
        
        // operand being null means a count(*)
		if (operand != null)
		{
            int previousReliability = orReliability( CompilerContext.AGGREGATE_RESTRICTION );
            bindOperand(fromList, subqueryList, aggregates);
            cc.setReliability( previousReliability );
            
			/*
			** Make sure that we don't have an aggregate 
			** IMMEDIATELY below us.  Don't search below
			** any ResultSetNodes.
			*/
			HasNodeVisitor visitor = new HasNodeVisitor(this.getClass(), ResultSetNode.class);
			operand.accept(visitor);
			if (visitor.hasNode())
			{
				throw StandardException.newException
                    (
                     SQLState.LANG_USER_AGGREGATE_CONTAINS_AGGREGATE, 
                     getSQLName()
                    );
			}

			// Also forbid any window function inside an aggregate unless in
			// subquery, cf. SQL 2003, section 10.9, SR 7 a).
			SelectNode.checkNoWindowFunctions(operand, aggregateName);

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
				throw StandardException.newException
                    (SQLState.LANG_USER_AGGREGATE_BAD_TYPE_NULL, getSQLName());
			}
		}

		/*
		** Ask the aggregate definition whether it can handle
	 	** the input datatype.
	 	*/
        aggregatorClassName = new StringBuffer();
        DataTypeDescriptor resultType = uad.getAggregator(dts, aggregatorClassName);

		if (resultType == null)
		{
			throw StandardException.newException
                (
                 SQLState.LANG_USER_AGGREGATE_BAD_TYPE, 
                 getSQLName(), 
                 operand.getTypeId().getSQLTypeName()
                 );
		}

        // For user-defined aggregates, the input operand may need to be
        // coerced to the expected input type of the aggregator.
        if ( isUserDefinedAggregate() )
        {
            ValueNode   castNode = ((UserAggregateDefinition) uad).castInputValue
                ( operand, getContextManager() );

            if ( castNode != null )
            {
                operand = castNode.bindExpression( fromList, subqueryList, aggregates );
            }
        }

		checkAggregatorClassName(aggregatorClassName.toString());

		setType(resultType);

		return this;
	}

	/**
	 * Resolve a user-defined aggregate.
	 */
    static AliasDescriptor resolveAggregate
        ( DataDictionary dd, SchemaDescriptor sd, String rawName, boolean noSchema )
        throws StandardException
    {
        // first see if this is one of the builtin aggregates which
        // implements the Aggregator interface
        AliasDescriptor ad = resolveBuiltinAggregate( dd, rawName, noSchema );
        if ( ad != null ) { return ad; }
        
        // if the schema has a null UUID, that means the schema has not
        // been created yet. in that case, it doesn't have any aggregates in it.
        if ( sd.getUUID() == null ) { return null; }
        
        java.util.List<AliasDescriptor> list = dd.getRoutineList
            ( sd.getUUID().toString(), rawName, AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR );

        if ( list.size() > 0 ) { return list.get( 0 ); }

        return null;
    }

    /**
     * Construct an AliasDescriptor for a modern builtin aggregate.
     */
    private static AliasDescriptor resolveBuiltinAggregate
        ( DataDictionary dd, String rawName, boolean noSchema )
        throws StandardException
    {
        // builtin aggregates may not be schema-qualified
        if ( !noSchema ) { return null; }

        BuiltinAggDescriptor    bad = null;

        for ( BuiltinAggDescriptor aggDescriptor : BUILTIN_MODERN_AGGS )
        {
            if ( aggDescriptor.aggName.equals( rawName ) )
            {
                bad = aggDescriptor;
                break;
            }
        }
        if ( bad == null ) { return null; }

        AliasInfo   aliasInfo = new AggregateAliasInfo( bad.argType, bad.returnType );
        
        return new AliasDescriptor
            (
             dd,
             null,
             rawName,
             dd.getSystemSchemaDescriptor().getUUID(),
             bad.aggClassName,
             AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR,
             AliasInfo.ALIAS_NAME_SPACE_AGGREGATE_AS_CHAR,
             false,
             aliasInfo,
             null
             );
    }
    
	/*
	** Make sure the aggregator class is ok
	*/
	private void checkAggregatorClassName(String className) throws StandardException
	{
		verifyClassExist(className);

		if (!classInspector.assignableTo(className, "org.apache.derby.iapi.sql.execute.ExecAggregator"))
		{
			throw StandardException.newException(SQLState.LANG_BAD_AGGREGATOR_CLASS2, 
													className, 
													getSQLName(),
													operand.getTypeId().getSQLTypeName());
		}
	}

		
	/*
	** Instantiate the aggregate definition.
	*/
	private void instantiateAggDef() throws StandardException
	{
        if ( uad == null )
        {
            Class<?> theClass = aggregateDefinitionClass;

            // get the class
            if (theClass == null)
            {
                String aggClassName = aggregateDefinitionClassName;
                verifyClassExist(aggClassName);

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
                instance = theClass.getConstructor().newInstance();
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
        }
	
		setOperator(aggregateName);
		setMethodName(aggregateDefinitionClassName);
	}

	/**
	 * Indicate whether this aggregate is distinct or not.
	 *
	 * @return 	true/false
	 */
    boolean isDistinct()
	{
		return distinct;
	}

	/**
	 * Get the class that implements that aggregator for this
	 * node.
	 *
	 * @return the class name
	 */
    String  getAggregatorClassName()
	{
		return aggregatorClassName.toString();
	}

	/**
	 * Get the class that implements that aggregator for this
	 * node.
	 *
	 * @return the class name
	 */
    String  getAggregateName()
	{
		return aggregateName;
	}

	/**
	 * Get the result column that has a new aggregator.
	 * This aggregator will be fed into the sorter.
	 *
	 * @param dd	the data dictionary
	 *
	 * @return the result column.  WARNING: it still needs to be bound
	 *
	 * @exception StandardException on error
	 */
    ResultColumn    getNewAggregatorResultColumn(DataDictionary dd)
		throws StandardException
	{
		String	className = aggregatorClassName.toString();

		DataTypeDescriptor compType =
            DataTypeDescriptor.getSQLDataTypeDescriptor(className);

		/*
		** Create a null of the right type.  The proper aggregators
		** are created dynamically by the SortObservers
		*/
		ConstantNode nullNode = getNullNode(compType);

		nullNode.bindExpression(
						null,	// from
						null,	// subquery
						null);	// aggregate

		/*
		** Create a result column with this new node below
		** it.
		*/
        return new ResultColumn(aggregateName, nullNode, getContextManager());
	}


	/**
	 * Get the aggregate expression in a new result
	 * column.
	 *
	 * @param dd the data dictionary
	 *
	 * @return the result column.  WARNING: it still needs to be bound
	 *
	 * @exception StandardException on error
	 */
    ResultColumn    getNewExpressionResultColumn(DataDictionary dd)
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

        return new ResultColumn(
            "##aggregate expression", node, getContextManager());
	}

	/**
	 * Get the null aggregate result expression
	 * column.
	 *
	 * @return the value node
	 *
	 * @exception StandardException on error
	 */
    ValueNode   getNewNullResultExpression()
		throws StandardException
	{
		/*
		** Create a result column with the aggrergate operand
		** it.
		*/
		return getNullNode(getTypeServices());
	}

	/**
	 * Do code generation for this unary operator.  Should
	 * never be called for an aggregate -- it should be converted
	 * into something else by code generation time.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
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
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "aggregateName: " + getSQLName() + "\n" +
				"distinct: " + distinct + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

    boolean isConstant()
	{
		return false;
	}
	
    @Override
    boolean constantExpression(PredicateList where)
	{
		return false;
	}

    /** Get the SQL name of the aggregate */
    public  String  getSQLName()
    {
        if ( isUserDefinedAggregate() )
        {
            return ((UserAggregateDefinition) uad).
                    getAliasDescriptor().getQualifiedName();
        }
        else { return aggregateName; }
    }
    
    /** Return true if this is a user-defined aggregate */
    private boolean isUserDefinedAggregate()
    {
        return uad instanceof UserAggregateDefinition;
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (userAggregateName != null) {
            userAggregateName = (TableName) userAggregateName.accept(v);
        }
    }
}
