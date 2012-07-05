/*

   Derby - Class org.apache.derby.impl.sql.compile.FromVTI

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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Vector;
import org.apache.derby.catalog.TypeDescriptor;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.RoutineAliasInfo;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.loader.ClassInspector;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.vti.DeferModification;
import org.apache.derby.vti.RestrictedVTI;
import org.apache.derby.vti.Restriction;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

/**
 * A FromVTI represents a VTI in the FROM list of a DML statement.
 *
 */
public class FromVTI extends FromTable implements VTIEnvironment
{

	JBitSet				correlationMap;
	JBitSet				dependencyMap;
	MethodCallNode	methodCall;
	TableName			exposedName;
	SubqueryList subqueryList;
	boolean				implementsVTICosting;
	boolean				optimized;
	boolean				materializable;
	boolean				isTarget;
	boolean				isDerbyStyleTableFunction;
	boolean				isRestrictedTableFunction;
	ResultSet			rs;

	private	FormatableHashtable	compileTimeConstants;

	// Number of columns returned by the VTI
	protected int numVTICols;

	private PredicateList restrictionList;

	/*
	** We don't know how expensive a virtual table will be.
	** Let's say it has 10000 rows with a cost of 100000.
	*/
	double estimatedCost = VTICosting.defaultEstimatedCost;
	double estimatedRowCount = VTICosting.defaultEstimatedRowCount;
	boolean supportsMultipleInstantiations = true;
	boolean vtiCosted;

	/* Version 1 or 2 VTI*/
	protected boolean			version2;
	private boolean				implementsPushable;
	private PreparedStatement	ps;

    private JavaValueNode[] methodParms;
    
    private boolean controlsDeferral;
    private int resultSetType = ResultSet.TYPE_FORWARD_ONLY;

    private String[] projectedColumnNames; // for RestrictedVTIs
    private Restriction vtiRestriction; // for RestrictedVTIs

    /**
	 * @param invocation		The constructor or static method for the VTI
	 * @param correlationName	The correlation name
	 * @param derivedRCL		The derived column list
	 * @param tableProperties	Properties list associated with the table
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
					Object invocation,
					Object correlationName,
					Object derivedRCL,
					Object tableProperties)
		throws StandardException
	{
        init( invocation,
              correlationName,
              derivedRCL,
              tableProperties,
              makeTableName(null, (String) correlationName));
	}

    /**
	 * @param invocation		The constructor or static method for the VTI
	 * @param correlationName	The correlation name
	 * @param derivedRCL		The derived column list
	 * @param tableProperties	Properties list associated with the table
     * @param exposedTableName  The table name (TableName class)
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
					Object invocation,
					Object correlationName,
					Object derivedRCL,
					Object tableProperties,
                    Object exposedTableName)
		throws StandardException
	{
		super.init(correlationName, tableProperties);

		this.methodCall = (MethodCallNode) invocation;

		resultColumns = (ResultColumnList) derivedRCL;
		subqueryList = (SubqueryList) getNodeFactory().getNode(
											C_NodeTypes.SUBQUERY_LIST,
											getContextManager());

		/* Cache exposed name for this table.
		 * The exposed name becomes the qualifier for each column
		 * in the expanded list.
		 */
		this.exposedName = (TableName) exposedTableName;
	}

	// Optimizable interface

	/**
	 * @see Optimizable#estimateCost
	 *
	 * @exception StandardException		Thrown on error
	 */
	public CostEstimate estimateCost(
				OptimizablePredicateList predList,
				ConglomerateDescriptor cd,
				CostEstimate outerCost,
				Optimizer optimizer,
				RowOrdering rowOrdering)
			throws StandardException
	{
		costEstimate = getCostEstimate(optimizer);

		/* Cost the VTI if it implements VTICosting.
		 * Otherwise we use the defaults.
		 * NOTE: We only cost the VTI once.
		 */
		if (implementsVTICosting && ! vtiCosted)
		{
			try
			{
				VTICosting vtic = getVTICosting();
				estimatedCost = vtic.getEstimatedCostPerInstantiation(this);
				estimatedRowCount = vtic.getEstimatedRowCount(this);
				supportsMultipleInstantiations = vtic.supportsMultipleInstantiations(this);

				if (ps != null) {
					ps.close();
					ps = null;
				}
				if (rs != null) {
					rs.close();
					rs = null;
				}
			}
			catch (SQLException sqle)
			{
				throw StandardException.unexpectedUserException(sqle);
			}
			vtiCosted = true;
		}

		costEstimate.setCost(estimatedCost, estimatedRowCount, estimatedRowCount);

		/*
		** Let the join strategy decide whether the cost of the base
		** scan is a single scan, or a scan per outer row.
		** NOTE: The multiplication should only be done against the
		** total row count, not the singleScanRowCount.
		** RESOLVE - If the join strategy does not do materialization,
		** the VTI does not support multiple instantiations and the
		** outer row count is not exactly 1 row, then we need to change
		** the costing formula to take into account the cost of creating
		** the temp conglomerate, writing to it and reading from it
		** outerCost.rowCount() - 1 times.
		*/
		if (getCurrentAccessPath().
				getJoinStrategy().
					multiplyBaseCostByOuterRows())
		{
			costEstimate.multiply(outerCost.rowCount(), costEstimate);
		}

		if ( ! optimized)
		{
			subqueryList.optimize(optimizer.getDataDictionary(),
									costEstimate.rowCount());
			subqueryList.modifyAccessPaths();
		}

		optimized = true;

		return costEstimate;
	}

	/**
	 * @see Optimizable#legalJoinOrder
	 */
	public boolean legalJoinOrder(JBitSet assignedTableMap)
	{
		/* In order to tell if this is a legal join order, we
		 * need to see if the assignedTableMap, ORed with the
		 * outer tables that we are correlated with, contains
		 * our dependency map.
		 */
		JBitSet tempBitSet = (JBitSet) assignedTableMap;
		tempBitSet.or(correlationMap);

		/* Have all of our dependencies been satisified? */
		return tempBitSet.contains(dependencyMap);
	}


	/** @see Optimizable#isMaterializable 
	 *
	 */
	public boolean isMaterializable()
	{
		return materializable;
	}

	/** @see Optimizable#supportsMultipleInstantiations */
	public boolean supportsMultipleInstantiations()
	{
		return supportsMultipleInstantiations;
	}

    /** Return true if this is a user-defined table function */
    public boolean isDerbyStyleTableFunction()
    {
        return isDerbyStyleTableFunction;
    }

	/**
	 * @see ResultSetNode#adjustForSortElimination()
	 */
	public void adjustForSortElimination()
	{
		/* It's possible that we have an ORDER BY on the columns for this
		 * VTI but that the sort was eliminated during preprocessing (see
		 * esp. SelectNode.preprocess()).  Take as an example the following
		 * query:
		 *
		 *   select distinct * from
		 *      table(syscs_diag.space_table('T1')) X order by 3
		 *
		 * For this query we will merge the ORDER BY and the DISTINCT and
		 * thereby eliminate the sort for the ORDER BY.  As a result we
		 * will end up here--but we don't need to do anything special to
		 * return VTI rows in the correct order, so this method is a no-op.
		 * DERBY-2805.
		 */
	}

	/**
	 * @see Optimizable#modifyAccessPath
	 *
	 * @exception StandardException		Thrown on error
	 */
	public Optimizable modifyAccessPath(JBitSet outerTables) throws StandardException
	{
		/* Close the rs if one was instantiated */
		if (rs != null)
		{
			try
			{
				rs.close();
				rs = null;
			}
			catch(Throwable t)
			{
				throw StandardException.unexpectedUserException(t);
			}
		}

		return super.modifyAccessPath(outerTables);
	}

	public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate)
		throws StandardException
	{
		if (!implementsPushable)
			return false;

        // Do not push down join predicates: those referencing more than one table.
        if( ! optimizablePredicate.getReferencedMap().hasSingleBitSet())
            return false;
        
		if (restrictionList == null) {
			restrictionList = (PredicateList) getNodeFactory().getNode(
											C_NodeTypes.PREDICATE_LIST,
											getContextManager());
		}

		restrictionList.addPredicate((Predicate) optimizablePredicate);
		return true;
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
			return "materializable: " + materializable + "\n" +
			  super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG) {
			super.printSubNodes(depth);

			if (methodCall != null)
			{
				printLabel(depth, "methodCall: ");
				methodCall.treePrint(depth + 1);
			}

			if (exposedName != null)
			{
				printLabel(depth, "exposedName: ");
				exposedName.treePrint(depth + 1);
			}

			if (subqueryList != null)
			{
				printLabel(depth, "subqueryList: ");
				subqueryList.treePrint(depth + 1);
			}
		}
	}

	/** 
	 * Return true if this VTI is a constructor. Otherwise, it is a static method.
	 */
	public boolean  isConstructor()
	{
		return ( methodCall instanceof NewInvocationNode );
	}

	/** 
	 * Return the constructor or static method invoked from this node
	 */
	public MethodCallNode getMethodCall()
	{
		return methodCall;
	}

	/**
	 * Get the exposed name for this table, which is the name that can
	 * be used to refer to it in the rest of the query.
	 *
	 * @return	The exposed name for this table.
	 */

	public String getExposedName()
	{
		return correlationName;
	}

    /**
     * @return the table name used for matching with column references.
     *
     */
    public TableName getExposedTableName()
    {
        return exposedName;
    }

	/**
	 * Mark this VTI as the target of a delete or update.
	 */
	void setTarget()
	{
		isTarget = true;
		version2 = true;
	}


	/**
	 * Bind the non VTI tables in this ResultSetNode.  This includes getting their
	 * descriptors from the data dictionary and numbering them.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindNonVTITables(DataDictionary dataDictionary, 
							FromList fromListParam) 
					throws StandardException
	{

		/* Assign the tableNumber.  (All other work done in bindVTITables() */
		if (tableNumber == -1)  // allow re-bind, in which case use old number
			tableNumber = getCompilerContext().getNextTableNumber();
		return this;
	}

    /**
     * @return The name of the VTI, mainly for debugging and error messages.
     */
    String getVTIName()
    {
        return methodCall.getJavaClassName();
    } // end of getVTIName

	/**
	 * Bind this VTI that appears in the FROM list.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode		The bound FromVTI.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode bindVTITables(FromList fromListParam) 
							throws StandardException
	{
		ResultColumnList	derivedRCL = resultColumns;

		LanguageConnectionContext lcc = getLanguageConnectionContext();

		/* NOTE - setting of table number moved to FromList.bindTables()
		 * in order to avoid an ordering problem with join columns in
		 * parameters.
		 */

		/* Bind the constructor or static method - does basic error checking.
		 * Correlated subqueries are not allowed as parameters to
		 * a VTI, so pass an empty FromList.
		 */
		Vector aggregateVector = new Vector();
		methodCall.bindExpression(fromListParam,
									 subqueryList,
									 aggregateVector);

		// Is the parameter list to the constructor valid for a VTI?
		methodParms = methodCall.getMethodParms();

        RoutineAliasInfo    routineInfo = methodCall.getRoutineInfo();

        if (
            (routineInfo !=null) &&
            routineInfo.getReturnType().isRowMultiSet() &&
            (routineInfo.getParameterStyle() == RoutineAliasInfo.PS_DERBY_JDBC_RESULT_SET)
            )			{
            isDerbyStyleTableFunction = true;
        }

        if ( isDerbyStyleTableFunction )
        {
            Method boundMethod = (Method) methodCall.getResolvedMethod();

            isRestrictedTableFunction = RestrictedVTI.class.isAssignableFrom( boundMethod.getReturnType() );
        }

		/* If we have a valid constructor, does class implement the correct interface? 
		 * If version2 is true, then it must implement PreparedStatement, otherwise
		 * it can implement either PreparedStatement or ResultSet.  (We check for
		 * PreparedStatement first.)
		 */

		if ( isConstructor() )
		{
		    NewInvocationNode   constructor = (NewInvocationNode) methodCall;
                
		    if (!constructor.assignableTo("java.sql.PreparedStatement"))
		    {
			if (version2)
			{
				throw StandardException.newException(SQLState.LANG_DOES_NOT_IMPLEMENT, 
										getVTIName(),
										"java.sql.PreparedStatement");
			}
			else if (! constructor.assignableTo("java.sql.ResultSet"))
			{
				throw StandardException.newException(SQLState.LANG_DOES_NOT_IMPLEMENT, 
										getVTIName(),
										"java.sql.ResultSet");
			}
		    }
		    else
		    {
			    version2 = true;
		    }
        
		    /* If this is a version 2 VTI */
		    if (version2)
		    {
			// Does it support predicates
			implementsPushable = constructor.assignableTo("org.apache.derby.vti.IQualifyable");
		    }
		    // Remember whether or not the VTI implements the VTICosting interface
		    implementsVTICosting = constructor.assignableTo(ClassName.VTICosting);
		}

        if ( isDerbyStyleTableFunction )
        {
            implementsVTICosting = implementsDerbyStyleVTICosting( methodCall.getJavaClassName() );
        }
            

		/* Build the RCL for this VTI.  We instantiate an object in order
		 * to get the ResultSetMetaData.
		 * 
		 * If we have a special trigger vti, then we branch off and get
		 * its rcl from the trigger table that is waiting for us in
		 * the compiler context.
		 */
		UUID triggerTableId;
		if ((isConstructor()) && ((triggerTableId = getSpecialTriggerVTITableName(lcc, methodCall.getJavaClassName())) != null)  )
		{
			TableDescriptor td = getDataDictionary().getTableDescriptor(triggerTableId);
			resultColumns = genResultColList(td);

			// costing info
			vtiCosted = true;
			estimatedCost = 50d;
			estimatedRowCount = 5d;
			supportsMultipleInstantiations = true;
		}
		else
		{	
			resultColumns = (ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());

			// if this is a Derby-style Table Function, then build the result
			// column list from the RowMultiSetImpl return datatype

			if ( isDerbyStyleTableFunction ) {
			    createResultColumnsForTableFunction( routineInfo.getReturnType() );
			}
			else
			{
            
			    ResultSetMetaData rsmd = getResultSetMetaData();
	
			    /* Wouldn't it be nice if we knew that the class/object would never
			     * return a null ResultSetMetaData.
			     */
			    if (rsmd == null)
			    {
				throw StandardException.newException(SQLState.LANG_NULL_RESULT_SET_META_DATA, 
											getVTIName());
			    }
	
			    // Remember how many columns VTI returns for partial row calculation
			    try
			    {
				numVTICols = rsmd.getColumnCount();
			    }
			    catch (SQLException sqle)
			    {
				numVTICols = 0;
			    }

			    resultColumns.createListFromResultSetMetaData(rsmd, exposedName, 
														  getVTIName() );
			}
		}
		numVTICols = resultColumns.size();
	
		/* Propagate the name info from the derived column list */
		if (derivedRCL != null)
		{
			 resultColumns.propagateDCLInfo(derivedRCL, correlationName);
		}

		return this;
	}

	/**
	 * Get the ResultSetMetaData for the class/object.  We first look for 
	 * the optional static method which has the same signature as the constructor.
	 * If it doesn't exist, then we instantiate an object and get the ResultSetMetaData
	 * from that object.
	 *
	 * @return The ResultSetMetaData from the class/object.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetMetaData getResultSetMetaData() 
		throws StandardException
	{
		// Get the actual 
		ResultSetMetaData rsmd = null;

		try
		{	
			if (version2)
			{
				ps = (PreparedStatement) getNewInstance();

				if (ps.getResultSetConcurrency() != ResultSet.CONCUR_UPDATABLE)
				{
					throw StandardException.newException(SQLState.LANG_UPDATABLE_VTI_NON_UPDATABLE_RS, 
														 getVTIName());
				}

				rsmd = ps.getMetaData();

                controlsDeferral = (ps instanceof DeferModification);

                /* See if the result set is known to be insensitive or not.
                 *
                 * Some older VTI implementations do not implement getResultSetType(). UpdatableVTITemplate
                 * does not implement it at all. UpdatableVTITemplate.getResultSetType throws an
                 * exception. In either of these cases make the conservative assumption that the result set is sensitive.
                 */
                try
                {
                    resultSetType = ps.getResultSetType();
                }
                catch( SQLException sqle){}
                catch( java.lang.AbstractMethodError ame){}
                catch( java.lang.NoSuchMethodError nsme){}

				if (!implementsVTICosting) {
					ps.close();
					ps = null;
				}

			}
			else
			{
				rs = (ResultSet) getNewInstance();

				rsmd = rs.getMetaData();

				if (!implementsVTICosting) {
					rs.close();
					rs = null;
				}
			}
		}
		catch(Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}

		return rsmd;
	}

    private Object getNewInstance()
        throws StandardException
    {
		NewInvocationNode   constructor = (NewInvocationNode) methodCall;
		Class[]  paramTypeClasses = constructor.getMethodParameterClasses();
		Object[] paramObjects = null;

		if (paramTypeClasses != null)
		{
			paramObjects = new Object[paramTypeClasses.length];

			for (int index = 0; index < paramTypeClasses.length; index++)
			{
				Class paramClass = paramTypeClasses[index];

				paramObjects[index] = methodParms[index].getConstantValueAsObject();

				// As-per the JDBC spec SMALLINT and TINYINT map to java.lang.Integer
				// as objects. This means if getConstantValueAsObject() has returned an
				// Integer obejct in these cases, whereas Java method calling requires
				// Short or Byte object.
				if ((paramObjects[index] != null) && paramClass.isPrimitive()) {

					if (paramClass.equals(Short.TYPE)) {
						paramObjects[index] =
							new Short(((Integer) paramObjects[index]).shortValue());
					} else if (paramClass.equals(Byte.TYPE)) {
						paramObjects[index] =
							new Byte(((Integer) paramObjects[index]).byteValue());
					}
				}

				// Pass defaults for unknown primitive values
				if (paramObjects[index] == null && 
					paramClass.isPrimitive())
				{
					if (paramClass.equals(Integer.TYPE))
					{
						paramObjects[index] = new Integer(0);
					}
					else if (paramClass.equals(Short.TYPE))
					{
						paramObjects[index] = new Short((short) 0);
					}
					else if (paramClass.equals(Byte.TYPE))
					{
						paramObjects[index] = new Byte((byte) 0);
					}
					else if (paramClass.equals(Long.TYPE))
					{
						paramObjects[index] = new Long((long) 0);
					}
					else if (paramClass.equals(Float.TYPE))
					{
						paramObjects[index] = new Float((float) 0);
					}
					else if (paramClass.equals(Double.TYPE))
					{
						paramObjects[index] = new Double((double) 0);
					}
					else if (paramClass.equals(Boolean.TYPE))
					{
						paramObjects[index] = Boolean.FALSE;
					}
					else if (paramClass.equals(Character.TYPE))
					{
						paramObjects[index] = new Character(Character.MIN_VALUE);
					}
				}
			}
		}
		else
		{
			paramTypeClasses = new Class[0];
			paramObjects = new Object[0];
		}

        try
        {
            ClassInspector classInspector = getClassFactory().getClassInspector();
            String javaClassName = methodCall.getJavaClassName();
            Constructor constr = classInspector.getClass(javaClassName).getConstructor(paramTypeClasses);

            return constr.newInstance(paramObjects);
        }
		catch(Throwable t)
		{
            if( t instanceof InvocationTargetException)
            {
                InvocationTargetException ite = (InvocationTargetException) t;
                Throwable wrappedThrowable = ite.getTargetException();
                if( wrappedThrowable instanceof StandardException)
                    throw (StandardException) wrappedThrowable;
            }
			throw StandardException.unexpectedUserException(t);
		}
    } // end of getNewInstance

    /**
     * Get the DeferModification interface associated with this VTI
     *
     * @return null if the VTI uses the default modification deferral
     */
    public DeferModification getDeferralControl( )
        throws StandardException
    {
        if( ! controlsDeferral)
            return null;
        try
        {
            return (DeferModification) getNewInstance();
        }
		catch(Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}
    } // end of getDeferralControl

    /**
     * @return the ResultSet type of the VTI, TYPE_FORWARD_ONLY if the getResultSetType() method
     *         of the VTI class throws an exception.
     */
    public int getResultSetType()
    {
        return resultSetType;
    }

	/**
	 * Bind the expressions in this VTI.  This means 
	 * binding the sub-expressions, as well as figuring out what the return 
	 * type is for each expression.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindExpressions(FromList fromListParam)
					throws StandardException
	{
		/* Figure out if the VTIs parameters are QUERY_INVARIANT.  If so,
		 * then the VTI is a candidate for materialization at execution time
		 * if it is the inner table of a join or in a subquery.
		 */
		materializable = methodCall.areParametersQueryInvariant();

		/* NOTE: We need to rebind any ColumnReferences that are parameters and are
		 * from other VTIs that appear after this one in the FROM list.
		 * These CRs will have uninitialized column and table numbers.
		 */
		Vector colRefs = getNodesFromParameters(ColumnReference.class);
		Vector aggregateVector = null;
		for (Enumeration e = colRefs.elements(); e.hasMoreElements(); )
		{
			ColumnReference ref = (ColumnReference)e.nextElement();

            //
            // Table Function parameters may not reference columns from other tables in the
            // FROM list of the current query block. See DERBY-5579. We also do not allow
            // VTI parameters to refer to other VTIs.
            //
            int referencedTableNumber = ref.getTableNumber();
                
            for ( int i = 0; i < fromListParam.size(); i++ )
            {
                FromTable   fromTable = (FromTable) fromListParam.elementAt( i );

                if ( referencedTableNumber == fromTable.getTableNumber() )
                {
                    if ( isDerbyStyleTableFunction || (fromTable instanceof FromVTI) )
                    {
                        throw StandardException.newException
                            ( SQLState.LANG_BAD_TABLE_FUNCTION_PARAM_REF, ref.getSQLColumnName() );
                    }
                }
            }

			// Rebind the CR if the tableNumber is uninitialized
			if (ref.getTableNumber() == -1)
			{
				// we need a fake agg list
				if (aggregateVector == null)
				{
					aggregateVector = new Vector();
				}
				ref.bindExpression(fromListParam,
									subqueryList,
									aggregateVector);
			}
		}
	}

	/**
	 * Get all of the nodes of the specified class
	 * from the parameters to this VTI.
	 *
	 * @param nodeClass	The Class of interest.
	 *
	 * @return A vector containing all of the nodes of interest.
	 *
	 * @exception StandardException		Thrown on error
	 */
	Vector getNodesFromParameters(Class nodeClass)
		throws StandardException
	{
		CollectNodesVisitor getCRs = new CollectNodesVisitor(nodeClass);
		methodCall.accept(getCRs);
		return getCRs.getList();
	}

	/**
	 * Expand a "*" into a ResultColumnList with all of the
	 * result columns from the subquery.
	 * @exception StandardException		Thrown on error
	 */
	public ResultColumnList getAllResultColumns(TableName allTableName)
			throws StandardException
	{
		ResultColumnList rcList = null;
		ResultColumn	 resultColumn;
		ValueNode		 valueNode;
		String			 columnName;
        TableName        toCompare;

		if(allTableName != null)
             toCompare = makeTableName(allTableName.getSchemaName(),correlationName);
        else
            toCompare = makeTableName(null,correlationName);

        if ( allTableName != null &&
             ! allTableName.equals(toCompare))
        {
            return null;
        }

		rcList = (ResultColumnList) getNodeFactory().getNode(
										C_NodeTypes.RESULT_COLUMN_LIST,
										getContextManager());

		/* Build a new result column list based off of resultColumns.
		 * NOTE: This method will capture any column renaming due to 
		 * a derived column list.
		 */
		int rclSize = resultColumns.size();
		for (int index = 0; index < rclSize; index++)
		{
			resultColumn = (ResultColumn) resultColumns.elementAt(index);

			if (resultColumn.isGenerated())
			{
				continue;
			}

			// Build a ResultColumn/ColumnReference pair for the column //
			columnName = resultColumn.getName();
			valueNode = (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.COLUMN_REFERENCE,
											columnName,
											exposedName,
											getContextManager());
			resultColumn = (ResultColumn) getNodeFactory().getNode(
											C_NodeTypes.RESULT_COLUMN,
											columnName,
											valueNode,
											getContextManager());

			// Build the ResultColumnList to return //
			rcList.addResultColumn(resultColumn);
		}
		return rcList;
	}

	/**
	 * Try to find a ResultColumn in the table represented by this FromBaseTable
	 * that matches the name in the given ColumnReference.
	 *
	 * @param columnReference	The columnReference whose name we're looking
	 *				for in the given table.
	 *
	 * @return	A ResultColumn whose expression is the ColumnNode
	 *			that matches the ColumnReference.
	 *		Returns null if there is no match.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultColumn getMatchingColumn(ColumnReference columnReference) throws StandardException
	{
		/* We could get called before our RCL is built.  That's okay, we'll
		 * just say that we don't match. 
		 */
		if (resultColumns == null)
		{
			return null;
		}

		ResultColumn	resultColumn = null;
		TableName		columnsTableName;

		columnsTableName = columnReference.getTableNameNode();

		/*
		** If the column did not specify a name, or the specified name
		** matches the table we're looking at, see whether the column
		** is in this table.
		*/
		if (columnsTableName == null || columnsTableName.equals(exposedName))
		{
			resultColumn = resultColumns.getResultColumn(columnReference.getColumnName());
			/* Did we find a match? */
			if (resultColumn != null)
			{
				columnReference.setTableNumber(tableNumber);
                columnReference.setColumnNumber(
                    resultColumn.getColumnPosition());
			}
		}

		return resultColumn;
	}

	/**
	 * Preprocess a ResultSetNode - this currently means:
	 *	o  Generating a referenced table map for each ResultSetNode.
	 *  o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
	 *  o  Converting the WHERE and HAVING clauses into PredicateLists and
	 *	   classifying them.
	 *  o  Ensuring that a ProjectRestrictNode is generated on top of every 
	 *     FromBaseTable and generated in place of every FromSubquery.  
	 *  o  Pushing single table predicates down to the new ProjectRestrictNodes.
	 *
	 * @param numTables			The number of tables in the DML Statement
	 * @param gbl				The group by list, if any
	 * @param fromList			The from list, if any
	 *
	 * @return ResultSetNode at top of preprocessed tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
		methodCall.preprocess(
								numTables,
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager()),
								 (SubqueryList) getNodeFactory().getNode(
								 					C_NodeTypes.SUBQUERY_LIST,
													getContextManager()),
								 (PredicateList) getNodeFactory().getNode(
								 					C_NodeTypes.PREDICATE_LIST,
													getContextManager()));
		/* Generate the referenced table map */
		referencedTableMap = new JBitSet(numTables);
		referencedTableMap.set(tableNumber);

		/* Create the dependency map.  This FromVTI depends on any
		 * tables which are referenced by the method call.  Note,
		 * though, that such tables should NOT appear in this node's
		 * referencedTableMap, since that field is really meant to
		 * hold the table numbers for any FromTables which appear
		 * AT OR UNDER the subtree whose root is this FromVTI.  That
		 * said, the tables referenced by methodCall do not appear
		 * "under" this FromVTI--on the contrary, they must appear
		 * "above" this FromVTI within the query tree in order to
		 * be referenced by the methodCall.  So methodCall table
		 * references do _not_ belong in this.referencedTableMap.
		 * (DERBY-3288)
		 */
		dependencyMap = new JBitSet(numTables);
		methodCall.categorize(dependencyMap, false);

		// Make sure this FromVTI does not "depend" on itself.
		dependencyMap.clear(tableNumber);

		// Get a JBitSet of the outer tables represented in the parameter list
		correlationMap = new JBitSet(numTables);
		methodCall.getCorrelationTables(correlationMap);

		return genProjectRestrict(numTables);
	}

	/** 
	 * Put a ProjectRestrictNode on top of each FromTable in the FromList.
	 * ColumnReferences must continue to point to the same ResultColumn, so
	 * that ResultColumn must percolate up to the new PRN.  However,
	 * that ResultColumn will point to a new expression, a VirtualColumnNode, 
	 * which points to the FromTable and the ResultColumn that is the source for
	 * the ColumnReference.  
	 * (The new PRN will have the original of the ResultColumnList and
	 * the ResultColumns from that list.  The FromTable will get shallow copies
	 * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
	 * will remain at the FromTable, with the PRN getting a new 
	 * VirtualColumnNode for each ResultColumn.expression.)
	 * We then project out the non-referenced columns.  If there are no referenced
	 * columns, then the PRN's ResultColumnList will consist of a single ResultColumn
	 * whose expression is 1.
	 *
	 * @param numTables			Number of tables in the DML Statement
	 *
	 * @return The generated ProjectRestrictNode atop the original FromTable.
	 *
	 * @exception StandardException		Thrown on error
	 */

	protected ResultSetNode genProjectRestrict(int numTables)
				throws StandardException
	{
		ResultColumnList	prRCList;

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		prRCList = resultColumns;
		resultColumns = resultColumns.copyListAndObjects();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 * NOTE: We don't want to mark the underlying RCs as referenced, otherwise
		 * we won't be able to project out any of them.
		 */
		prRCList.genVirtualColumnNodes(this, resultColumns, false);

		/* Project out any unreferenced columns.  If there are no referenced 
		 * columns, generate and bind a single ResultColumn whose expression is 1.
		 */
		prRCList.doProjection();

		/* Finally, we create the new ProjectRestrictNode */
		return (ResultSetNode) getNodeFactory().getNode(
								C_NodeTypes.PROJECT_RESTRICT_NODE,
								this,
								prRCList,
								null,	/* Restriction */
								null,   /* Restriction as PredicateList */
								null,	/* Project subquery list */
								null,	/* Restrict subquery list */
								tableProperties,
								getContextManager()	 );
	}

	/**
	 * Return whether or not to materialize this ResultSet tree.
	 *
	 * @return Whether or not to materialize this ResultSet tree.
	 *			would return valid results.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean performMaterialization(JBitSet outerTables)
		throws StandardException
	{
		/* We need to materialize the VTI iff:
		 *	o  It is an inner table.
		 *	o  The VTI can be materialized.
		 *	o  The VTI cannot be instantiated multiple times.
		 *	o  The join strategy does not do materialization.
		 * RESOLVE - We don't have to materialize if all of the
		 * outer tables are 1 row tables.
		 */
		return (outerTables.getFirstSetBit() != -1 &&	
				! outerTables.hasSingleBitSet() && // Not the outer table
				(! getTrulyTheBestAccessPath().
					getJoinStrategy().
						doesMaterialization()) &&		// Join strategy does not do materialization
					isMaterializable() &&					// VTI can be materialized
				! supportsMultipleInstantiations		// VTI does not support multiple instantiations
				);
	}

    /**
     * Compute the projection and restriction to be pushed to the external
     * table function if it is a RestrictedVTI. This method is called by the
     * parent ProjectRestrictNode at code generation time. See DERBY-4357.
     *
     * @param parentPredicates The full list of predicates to be applied by the parent ProjectRestrictNode
     */
    void computeProjectionAndRestriction( PredicateList parentPredicates )
        throws StandardException
    {
        // nothing to do if this is a not a restricted table function
        if ( !isRestrictedTableFunction ) { return; }

        computeRestriction( parentPredicates, computeProjection( ) );
    }
    /**
     * Fills in the array of projected column names suitable for handing to
     * RestrictedVTI.initScan(). Returns a map of the exposed column names
     * to the actual names of columns in the table function. This is useful
     * because the predicate refers to the exposed column names.
     */
    private HashMap computeProjection( ) throws StandardException
    {
        HashMap  nameMap = new HashMap();

        ResultColumnList allVTIColumns = getResultColumns();
        int              totalColumnCount = allVTIColumns.size();

        projectedColumnNames = new String[ totalColumnCount ];

        for ( int i = 0; i < totalColumnCount; i++ )
        {
            ResultColumn column = allVTIColumns.getResultColumn( i + 1 );
            String       exposedName = column.getName();

            if ( column.isReferenced() )
            {
                String       baseName = column.getBaseColumnNode().getColumnName();
                
                projectedColumnNames[ i ] = baseName;

                nameMap.put( exposedName, baseName );
            }
        }

        return nameMap;
    }
    /**
     * Fills in the restriction to be handed to a RestrictedVTI at run-time.
     *
     * @param parentPredicates The full list of predicates to be applied by the parent ProjectRestrictNode
     * @param columnNameMap Mapping between the exposed column names used in the predicates and the actual column names declared for the table function at CREATE FUNCTION time.
     */
    private void computeRestriction( PredicateList parentPredicates, HashMap columnNameMap )
        throws StandardException
    {
        if ( parentPredicates == null )  { return; }

        int predicateCount = parentPredicates.size();

        // walk the list, looking for qualifiers, that is, WHERE clause
        // fragments (conjuncts)  which can be pushed into the table function
        for ( int i = 0; i < predicateCount; i++ )
        {
            Predicate predicate = (Predicate) parentPredicates.elementAt( i );

            if ( canBePushedDown( predicate ) )
            {
                // A Predicate has a top level AND node
                Restriction newRestriction = makeRestriction( predicate.getAndNode(), columnNameMap );

                // If newRestriction is null, then we are confused. Don't push
                // the restriction into the table function
                if ( newRestriction == null )
                {
                    vtiRestriction = null;
                    return;
                }

                // If we get here, then we still understand the restriction
                // we're compiling.

                if ( vtiRestriction == null ) { vtiRestriction = newRestriction; }
                else { vtiRestriction = new Restriction.AND( vtiRestriction, newRestriction ); }
            }
        }
    }
    /** Return true if the predicate can be pushed into a RestrictedVTI */
    private boolean canBePushedDown( Predicate predicate ) throws StandardException
    {
        JBitSet referencedSet = predicate.getReferencedSet();

        // we want this to be a qualifier on only this FROM table */
        return
            (
             predicate.isQualifier() &&
             (referencedSet != null) &&
             (referencedSet.hasSingleBitSet() ) &&
             (referencedSet.get( getTableNumber() ) )
             );
    }
    /**
     * Turn a compile-time WHERE clause fragment into a run-time
     * Restriction. Returns null if the clause could not be understood.
     *
     * @param clause The clause which should be turned into a Restriction.
     * @param columnNameMap Mapping between the exposed column names used in the predicates and the actual column names declared for the table function at CREATE FUNCTION time.
     */
    private Restriction makeRestriction( ValueNode clause, HashMap columnNameMap )
        throws StandardException
    {
        if ( clause instanceof AndNode )
        {
            AndNode andOperator = (AndNode) clause;

            // strip off trailing vacuous TRUE constant if present
            if ( andOperator.getRightOperand().isBooleanTrue() )
            { return makeRestriction( andOperator.getLeftOperand(), columnNameMap ); }
            
            Restriction leftRestriction = makeRestriction( andOperator.getLeftOperand(), columnNameMap );
            Restriction rightRestriction = makeRestriction( andOperator.getRightOperand(), columnNameMap );

            if ( (leftRestriction == null) || (rightRestriction == null) ) { return null; }

            return new Restriction.AND( leftRestriction, rightRestriction );
        }
        else if ( clause instanceof OrNode )
        {
            OrNode orOperator = (OrNode) clause;
            
            // strip off trailing vacuous FALSE constant if present
            if ( orOperator.getRightOperand().isBooleanFalse() )
            { return makeRestriction( orOperator.getLeftOperand(), columnNameMap ); }
            
            Restriction leftRestriction = makeRestriction( orOperator.getLeftOperand(), columnNameMap );
            Restriction rightRestriction = makeRestriction( orOperator.getRightOperand(), columnNameMap );

            if ( (leftRestriction == null) || (rightRestriction == null) ) { return null; }

            return new Restriction.OR( leftRestriction, rightRestriction );
        }
        else if ( clause instanceof BinaryRelationalOperatorNode )
        { return makeLeafRestriction( (BinaryRelationalOperatorNode) clause, columnNameMap ); }
        else if ( clause instanceof IsNullNode )
        { return makeIsNullRestriction( (IsNullNode) clause, columnNameMap ); }
        else { return iAmConfused( clause ); }
    }
    /**
     * Makes a Restriction out of a comparison between a constant and a column
     * in the VTI.
     *
     * @param clause The clause which should be turned into a Restriction.
     * @param columnNameMap Mapping between the exposed column names used in the predicates and the actual column names declared for the table function at CREATE FUNCTION time.
     */
    private Restriction makeLeafRestriction( BinaryRelationalOperatorNode clause, HashMap columnNameMap )
        throws StandardException
    {
        int rawOperator = clause.getOperator();
        ColumnReference rawColumn;
        ValueNode rawValue;

        if ( clause.getLeftOperand() instanceof ColumnReference )
        {
            rawColumn = (ColumnReference) clause.getLeftOperand();
            rawValue = clause.getRightOperand();
        }
        else if ( clause.getRightOperand() instanceof ColumnReference )
        {
            rawColumn = (ColumnReference) clause.getRightOperand();
            rawValue = clause.getLeftOperand();
            rawOperator = flipOperator( rawOperator );
        }
        else { return iAmConfused( clause ); }

        int comparisonOperator = mapOperator( rawOperator );
        if ( comparisonOperator < 0 ) { return iAmConfused( clause ); }

        String columnName = (String) columnNameMap.get( rawColumn.getColumnName() );
        Object constantOperand = squeezeConstantValue( rawValue );
        if ( (columnName == null) || (constantOperand == null) ) { return iAmConfused( clause ); }

        return new Restriction.ColumnQualifier( columnName, comparisonOperator, constantOperand );
    }
    /**
     * Makes an IS NULL comparison of a column
     * in the VTI.
     *
     * @param clause The IS NULL (or IS NOT NULL) node
     * @param columnNameMap Mapping between the exposed column names used in the predicates and the actual column names declared for the table function at CREATE FUNCTION time.
     */
    private Restriction makeIsNullRestriction( IsNullNode clause, HashMap columnNameMap )
        throws StandardException
    {
        ColumnReference rawColumn = (ColumnReference) clause.getOperand();

        int comparisonOperator = mapOperator( clause.getOperator() );
        if ( comparisonOperator < 0 ) { return iAmConfused( clause ); }
        if (
            (comparisonOperator != Restriction.ColumnQualifier.ORDER_OP_ISNULL) &&
            (comparisonOperator != Restriction.ColumnQualifier.ORDER_OP_ISNOTNULL)
            ) { return iAmConfused( clause ); }

        String columnName = (String) columnNameMap.get( rawColumn.getColumnName() );
        if ( columnName == null ) { return iAmConfused( clause ); }

        return new Restriction.ColumnQualifier( columnName, comparisonOperator, null );
    }
    /** This is a handy place to put instrumentation for tracing trees which we don't understand */
    private Restriction iAmConfused( ValueNode clause ) throws StandardException
    {
        return null;
    }
    /** Flip the sense of a comparison */
    private int flipOperator( int rawOperator ) throws StandardException
    {
        switch( rawOperator )
        {
        case RelationalOperator.EQUALS_RELOP:         return RelationalOperator.EQUALS_RELOP;
        case RelationalOperator.GREATER_EQUALS_RELOP: return RelationalOperator.LESS_EQUALS_RELOP;
        case RelationalOperator.GREATER_THAN_RELOP:   return RelationalOperator.LESS_THAN_RELOP;
        case RelationalOperator.LESS_EQUALS_RELOP:    return RelationalOperator.GREATER_EQUALS_RELOP;
        case RelationalOperator.LESS_THAN_RELOP:      return RelationalOperator.GREATER_THAN_RELOP;
        case RelationalOperator.NOT_EQUALS_RELOP:     return RelationalOperator.NOT_EQUALS_RELOP;

        case RelationalOperator.IS_NOT_NULL_RELOP:
        case RelationalOperator.IS_NULL_RELOP:
        default:
            if ( SanityManager.DEBUG )
            {
                SanityManager.THROWASSERT( "Unrecognized relational operator: " + rawOperator );
            }
        }

        return -1;
    }
    /** Map internal operator constants to user-visible ones */
    private int mapOperator( int rawOperator ) throws StandardException
    {
        switch( rawOperator )
        {
        case RelationalOperator.EQUALS_RELOP:         return Restriction.ColumnQualifier.ORDER_OP_EQUALS;
        case RelationalOperator.GREATER_EQUALS_RELOP: return Restriction.ColumnQualifier.ORDER_OP_GREATEROREQUALS;
        case RelationalOperator.GREATER_THAN_RELOP:   return Restriction.ColumnQualifier.ORDER_OP_GREATERTHAN;
        case RelationalOperator.LESS_EQUALS_RELOP:    return Restriction.ColumnQualifier.ORDER_OP_LESSOREQUALS;
        case RelationalOperator.LESS_THAN_RELOP:      return Restriction.ColumnQualifier.ORDER_OP_LESSTHAN;
        case RelationalOperator.IS_NULL_RELOP:        return Restriction.ColumnQualifier.ORDER_OP_ISNULL;
        case RelationalOperator.IS_NOT_NULL_RELOP:    return Restriction.ColumnQualifier.ORDER_OP_ISNOTNULL;
        case RelationalOperator.NOT_EQUALS_RELOP:     return Restriction.ColumnQualifier.ORDER_OP_NOT_EQUALS;

        default:
            if ( SanityManager.DEBUG )
            {
                SanityManager.THROWASSERT( "Unrecognized relational operator: " + rawOperator );
            }
        }

        return -1;
    }
    /**
     * Get the constant or parameter reference out of a comparand. Return null
     * if we are confused. A parameter reference is wrapped in an integer array
     * to distinguish it from a constant integer.
     */
    private Object squeezeConstantValue( ValueNode valueNode ) throws StandardException
    {
        if ( valueNode instanceof ParameterNode )
        {
            return new int[] { ((ParameterNode) valueNode).getParameterNumber() };
        }
        else if ( valueNode instanceof ConstantNode )
        {
            return ((ConstantNode) valueNode).getValue().getObject();
        }
        else
        {
            return iAmConfused( valueNode );
        }
    }
    
	/**
	 * Generation on a FromVTI creates a wrapper around
	 * the user's java.sql.ResultSet
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb The MethodBuilder for the execute() method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
        // If necessary, compute the projection to be pushed into the table
        // function
        if ( isRestrictedTableFunction && ( projectedColumnNames == null) ) { computeProjection(); }
        
		/* NOTE: We need to remap any CRs within the parameters
		 * so that we get their values from the right source
		 * row.  For example, if a CR is a join column, we need
		 * to get the value from the source table and not the
		 * join row since the join row hasn't been filled in yet.
		 */
		RemapCRsVisitor rcrv = new RemapCRsVisitor(true);
		methodCall.accept(rcrv);

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		acb.pushGetResultSetFactoryExpression(mb);
		int nargs = getScanArguments(acb, mb);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getVTIResultSet",ClassName.NoPutResultSet, nargs);
	}

	private int getScanArguments(ActivationClassBuilder acb,
										  MethodBuilder mb)
		throws StandardException
	{
		int				rclSize = resultColumns.size();
		FormatableBitSet			referencedCols = new FormatableBitSet(rclSize);
		int				erdNumber = -1;
		int				numSet = 0;

		// Get our final cost estimate.
		costEstimate = getFinalCostEstimate();

		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
			if (rc.isReferenced())
			{
				referencedCols.set(index);
				numSet++;
			}
		}

		// Only add referencedCols if not all columns are accessed
		if (numSet != numVTICols)
		{
			erdNumber = acb.addItem(referencedCols);
		}

		// compileTimeConstants can be null
		int ctcNumber = acb.addItem(compileTimeConstants);

		acb.pushThisAsActivation(mb); // arg 1

        // get a function to allocate scan rows of the right shape and size
		resultColumns.generateHolder(acb, mb); // arg 2

		// For a Version 2 VTI we never maintain the java.sql.PreparedStatement
		// from compile time to execute time. This would rquire the PreparedStatement
		// to be shareable across multiple connections, which is not the model for
		// java.sql.PreparedStatement.

		// For a Version 2 VTI we do pass onto the ResultSet the re-useability
		// of the java.sql.PreparedStatement at runtime. The java.sql.PreparedStatement
		// is re-uesable if
		//
		// o  No ? or ColumnReferences in parameters

		boolean reuseablePs = version2 &&
			(getNodesFromParameters(ParameterNode.class).size() == 0) &&
			(getNodesFromParameters(ColumnReference.class).size() == 0);



		mb.push(resultSetNumber); // arg 3

		// The generated method for the constructor
		generateConstructor(acb, mb, reuseablePs); // arg 4

		// Pass in the class name
		mb.push(methodCall.getJavaClassName()); // arg 5

		if (restrictionList != null) {
			restrictionList.generateQualifiers(acb, mb, this, true);
		}
		else
			mb.pushNull(ClassName.Qualifier + "[][]");

		// Pass in the erdNumber for the referenced column FormatableBitSet
		mb.push(erdNumber); // arg 6

		// Whether or not this is a version 2 VTI
		mb.push(version2);

		mb.push(reuseablePs);

		mb.push(ctcNumber);

		// Whether or not this is a target VTI
		mb.push(isTarget);

		// isolation level of the scan (if specified)
		mb.push(getCompilerContext().getScanIsolationLevel());

		// estimated row count
		mb.push(costEstimate.rowCount());

		// estimated cost
		mb.push(costEstimate.getEstimatedCost());

		// Whether or not this is a Derby-style Table Function
		mb.push(isDerbyStyleTableFunction);

		// Push the return type
        int rtNum = -1;
        if ( isDerbyStyleTableFunction  )
        {
            rtNum = acb.addItem(methodCall.getRoutineInfo().getReturnType());
        }
        mb.push(rtNum);

        // push the projection and restriction for RestrictedVTIs
        mb.push( storeObjectInPS( acb, projectedColumnNames ) );
        mb.push( storeObjectInPS( acb, vtiRestriction ) );        

		return 18;
	}
    /** Store an object in the prepared statement.  Returns -1 if the object is
     * null. Otherwise returns the object's retrieval handle.
     */
    private int storeObjectInPS( ActivationClassBuilder acb, Object obj ) throws StandardException
    {
        if ( obj == null ) { return -1; }
        else { return acb.addItem( obj ); }
    }

	private void generateConstructor(ActivationClassBuilder acb,
										   MethodBuilder mb, boolean reuseablePs)
		throws StandardException
	{
        
        String vtiType = version2 ?
                "java.sql.PreparedStatement" : "java.sql.ResultSet";
		// this sets up the method and the static field.
		// generates:
		// 	java.sql.ResultSet userExprFun { }
		MethodBuilder userExprFun = acb.newGeneratedFun(
                vtiType, Modifier.PUBLIC);
		userExprFun.addThrownException("java.lang.Exception");


		// If it's a re-useable PreparedStatement then hold onto it.
		LocalField psHolder = reuseablePs ? acb.newFieldDeclaration(Modifier.PRIVATE, "java.sql.PreparedStatement") : null;

		if (reuseablePs) {

			userExprFun.getField(psHolder);
			userExprFun.conditionalIfNull();
		}

		methodCall.generateExpression(acb, userExprFun);
        userExprFun.upCast(vtiType);

		if (reuseablePs) {

			userExprFun.putField(psHolder);

			userExprFun.startElseCode();

			userExprFun.getField(psHolder);

			userExprFun.completeConditional();
		}

		userExprFun.methodReturn();



		// methodCall knows it is returning its value;

		/* generates:
		 *    return <newInvocation.generate(acb)>;
		 */
		// we are done modifying userExprFun, complete it.
		userExprFun.complete();

   		// constructor is used in the final result set as an access of the new static
		// field holding a reference to this new method.
		// generates:
		//	ActivationClass.userExprFun
		// which is the static field that "points" to the userExprFun
		// that evaluates the where clause.
		acb.pushMethodReference(mb, userExprFun);


		// now add in code to close the reusable PreparedStatement when
		// the activation is closed.
		if (reuseablePs) {
			MethodBuilder closeActivationMethod = acb.getCloseActivationMethod();

			closeActivationMethod.getField(psHolder);
			closeActivationMethod.conditionalIfNull();
			  // do nothing
			  closeActivationMethod.push(0); // work around for no support for real if statements
			closeActivationMethod.startElseCode(); 
			  closeActivationMethod.getField(psHolder);
			  closeActivationMethod.callMethod(VMOpcode.INVOKEINTERFACE, "java.sql.Statement",
				  "close", "void", 0);
			  closeActivationMethod.push(0);

			closeActivationMethod.completeConditional();
			closeActivationMethod.endStatement();
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
		return (! baseTable) && name.equals(methodCall.getJavaClassName());
	}

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

		if (methodCall != null)
		{
			methodCall = (MethodCallNode) methodCall.accept(v);
		}
	}

	/**
	 * Check and see if we have a special trigger VTI.
	 * If it cannot be bound (because we aren't actually 
	 * compiling or executing a trigger), then throw 
	 * an exception.
	 * 
	 * @return null if not a special trigger vti, or the table
	 * id if it is
	 */
	private UUID getSpecialTriggerVTITableName(LanguageConnectionContext lcc, String className)
		throws StandardException
	{
		if (className.equals(ClassName.TriggerNewTransitionRows) ||
		    className.equals(ClassName.TriggerOldTransitionRows))
		{
			// if there isn't an active trigger being compiled, error
			if (lcc.getTriggerTable() != null)
			{
				return lcc.getTriggerTable().getUUID();
			}
			else if (lcc.getTriggerExecutionContext() != null)
			{
				return lcc.getTriggerExecutionContext().getTargetTableId();
			}
			else
			{
				throw StandardException.newException(SQLState.LANG_CANNOT_BIND_TRIGGER_V_T_I, className);
			}
		}
		return (UUID)null;
	}

	private ResultColumnList genResultColList(TableDescriptor td)
			throws StandardException
	{
		ResultColumnList 			rcList = null;
		ResultColumn	 			resultColumn;
		ValueNode		 			valueNode;
		ColumnDescriptor 			colDesc = null;

		/* Add all of the columns in the table */
		rcList = (ResultColumnList) getNodeFactory().getNode(
										C_NodeTypes.RESULT_COLUMN_LIST,
										getContextManager());
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		int					 cdlSize = cdl.size();

		for (int index = 0; index < cdlSize; index++)
		{
			/* Build a ResultColumn/BaseColumnNode pair for the column */
			colDesc = (ColumnDescriptor) cdl.elementAt(index);

			valueNode = (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.BASE_COLUMN_NODE,
											colDesc.getColumnName(),
									  		exposedName,
											colDesc.getType(),
											getContextManager());
			resultColumn = (ResultColumn) getNodeFactory().getNode(
											C_NodeTypes.RESULT_COLUMN,
											colDesc,
											valueNode,
											getContextManager());

			/* Build the ResultColumnList to return */
			rcList.addResultColumn(resultColumn);
		}

		return rcList;
	}
	
	public boolean needsSpecialRCLBinding()
	{
		return true;
	}

	boolean isUpdatableCursor() throws StandardException {
		return true;
	}

	/*
	** VTIEnvironment
	*/
	public final boolean isCompileTime() {
		return true;
	}

	public String getOriginalSQL() {
		return getCompilerContext().getParser().getSQLtext();
	}
	
	public final int getStatementIsolationLevel() {
		return ExecutionContext.CS_TO_JDBC_ISOLATION_LEVEL_MAP[getCompilerContext().getScanIsolationLevel()];
	}

	public void setSharedState(String key, java.io.Serializable value) {

		if (key == null)
			return;

		if (compileTimeConstants == null)
			compileTimeConstants = new FormatableHashtable();

		compileTimeConstants.put(key, value);
	}

	public Object getSharedState(String key) {
		if ((key == null) || (compileTimeConstants == null))
			return null;

		return compileTimeConstants.get(key);
	}

    /**
     * Add result columns for a Derby-style Table Function
     */
    private void    createResultColumnsForTableFunction
        (TypeDescriptor td)
        throws StandardException
    {
        String[] columnNames = td.getRowColumnNames();
        TypeDescriptor[] types = td.getRowTypes();
        for ( int i = 0; i < columnNames.length; i++ )
        {
            resultColumns.addColumn( exposedName, columnNames[ i ],
                    DataTypeDescriptor.getType(types[i]));
        }

    }

    /**
     * Return true if this Derby Style Table Function implements the VTICosting
     * interface. The class must satisfy the following conditions:
     *
     * <ul>
     * <li>Implements VTICosting</li>
     * <li>Has a public, no-arg constructor</li>
     * </ul>
     */
    private boolean implementsDerbyStyleVTICosting( String className )
        throws StandardException
    {
        Constructor     constructor = null;
        Class           vtiClass = lookupClass( className );
        Class           vtiCostingClass = lookupClass( VTICosting.class.getName() );

        try {
            if ( !vtiCostingClass.isAssignableFrom( vtiClass ) ) { return false; }
        }
        catch (Throwable t)
        {
            throw StandardException.unexpectedUserException( t );
        }

        try {
            constructor = vtiClass.getConstructor( new Class[] {} );
        }
        catch (Throwable t)
        {
            throw StandardException.newException
                ( SQLState.LANG_NO_COSTING_CONSTRUCTOR, t, className );
        }
        
        if ( Modifier.isPublic( constructor.getModifiers() ) ) { return true; }

        // Bad class. It thinks it implements VTICosting, but it doesn't
        // have a public no-arg constructor
        throw StandardException.newException
            ( SQLState.LANG_NO_COSTING_CONSTRUCTOR, className );
    }

    /**
     * Get the VTICosting implementation for this optimizable VTI.
     */
    private VTICosting  getVTICosting()
        throws StandardException
    {
        if ( !isDerbyStyleTableFunction ) { return (version2) ? (VTICosting) ps : (VTICosting) rs; }
        
        String              className = methodCall.getJavaClassName();
        Class               vtiClass = lookupClass( className );
        
        try {
            Constructor         constructor = vtiClass.getConstructor( new Class[] {} );
            VTICosting          result = (VTICosting) constructor.newInstance( null );

            return result;
        }
        catch (Throwable t)
        {
            throw StandardException.unexpectedUserException( t );
        }
    }

    /**
     * Lookup the class that holds the VTI.
     */
    private Class lookupClass( String className )
        throws StandardException
    {
        try {
            return getClassFactory().getClassInspector().getClass( className );
        }
        catch (ClassNotFoundException t)
        {
            throw StandardException.unexpectedUserException( t );
        }
    }

}
