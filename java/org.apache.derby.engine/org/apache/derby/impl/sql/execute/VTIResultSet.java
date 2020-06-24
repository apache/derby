/*

   Derby - Class org.apache.derby.impl.sql.execute.VTIResultSet

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

import org.apache.derby.catalog.TypeDescriptor;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecRowBuilder;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ParameterValueSet; 
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.VariableSizeDataValue;
import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.transaction.TransactionControl;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableHashtable;

import org.apache.derby.vti.DeferModification;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;
import org.apache.derby.vti.AwareVTI;
import org.apache.derby.vti.RestrictedVTI;
import org.apache.derby.vti.Restriction;
import org.apache.derby.vti.VTIContext;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.ResultSetMetaData;
import org.w3c.dom.Element;

/**
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-1700
class VTIResultSet extends NoPutResultSetImpl
	implements CursorResultSet, VTIEnvironment {

	/* Run time statistics variables */
	public int rowsReturned;
	public String javaClassName;

    private GeneratedMethod constructor;
	private PreparedStatement userPS;
	private ResultSet userVTI;
	private final ExecRow allocatedRow;
	private FormatableBitSet referencedColumns;
	private boolean version2;
	private boolean reuseablePs;
	private boolean isTarget;
    private final FormatableHashtable compileTimeConstants;

	private boolean pushedProjection;
	private IFastPath	fastPath;

	private Qualifier[][]	pushedQualifiers;

	private boolean[] runtimeNullableColumn;

	private boolean isDerbyStyleTableFunction;

    private final TypeDescriptor returnType;

    private DataTypeDescriptor[]    returnColumnTypes;

    private String[] vtiProjection;
    private Restriction vtiRestriction;

    private String  vtiSchema;
    private String  vtiName;

	/**
		Specified isolation level of SELECT (scan). If not set or
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
		not application, it will be set to TransactionControl.UNSPECIFIED_ISOLATION_LEVEL
	*/
	private int scanIsolationLevel = TransactionControl.UNSPECIFIED_ISOLATION_LEVEL;

    //
    // class interface
    //
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
    VTIResultSet(Activation activation, int row, int resultSetNumber,
				 GeneratedMethod constructor,
				 String javaClassName,
				 Qualifier[][] pushedQualifiers,
				 int erdNumber,
				 boolean version2, boolean reuseablePs,
				 int ctcNumber,
				 boolean isTarget,
				 int scanIsolationLevel,
			     double optimizerEstimatedRowCount,
				 double optimizerEstimatedCost,
				 boolean isDerbyStyleTableFunction,
                 int returnTypeNumber,
                 int vtiProjectionNumber,
                 int vtiRestrictionNumber,
//IC see: https://issues.apache.org/jira/browse/DERBY-6117
                 String vtiSchema,
                 String vtiName
                 ) 
		throws StandardException
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
		this.constructor = constructor;
		this.javaClassName = javaClassName;
		this.version2 = version2;
		this.reuseablePs = reuseablePs;
		this.isTarget = isTarget;
		this.pushedQualifiers = pushedQualifiers;
		this.scanIsolationLevel = scanIsolationLevel;
		this.isDerbyStyleTableFunction = isDerbyStyleTableFunction;
//IC see: https://issues.apache.org/jira/browse/DERBY-6117
        this.vtiSchema = vtiSchema;
        this.vtiName = vtiName;

        ExecPreparedStatement ps = activation.getPreparedStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-6003

        this.allocatedRow = ((ExecRowBuilder) ps.getSavedObject(row))
                .build(activation.getExecutionFactory());

//IC see: https://issues.apache.org/jira/browse/DERBY-3616
        this.returnType = returnTypeNumber == -1 ? null :
            (TypeDescriptor)
            activation.getPreparedStatement().getSavedObject(returnTypeNumber);

        this.vtiProjection = vtiProjectionNumber == -1 ? null :
            (String[])
            activation.getPreparedStatement().getSavedObject(vtiProjectionNumber);

        this.vtiRestriction = vtiRestrictionNumber == -1 ? null :
            (Restriction)
            activation.getPreparedStatement().getSavedObject(vtiRestrictionNumber);

		if (erdNumber != -1)
		{
			this.referencedColumns = (FormatableBitSet)(activation.getPreparedStatement().
								getSavedObject(erdNumber));
		}

		compileTimeConstants = (FormatableHashtable) (activation.getPreparedStatement().
								getSavedObject(ctcNumber));

        // compileTimeConstants cannot be null, even if there are no
        // constants, since VTIResultSet.setSharedState() may want to
        // add constants to it during execution.
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(compileTimeConstants != null,
                                 "compileTimeConstants is null");
        }

		recordConstructorTime();
    }

	//
	// ResultSet interface (leftover from NoPutResultSet)
	//


	/**
     * Sets state to 'open'.
	 *
	 * @exception StandardException thrown if activation closed.
     */
	public void	openCore() throws StandardException 
	{
		beginTime = getCurrentTimeMillis();
		if (SanityManager.DEBUG)
		    SanityManager.ASSERT( ! isOpen, "VTIResultSet already open");

	    isOpen = true;
		numOpens++;

		/* We need to Instantiate the user's ResultSet on the each open since
		 * there is no way to close and then reopen a java.sql.ResultSet.
		 * For Version 2 VTIs, we may be able to skip instantiated their
		 * PreparedStatement here.
		 */
		try {
			if (version2)
			{
				userPS = (PreparedStatement) constructor.invoke(activation);

				if (userPS instanceof org.apache.derby.vti.Pushable) {
					org.apache.derby.vti.Pushable p = (org.apache.derby.vti.Pushable) userPS;
					if (referencedColumns != null) {
						pushedProjection = p.pushProjection(this, getProjectedColList());
					}
				}

				if (userPS instanceof org.apache.derby.vti.IQualifyable) {
					org.apache.derby.vti.IQualifyable q = (org.apache.derby.vti.IQualifyable) userPS;

					q.setQualifiers(this, pushedQualifiers);
				}
				fastPath = userPS instanceof IFastPath ? (IFastPath) userPS : null;

                if( isTarget
                    && userPS instanceof DeferModification
                    && activation.getConstantAction() instanceof UpdatableVTIConstantAction)
                {
                    UpdatableVTIConstantAction constants = (UpdatableVTIConstantAction) activation.getConstantAction();
                    ((DeferModification) userPS).modificationNotify( constants.statementType, constants.deferred);
                }
                
				if ((fastPath != null) && fastPath.executeAsFastPath())
					;
				else
					userVTI = userPS.executeQuery();

				/* Save off the target VTI */
				if (isTarget)
				{
					activation.setTargetVTI(userVTI);
				}

			}
			else
			{
				userVTI = (ResultSet) constructor.invoke(activation);

                if ( userVTI instanceof RestrictedVTI )
                {
                    RestrictedVTI restrictedVTI = (RestrictedVTI) userVTI;

                    restrictedVTI.initScan( vtiProjection, cloneRestriction( activation ) );
                }

//IC see: https://issues.apache.org/jira/browse/DERBY-6117
                if ( userVTI instanceof AwareVTI )
                {
                    AwareVTI awareVTI = (AwareVTI) userVTI;

                    awareVTI.setContext
                        (
                         new VTIContext
                         (
                          vtiSchema,
                          vtiName,
                          activation.getLanguageConnectionContext().getStatementContext().getStatementText()
                          )
                         );
                }
			}

			// Set up the nullablity of the runtime columns, may be delayed
			setNullableColumnList();
		}
		catch (Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}


		openTime += getElapsedMillis(beginTime);
	}

    /**
     * Clone the restriction for a Restricted VTI, filling in parameter values
     * as necessary.
     */
    private Restriction cloneRestriction( Activation activation ) throws StandardException
    {
        if ( vtiRestriction == null ) { return null; }
        else { return cloneRestriction( activation, vtiRestriction ); }
    }
    private Restriction cloneRestriction( Activation activation, Restriction original )
        throws StandardException
    {
        if ( original instanceof Restriction.AND)
        {
            Restriction.AND and = (Restriction.AND) original;
            
            return new Restriction.AND
                (
                 cloneRestriction( activation, and.getLeftChild() ),
                 cloneRestriction( activation, and.getRightChild() )
                 );
        }
        else if ( original instanceof Restriction.OR)
        {
            Restriction.OR or = (Restriction.OR) original;
            
            return new Restriction.OR
                (
                 cloneRestriction( activation, or.getLeftChild() ),
                 cloneRestriction( activation, or.getRightChild() )
                 );
        }
        else if ( original instanceof Restriction.ColumnQualifier)
        {
            Restriction.ColumnQualifier cq = (Restriction.ColumnQualifier) original;
            Object originalConstant = cq.getConstantOperand();
            Object newConstant;

            if ( originalConstant ==  null ) { newConstant = null; }
            else if ( originalConstant instanceof int[] )
            {
                int parameterNumber = ((int[]) originalConstant)[ 0 ];
                ParameterValueSet pvs = activation.getParameterValueSet();

                newConstant = pvs.getParameter( parameterNumber ).getObject();
            }
            else { newConstant = originalConstant; }
           
            return new Restriction.ColumnQualifier
                (
                 cq.getColumnName(),
                 cq.getComparisonOperator(),
                 newConstant
                 );
        }
        else
        {
            throw StandardException.newException( SQLState.NOT_IMPLEMENTED, original.getClass().getName() );
        }
    }

	private boolean[] setNullableColumnList() throws SQLException, StandardException {

		if (runtimeNullableColumn != null)
			return runtimeNullableColumn;

		// Derby-style table functions return SQL rows which don't have not-null
		// constraints bound to them
		if ( isDerbyStyleTableFunction )
		{
		    int         count = getAllocatedRow().nColumns() + 1;
            
		    runtimeNullableColumn = new boolean[ count ];
		    for ( int i = 0; i < count; i++ )   { runtimeNullableColumn[ i ] = true; }
            
		    return runtimeNullableColumn;
		}

		if (userVTI == null)
			return null;

		ResultSetMetaData rsmd = userVTI.getMetaData();
		boolean[] nullableColumn = new boolean[rsmd.getColumnCount() + 1];
		for (int i = 1; i <  nullableColumn.length; i++) {
			nullableColumn[i] = rsmd.isNullable(i) != ResultSetMetaData.columnNoNulls;
		}

		return runtimeNullableColumn = nullableColumn;
	}

	/**
	 * If the VTI is a version2 vti that does not
	 * need to be instantiated multiple times then
	 * we simply close the current ResultSet and 
	 * create a new one via a call to 
	 * PreparedStatement.executeQuery().
	 *
	 * @see NoPutResultSet#openCore
	 * @exception StandardException thrown if cursor finished.
	 */
	public void reopenCore() throws StandardException
	{
		if (reuseablePs)
		{
			/* close the user ResultSet.
			 */
			if (userVTI != null)
			{
				try
				{
					userVTI.close();
					userVTI = userPS.executeQuery();

					/* Save off the target VTI */
					if (isTarget)
					{
						activation.setTargetVTI(userVTI);
					}
				} catch (SQLException se)
				{
					throw StandardException.unexpectedUserException(se);
				}
			}
		}
		else
		{
			close();
			openCore();	
		}
	}

	/**
     * If open and not returned yet, returns the row
     * after plugging the parameters into the expressions.
	 *
	 * @exception StandardException thrown on failure.
     */
	public ExecRow	getNextRowCore() throws StandardException 
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6216
		if( isXplainOnlyMode() )
			return null;

	    ExecRow result = null;

		beginTime = getCurrentTimeMillis();
		
		if ( isOpen ) 
		{
			try
			{
				if ((userVTI == null) && (fastPath != null)) {
					result = getAllocatedRow();
					int action = fastPath.nextRow(result.getRowArray());
					if (action == IFastPath.GOT_ROW)
						;
					else if (action == IFastPath.SCAN_COMPLETED)
						result = null;
					else if (action == IFastPath.NEED_RS) {
						userVTI = userPS.executeQuery();
					}
				}
				if ((userVTI != null))
                {
                    if( ! userVTI.next())
                    {
                        if( null != fastPath)
                            fastPath.rowsDone();
                        result = null;
                    }
                    else
                    {
                        // Get the cached row and fill it up
                        result = getAllocatedRow();
                        populateFromResultSet(result);
                        if (fastPath != null)
                        { fastPath.currentRow(userVTI, result.getRowArray()); }
//IC see: https://issues.apache.org/jira/browse/DERBY-6151

                        SQLWarning  warnings = userVTI.getWarnings();
                        if ( warnings != null ) { addWarning( warnings ); }
                    }
				}
			}
			catch (Throwable t)
			{
				throw StandardException.unexpectedUserException(t);
			}

		}

		setCurrentRow(result);
		if (result != null)
		{
			rowsReturned++;
			rowsSeen++;
		}

		nextTime += getElapsedMillis(beginTime);
	    return result;
	}

	

	/**
     * @see org.apache.derby.iapi.sql.ResultSet#close
	 *
	 * @exception StandardException thrown on error
	 */
	public void	close() throws StandardException
	{
		beginTime = getCurrentTimeMillis();
		if (isOpen) {

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();

			/* close the user ResultSet.  We have to eat any exception here
			 * since our close() method cannot throw an exception.
			 */
			if (userVTI != null)
			{
				try
				{
					userVTI.close();
				} catch (SQLException se)
				{
					throw StandardException.unexpectedUserException(se);
				}
				finally {
					userVTI = null;
				}
			}
			if ((userPS != null) && !reuseablePs)
			{
				try
				{
					userPS.close();
				} catch (SQLException se)
				{
					throw StandardException.unexpectedUserException(se);
				}
				finally {
					userPS = null;
				}
			}
			super.close();
		}
		else
			if (SanityManager.DEBUG)
				SanityManager.DEBUG("CloseRepeatInfo","Close of VTIResultSet repeated");

		closeTime += getElapsedMillis(beginTime);
	}

	public void finish() throws StandardException {

		// for a reusablePS it will be closed by the activation
		// when it is closed.
		if ((userPS != null) && !reuseablePs)
		{
			try
			{
				userPS.close();
				userPS = null;
			} catch (SQLException se)
			{
				throw StandardException.unexpectedUserException(se);
			}
		}

		finishAndRTS();

	}

	/**
	 * Return the total amount of time spent in this ResultSet
	 *
	 * @param type	CURRENT_RESULTSET_ONLY - time spent only in this ResultSet
	 *				ENTIRE_RESULTSET_TREE  - time spent in this ResultSet and below.
	 *
	 * @return long		The total amount of time spent (in milliseconds).
	 */
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;
		return totTime;
	}

	//
	// CursorResultSet interface
	//

	/**
	 * This is not operating against a stored table,
	 * so it has no row location to report.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public RowLocation getRowLocation() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

	/**
	 * This is not used in positioned update and delete,
	 * so just return a null.
	 *
	 * @see CursorResultSet
	 *
	 * @return a null.
	 */
	public ExecRow getCurrentRow() {
		if (SanityManager.DEBUG)
			SanityManager.THROWASSERT("RowResultSet used in positioned update/delete");
		return null;
	}

	// Class implementation

	/**
	 * Return the GeneratedMethod for instantiating the VTI.
	 *
	 * @return The  GeneratedMethod for instantiating the VTI.
	 */
	GeneratedMethod getVTIConstructor()
	{
		return constructor;
	}

	boolean isReuseablePs() {
		return reuseablePs;
	}


	/**
	 * Cache the ExecRow for this result set.
	 *
	 * @return The cached ExecRow for this ResultSet
	 *
	 * @exception StandardException thrown on failure.
	 */
	private ExecRow getAllocatedRow()
		throws StandardException
	{
		return allocatedRow;
	}

	private int[] getProjectedColList() {

		FormatableBitSet refs = referencedColumns;
		int size = refs.size();
		int arrayLen = 0;
		for (int i = 0; i < size; i++) {
			if (refs.isSet(i))
				arrayLen++;
		}

		int[] colList = new int[arrayLen];
		int offset = 0;
		for (int i = 0; i < size; i++) {
			if (refs.isSet(i))
				colList[offset++] = i + 1;
		}

		return colList;
	}
	/**
	 * @exception StandardException thrown on failure to open
	 */
	public void populateFromResultSet(ExecRow row)
		throws StandardException
	{
		try
		{
            DataTypeDescriptor[]    columnTypes = null;
            if ( isDerbyStyleTableFunction )
            {
                    columnTypes = getReturnColumnTypes();
            }

			boolean[] nullableColumn = setNullableColumnList();
			DataValueDescriptor[] columns = row.getRowArray();
			// ExecRows are 0-based, ResultSets are 1-based
			int rsColNumber = 1;
			for (int index = 0; index < columns.length; index++)
			{
				// Skip over unreferenced columns
				if (referencedColumns != null && (! referencedColumns.get(index)))
				{
					if (!pushedProjection)
						rsColNumber++;

					continue;
				}

				columns[index].setValueFromResultSet(
									userVTI, rsColNumber, 
									/* last parameter is whether or
									 * not the column is nullable
									 */
									nullableColumn[rsColNumber]);
				rsColNumber++;

                // for Derby-style table functions, coerce the value coming out
                // of the ResultSet to the declared SQL type of the return
                // column
                if ( isDerbyStyleTableFunction )
                {
                    DataTypeDescriptor  dtd = columnTypes[ index ];
                    DataValueDescriptor dvd = columns[ index ];

                    cast( dtd, dvd );
                }

            }

		} catch (StandardException se) {
			throw se;
		}
		catch (Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}
	}

	public final int getScanIsolationLevel() {
		return scanIsolationLevel;
	}

	/*
	** VTIEnvironment
	*/
	public final boolean isCompileTime() {
		return false;
	}

	public final String getOriginalSQL() {
		return activation.getPreparedStatement().getSource();
	}

	public final int getStatementIsolationLevel() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
		return TransactionControl.jdbcIsolationLevel( getScanIsolationLevel() );
	}


	public final void setSharedState(String key, java.io.Serializable value) {
		if (key == null)
			return;

		if (value == null)
			compileTimeConstants.remove(key);
		else
			compileTimeConstants.put(key, value);


	}

	public Object getSharedState(String key) {
		if ((key == null) || (compileTimeConstants == null))
			return null;

		return compileTimeConstants.get(key);
	}

    /**
     * <p>
     * Get the types of the columns returned by a Derby-style table function.
     * </p>
     */
    private DataTypeDescriptor[]    getReturnColumnTypes()
        throws StandardException
    {
        if ( returnColumnTypes == null )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-3616
            TypeDescriptor[] columnTypes = returnType.getRowTypes();
            int                         count = columnTypes.length;

            returnColumnTypes = new DataTypeDescriptor[ count ];
            for ( int i = 0; i < count; i++ )
            {
                returnColumnTypes[ i ] = DataTypeDescriptor.getType( columnTypes[ i ] );
            }
        }

        return returnColumnTypes;
    }

    /**
     * <p>
     * Cast the value coming out of the user-coded ResultSet. The
     * rules are described in CastNode.getDataValueConversion().
     * </p>
     */
    private void    cast( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        TypeId      typeID = dtd.getTypeId();

        if ( !typeID.isBlobTypeId() && !typeID.isClobTypeId() )
        {
            if ( typeID.isLongVarcharTypeId() ) { castLongvarchar( dtd, dvd ); }
            else if ( typeID.isLongVarbinaryTypeId() ) { castLongvarbinary( dtd, dvd ); }
//IC see: https://issues.apache.org/jira/browse/DERBY-3536
            else if ( typeID.isDecimalTypeId() ) { castDecimal( dtd, dvd ); }
            else
            {
                Object      o = dvd.getObject();

                dvd.setObjectForCast( o, true, typeID.getCorrespondingJavaTypeName() );

                if ( typeID.variableLength() )
                {
                    VariableSizeDataValue   vsdv = (VariableSizeDataValue) dvd;
                    int                                 width;
                    if ( typeID.isNumericTypeId() ) { width = dtd.getPrecision(); }
                    else { width = dtd.getMaximumWidth(); }
            
                    vsdv.setWidth( width, dtd.getScale(), false );
                }
            }

        }

    }

    /**
     * <p>
     * Truncate long varchars to the legal maximum.
     * </p>
     */
    private void    castLongvarchar( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        if ( dvd.getLength() > TypeId.LONGVARCHAR_MAXWIDTH )
        {
            dvd.setValue( dvd.getString().substring( 0, TypeId.LONGVARCHAR_MAXWIDTH ) );
        }
    }
    
    /**
     * <p>
     * Truncate long varbinary values to the legal maximum.
     * </p>
     */
    private void    castLongvarbinary( DataTypeDescriptor dtd, DataValueDescriptor dvd )
        throws StandardException
    {
        if ( dvd.getLength() > TypeId.LONGVARBIT_MAXWIDTH )
        {
            byte[]  original = dvd.getBytes();
            byte[]  result = new byte[ TypeId.LONGVARBIT_MAXWIDTH ];

            System.arraycopy( original, 0, result, 0, TypeId.LONGVARBIT_MAXWIDTH );
            
            dvd.setValue( result );
        }
    }
    
    /**
     * <p>
     * Set the correct precision and scale for a decimal value.
     * </p>
     */
    private void    castDecimal( DataTypeDescriptor dtd, DataValueDescriptor dvd )
//IC see: https://issues.apache.org/jira/browse/DERBY-3536
        throws StandardException
    {
        VariableSizeDataValue   vsdv = (VariableSizeDataValue) dvd;
            
        vsdv.setWidth( dtd.getPrecision(), dtd.getScale(), false );
    }
    
    public Element toXML( Element parentNode, String tag ) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
        Element myNode = super.toXML( parentNode, tag );
        myNode.setAttribute( "javaClassName", javaClassName );
        
        return myNode;
    }
}
