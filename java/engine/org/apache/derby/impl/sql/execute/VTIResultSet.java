/*

   Derby - Class org.apache.derby.impl.sql.execute.VTIResultSet

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableHashtable;

import org.apache.derby.vti.DeferModification;
import org.apache.derby.vti.IFastPath;
import org.apache.derby.vti.VTIEnvironment;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;


/**
 */
public class VTIResultSet extends NoPutResultSetImpl
	implements CursorResultSet, VTIEnvironment {

	/* Run time statistics variables */
	public int rowsReturned;
	public String javaClassName;

    private boolean next;
	private ClassInspector classInspector;
    private GeneratedMethod row;
    private GeneratedMethod constructor;
    protected GeneratedMethod closeCleanup;
	private PreparedStatement userPS;
	private ResultSet userVTI;
	private ExecRow allocatedRow;
	private FormatableBitSet referencedColumns;
	private boolean version2;
	private boolean reuseablePs;
	private boolean isTarget;
	private FormatableHashtable compileTimeConstants;
	private int ctcNumber;

	private boolean pushedProjection;
	private IFastPath	fastPath;

	private Qualifier[][]	pushedQualifiers;

	private boolean[] runtimeNullableColumn;

	/**
		Specified isolation level of SELECT (scan). If not set or
		not application, it will be set to ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL
	*/
	private int scanIsolationLevel = ExecutionContext.UNSPECIFIED_ISOLATION_LEVEL;

    //
    // class interface
    //
    VTIResultSet(Activation activation, GeneratedMethod row, int resultSetNumber,
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
				 GeneratedMethod closeCleanup) 
		throws StandardException
	{
		super(activation, resultSetNumber, 
			  optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.row = row;
		this.constructor = constructor;
		this.javaClassName = javaClassName;
		this.version2 = version2;
		this.reuseablePs = reuseablePs;
		this.isTarget = isTarget;
		this.pushedQualifiers = pushedQualifiers;
		this.scanIsolationLevel = scanIsolationLevel;

		if (erdNumber != -1)
		{
			this.referencedColumns = (FormatableBitSet)(activation.getPreparedStatement().
								getSavedObject(erdNumber));
		}

		this.ctcNumber = ctcNumber;
		compileTimeConstants = (FormatableHashtable) (activation.getPreparedStatement().
								getSavedObject(ctcNumber));

		this.closeCleanup = closeCleanup;
		constructorTime += getElapsedMillis(beginTime);
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

	private boolean[] setNullableColumnList() throws SQLException {

		if (runtimeNullableColumn != null)
			return runtimeNullableColumn;

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
                            fastPath.currentRow(userVTI, result.getRowArray());
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
			if (closeCleanup != null) {
				closeCleanup.invoke(activation); // let activation tidy up
			}

			// we don't want to keep around a pointer to the
			// row ... so it can be thrown away.
			// REVISIT: does this need to be in a finally
			// block, to ensure that it is executed?
	    	clearCurrentRow();
	    	next = false;

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
		if (allocatedRow == null)
		{
			allocatedRow = (ExecRow) row.invoke(activation);
		}

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
		return ExecutionContext.CS_TO_JDBC_ISOLATION_LEVEL_MAP[getScanIsolationLevel()];
	}


	public final void setSharedState(String key, java.io.Serializable value) {
		if (key == null)
			return;

		if (compileTimeConstants == null) {

			Object[] savedObjects = activation.getPreparedStatement().getSavedObjects();

			synchronized (savedObjects) {

				compileTimeConstants = (FormatableHashtable) savedObjects[ctcNumber];
				if (compileTimeConstants == null) {
					compileTimeConstants = new FormatableHashtable();
					savedObjects[ctcNumber] = compileTimeConstants;
				}
			}
		}

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
}
