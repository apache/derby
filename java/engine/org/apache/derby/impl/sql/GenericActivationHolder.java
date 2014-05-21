/*

   Derby - Class org.apache.derby.impl.sql.GenericActivationHolder

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

package org.apache.derby.impl.sql;

import java.sql.SQLWarning;
import java.util.Enumeration;
import java.util.Vector;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Row;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.SQLSessionContext;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.TemporaryRowHolder;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.BaseActivation;

/**
 * This class holds an Activation, and passes through most of the calls
 * to the activation.  The purpose of this class is to allow a PreparedStatement
 * to be recompiled without the caller having to detect this and get a new
 * activation.
 *
 * In addition to the Activation, this class holds a reference to the
 * PreparedStatement that created it, along with a reference to the
 * GeneratedClass that was associated with the PreparedStatement at the time
 * this holder was created.  These references are used to validate the
 * Activation, to ensure that an activation is used only with the
 * PreparedStatement that created it, and to detect when recompilation has
 * happened.
 *
 * We detect recompilation by checking whether the GeneratedClass has changed.
 * If it has, we try to let the caller continue to use this ActivationHolder.
 * We create a new instance of the new GeneratedClass (that is, we create a
 * new Activation), and we compare the number and type of parameters.  If these
 * are compatible, we copy the parameters from the old to the new Activation.
 * If they are not compatible, we throw an exception telling the user that
 * the Activation is out of date, and they need to get a new one.
 *
 */

final public class GenericActivationHolder implements Activation
{
	public BaseActivation			ac;
	ExecPreparedStatement	ps;
	GeneratedClass			gc;
	DataTypeDescriptor[]	paramTypes;
	private final LanguageConnectionContext lcc;
	/**
	 * Constructor for an ActivationHolder
	 *
	 * @param gc	The GeneratedClass of the Activation
	 * @param ps	The PreparedStatement this ActivationHolder is associated
	 *				with
	 *
	 * @exception StandardException		Thrown on error
	 */
	GenericActivationHolder(LanguageConnectionContext lcc, GeneratedClass gc, ExecPreparedStatement ps, boolean scrollable)
			throws StandardException
	{
		this.lcc = lcc;
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(gc != null, "generated class is null , ps is a " + ps.getClass());
		}

		this.gc = gc;
		this.ps = ps;

		ac = (BaseActivation) gc.newInstance(lcc);
		ac.setupActivation(ps, scrollable);
		paramTypes = ps.getParameterTypes();
	}

	/* Activation interface */

	/**
	 * @see Activation#reset
	 *
	 * @exception StandardException thrown on failure
	 */
	public void	reset() throws StandardException
	{
		ac.reset();
	}

	/**
	 * Temporary tables can be declared with ON COMMIT DELETE ROWS. But if the table has a held curosr open at
	 * commit time, data should not be deleted from the table. This method, (gets called at commit time) checks if this
	 * activation held cursor and if so, does that cursor reference the passed temp table name.
	 *
	 * @return	true if this activation has held cursor and if it references the passed temp table name
	 */
	public boolean checkIfThisActivationHasHoldCursor(String tableName)
	{
		return ac.checkIfThisActivationHasHoldCursor(tableName);
	}

	/**
	 * @see Activation#setCursorName
	 *
	 */
	public void	setCursorName(String cursorName)
	{
		ac.setCursorName(cursorName);
	}

	/**
	 * @see Activation#getCursorName
	 */
	public String	getCursorName()
	{
		return ac.getCursorName();
	}

	/**
	 * @see Activation#setResultSetHoldability
	 *
	 */
	public void	setResultSetHoldability(boolean resultSetHoldability)
	{
		ac.setResultSetHoldability(resultSetHoldability);
	}

	/**
	 * @see Activation#getResultSetHoldability
	 */
	public boolean	getResultSetHoldability()
	{
		return ac.getResultSetHoldability();
	}

	/** @see Activation#setAutoGeneratedKeysResultsetInfo */
	public void setAutoGeneratedKeysResultsetInfo(int[] columnIndexes, String[] columnNames)
	{
		ac.setAutoGeneratedKeysResultsetInfo(columnIndexes, columnNames);
	}

	/** @see Activation#getAutoGeneratedKeysResultsetMode */
	public boolean getAutoGeneratedKeysResultsetMode()
	{
		return ac.getAutoGeneratedKeysResultsetMode();
	}

	/** @see Activation#getAutoGeneratedKeysColumnIndexes */
	public int[] getAutoGeneratedKeysColumnIndexes()
	{
		return ac.getAutoGeneratedKeysColumnIndexes();
	}

	/** @see Activation#getAutoGeneratedKeysColumnNames */
	public String[] getAutoGeneratedKeysColumnNames()
	{
		return ac.getAutoGeneratedKeysColumnNames();
	}

	/** @see org.apache.derby.iapi.sql.Activation#getLanguageConnectionContext */
	public	LanguageConnectionContext	getLanguageConnectionContext()
	{
		return	lcc;
	}

	public TransactionController getTransactionController()
	{
		return ac.getTransactionController();
	}

	/** @see Activation#getExecutionFactory */
	public	ExecutionFactory	getExecutionFactory()
	{
		return	ac.getExecutionFactory();
	}

	/**
	 * @see Activation#getParameterValueSet
	 */
	public ParameterValueSet	getParameterValueSet()
	{
		return ac.getParameterValueSet();
	}

	/**
	 * @see Activation#setParameters
	 */
	public void	setParameters(ParameterValueSet parameterValues, DataTypeDescriptor[] parameterTypes) throws StandardException
	{
		ac.setParameters(parameterValues, parameterTypes);
	}

	/** 
	 * @see Activation#execute
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ResultSet execute() throws StandardException
	{
		/*
		** Synchronize to avoid problems if another thread is preparing
		** the statement at the same time we're trying to execute it.
		*/
		// synchronized (ps)
		{
			/* Has the activation class changed or has the activation been
			 * invalidated? */
            final boolean needNewClass =
                    gc == null || gc != ps.getActivationClass();
			if (needNewClass || !ac.isValid())
			{

                GeneratedClass newGC;

				if (needNewClass) {
                    // The statement has been re-prepared since the last time
                    // we executed it. Get the new activation class.
                    newGC = ps.getActivationClass();
                    if (newGC == null) {
                        // There is no class associated with the statement.
                        // Tell the caller that the statement needs to be
                        // recompiled.
                        throw StandardException.newException(
                                SQLState.LANG_STATEMENT_NEEDS_RECOMPILE);
                    }
				} else {
					// Reuse the generated class, we just want a new activation
					// since the old is no longer valid.
					newGC = gc;
				}


				/*
				** If we get here, it means the Activation has been invalidated
				** or the PreparedStatement has been recompiled.  Get a new
				** Activation and check whether the parameters are compatible.
				** If so, transfer the parameters from the old Activation to
				** the new one, and make that the current Activation.  If not,
				** throw an exception.
				*/
				BaseActivation		newAC = (BaseActivation) newGC.newInstance(lcc);

				DataTypeDescriptor[]	newParamTypes = ps.getParameterTypes();

				/*
				** Link the new activation to the prepared statement.
				*/
				newAC.setupActivation(ps, ac.getScrollable());

				newAC.setParameters(ac.getParameterValueSet(), paramTypes);


				/*
				** IMPORTANT
				**
				** Copy any essential state from the old activation
				** to the new activation. This must match the state
				** setup in EmbedStatement.
				** singleExecution, cursorName, holdability, maxRows.
				*/

				if (ac.isSingleExecution())
					newAC.setSingleExecution();

				newAC.setCursorName(ac.getCursorName());

				newAC.setResultSetHoldability(ac.getResultSetHoldability());
				if (ac.getAutoGeneratedKeysResultsetMode()) //Need to do copy only if auto generated mode is on
                {
					newAC.setAutoGeneratedKeysResultsetInfo(ac.getAutoGeneratedKeysColumnIndexes(),
					ac.getAutoGeneratedKeysColumnNames());
                }
				newAC.setMaxRows(ac.getMaxRows());

				// break the link with the prepared statement
				ac.setupActivation(null, false);
				ac.close();

				/* Remember the new class information */
				ac = newAC;
				gc = newGC;
				paramTypes = newParamTypes;
			}
		}

		String cursorName = ac.getCursorName();
		if (cursorName != null)
		{
			// have to see if another activation is open
			// with the same cursor name. If so we can't use this name

			Activation activeCursor = lcc.lookupCursorActivation(cursorName);

			if ((activeCursor != null) && (activeCursor != ac)) {
				throw StandardException.newException(SQLState.LANG_CURSOR_ALREADY_EXISTS, cursorName);
			}
		}

		return ac.execute();
	}

	/**
	 * @see Activation#getResultSet
	 *
	 * @return the current ResultSet of this activation.
	 */
	public ResultSet getResultSet()
	{
		return ac.getResultSet();
	}

	/**
	 * @see Activation#setCurrentRow
	 *
	 */
	public void setCurrentRow(ExecRow currentRow, int resultSetNumber) 
	{
		ac.setCurrentRow(currentRow, resultSetNumber);
	}

	/**
	 * @see Activation#getCurrentRow
	 *
	 */
	public Row getCurrentRow(int resultSetNumber)
    {
        return ac.getCurrentRow( resultSetNumber );
    }
    
	/**
	 * @see Activation#clearCurrentRow
	 */
	public void clearCurrentRow(int resultSetNumber) 
	{
		ac.clearCurrentRow(resultSetNumber);
	}

	/**
	 * @see Activation#getPreparedStatement
	 */
	public ExecPreparedStatement getPreparedStatement()
	{
		return ps;
	}

	public void checkStatementValidity() throws StandardException {
		ac.checkStatementValidity();
	}

	/**
	 * @see Activation#getResultDescription
	 */
	public ResultDescription getResultDescription()
	{
		return ac.getResultDescription();
	}

	/**
	 * @see Activation#getDataValueFactory
	 */
	public DataValueFactory getDataValueFactory()
	{
		return ac.getDataValueFactory();
	}

	/**
	 * @see Activation#getRowLocationTemplate
	 */
	public RowLocation getRowLocationTemplate(int itemNumber)
	{
		return ac.getRowLocationTemplate(itemNumber);
	}

	/**
	 * @see Activation#getHeapConglomerateController
	 */
	public ConglomerateController getHeapConglomerateController()
	{
		return ac.getHeapConglomerateController();
	}

	/**
	 * @see Activation#setHeapConglomerateController
	 */
	public void setHeapConglomerateController(ConglomerateController updateHeapCC)
	{
		ac.setHeapConglomerateController(updateHeapCC);
	}

	/**
	 * @see Activation#clearHeapConglomerateController
	 */
	public void clearHeapConglomerateController()
	{
		ac.clearHeapConglomerateController();
	}

	/**
	 * @see Activation#getIndexScanController
	 */
	public ScanController getIndexScanController()
	{
		return ac.getIndexScanController();
	}

	/**
	 * @see Activation#setIndexScanController
	 */
	public void setIndexScanController(ScanController indexSC)
	{
		ac.setIndexScanController(indexSC);
	}

	/**
	 * @see Activation#getIndexConglomerateNumber
	 */
	public long getIndexConglomerateNumber()
	{
		return ac.getIndexConglomerateNumber();
	}

	/**
	 * @see Activation#setIndexConglomerateNumber
	 */
	public void setIndexConglomerateNumber(long indexConglomerateNumber)
	{
		ac.setIndexConglomerateNumber(indexConglomerateNumber);
	}

	/**
	 * @see Activation#clearIndexScanInfo
	 */
	public void clearIndexScanInfo()
	{
		ac.clearIndexScanInfo();
	}

	/**
	 * @see Activation#close
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void close() throws StandardException
	{
		ac.close();
	}

	/**
	 * @see Activation#isClosed
	 */
	public boolean isClosed()
	{
		return ac.isClosed();
	}

	/**
		Set the activation for a single execution.

		@see Activation#setSingleExecution
	*/
	public void setSingleExecution() {
		ac.setSingleExecution();
	}

	/**
		Is the activation set up for a single execution.

		@see Activation#isSingleExecution
	*/
	public boolean isSingleExecution() {
		return ac.isSingleExecution();
	}

	/**
		Get the number of subqueries in the entire query.
		@return int	 The number of subqueries in the entire query.
	 */
	public int getNumSubqueries() {
		return ac.getNumSubqueries();
	}

	/**
	 * @see Activation#setForCreateTable()
	 */
	public void setForCreateTable()
	{
		ac.setForCreateTable();
	}

	/**
	 * @see Activation#getForCreateTable()
	 */
	public boolean getForCreateTable()
	{
		return ac.getForCreateTable();
	}

	/**
	 * @see Activation#setDDLTableDescriptor
	 */
	public void setDDLTableDescriptor(TableDescriptor td)
	{
		ac.setDDLTableDescriptor(td);
	}

	/**
	 * @see Activation#getDDLTableDescriptor
	 */
	public TableDescriptor getDDLTableDescriptor()
	{
		return ac.getDDLTableDescriptor();
	}

	/**
	 * @see Activation#setMaxRows
	 */
	public void setMaxRows(long maxRows)
	{
		ac.setMaxRows(maxRows);
	}

	/**
	 * @see Activation#getMaxRows
	 */
	public long getMaxRows()
	{
		return ac.getMaxRows();
	}

	public void setTargetVTI(java.sql.ResultSet targetVTI)
	{
		ac.setTargetVTI(targetVTI);
	}

	public java.sql.ResultSet getTargetVTI()
	{
		return ac.getTargetVTI();
	}

	public SQLSessionContext getSQLSessionContextForChildren() {
		return ac.getSQLSessionContextForChildren();
    }

	public SQLSessionContext setupSQLSessionContextForChildren(boolean push) {
		return ac.setupSQLSessionContextForChildren(push);
    }

	public void setParentActivation(Activation a) {
		ac.setParentActivation(a);
	}

	public Activation getParentActivation() {
		return ac.getParentActivation();
	}


	/* Dependable interface implementation */

	/**
	 * @see Dependable#getDependableFinder
	 */
	public DependableFinder getDependableFinder()
	{
		// Vacuous implementation to make class concrete, only needed for
		// BaseActivation
		if (SanityManager.DEBUG) {
			SanityManager.NOTREACHED();
		}

		return null;
	}


	/**
	 * @see Dependable#getObjectName
	 */
	public String getObjectName()
	{
		// Vacuous implementation to make class concrete, only needed for
		// BaseActivation
		if (SanityManager.DEBUG) {
			SanityManager.NOTREACHED();
		}

		return null;
	}



	/**
	 * @see Dependable#getObjectID
	 */
	public UUID getObjectID()
	{
		// Vacuous implementation to make class concrete, only needed for
		// BaseActivation
		if (SanityManager.DEBUG) {
			SanityManager.NOTREACHED();
		}

		return null;
	}


	/**
	 * @see Dependable#getClassType
	 */
	public String getClassType()
	{
		// Vacuous implementation to make class concrete, only needed for
		// BaseActivation
		if (SanityManager.DEBUG) {
			SanityManager.NOTREACHED();
		}

		return null;
	}


	/**
	 * @see Dependable#isPersistent
	 */
	public boolean isPersistent()
	{
		// Vacuous implementation to make class concrete, only needed for
		// BaseActivation
		if (SanityManager.DEBUG) {
			SanityManager.NOTREACHED();
		}

		return false;
	}


	/* Dependent interface implementation */

	/**
	 * @see org.apache.derby.iapi.sql.depend.Dependent#isValid
	 */
	public boolean isValid() {
		// Vacuous implementation to make class concrete, only needed for
		// BaseActivation
		if (SanityManager.DEBUG) {
			SanityManager.NOTREACHED();
		}

		return false;
	}

	/**
	 * @see org.apache.derby.iapi.sql.depend.Dependent#makeInvalid
	 */
	public void makeInvalid(int action,
							LanguageConnectionContext lcc)
			throws StandardException {
		// Vacuous implementation to make class concrete, only needed for
		// BaseActivation
		if (SanityManager.DEBUG) {
			SanityManager.NOTREACHED();
		}
	}

	/**
	 * @see org.apache.derby.iapi.sql.depend.Dependent#prepareToInvalidate
	 */
	public void prepareToInvalidate(Provider p, int action,
							 LanguageConnectionContext lcc)
			throws StandardException {
		// Vacuous implementation to make class concrete, only needed for
		// BaseActivation
		if (SanityManager.DEBUG) {
			SanityManager.NOTREACHED();
		}
	}


	/* Class implementation */

	/**
	 * Mark the activation as unused.  
	 */
	public void markUnused()
	{
		ac.markUnused();
	}

	/**
	 * Is the activation in use?
	 *
	 * @return true/false
	 */
	public boolean isInUse()
	{
		return ac.isInUse();
	}
	/**
	  @see org.apache.derby.iapi.sql.Activation#addWarning
	  */
	public void addWarning(SQLWarning w)
	{
		ac.addWarning(w);
	}

	/**
	  @see org.apache.derby.iapi.sql.Activation#getWarnings
	  */
	public SQLWarning getWarnings()
	{
		return ac.getWarnings();
	}

	/**
	  @see org.apache.derby.iapi.sql.Activation#clearWarnings
	  */
	public void clearWarnings()
	{
		ac.clearWarnings();
	}

	/**
		@see Activation#informOfRowCount
		@exception StandardException	Thrown on error
	 */
	public void informOfRowCount(NoPutResultSet resultSet, long rowCount)
					throws StandardException
	{
		ac.informOfRowCount(resultSet, rowCount);
	}

	/**
	 * @see Activation#isCursorActivation
	 */
	public boolean isCursorActivation()
	{
		return ac.isCursorActivation();
	}

    public  ConstantAction    pushConstantAction( ConstantAction newConstantAction )
    {
        return ac.pushConstantAction( newConstantAction );
    }

    public  ConstantAction    popConstantAction()
    {
        return ac.popConstantAction();
    }

	public ConstantAction getConstantAction() {
		return ac.getConstantAction();
	}

	public void setParentResultSet(TemporaryRowHolder rs, String resultSetId)
	{
		ac.setParentResultSet(rs, resultSetId);
	}


    @SuppressWarnings("UseOfObsoleteCollectionType")
    public Vector<TemporaryRowHolder> getParentResultSet(String resultSetId)
	{
		return ac.getParentResultSet(resultSetId);
	}

	public void clearParentResultSets()
	{
		ac.clearParentResultSets();
	}

    public Enumeration<String> getParentResultSetKeys()
	{
		return ac.getParentResultSetKeys();
	}

	public void setForUpdateIndexScan(CursorResultSet forUpdateResultSet)
	{
		ac.setForUpdateIndexScan(forUpdateResultSet);
	}

	public CursorResultSet getForUpdateIndexScan()
	{
		return ac.getForUpdateIndexScan();
	}

	public java.sql.ResultSet[][] getDynamicResults() {
		return ac.getDynamicResults();
	}
	public int getMaxDynamicResults() {
		return ac.getMaxDynamicResults();
	}

	public NumberDataValue getCurrentValueAndAdvance
        ( String sequenceUUIDstring, int typeFormatID )
        throws StandardException
    {
        return ac.getCurrentValueAndAdvance( sequenceUUIDstring, typeFormatID );
    }

}
