/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericExecutionFactory

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.impl.sql.GenericResultDescription;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ResultSetFactory;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;
import org.apache.derby.iapi.sql.execute.ScanQualifier;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.jdbc.ConnectionContext;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.io.FormatableProperties;
import org.apache.derby.catalog.TypeDescriptor;
import java.util.Properties;
import java.util.Vector;

/**
	This Factory is for creating the execution items needed
	by a connection for a given database.  Once created for
	the connection, they should be pushed onto the execution context
	so that they can be found again by subsequent actions during the session.

	@author ames
 */
public class GenericExecutionFactory
	implements ModuleControl, ModuleSupportable, ExecutionFactory {

	//
	// ModuleControl interface
	//
	public boolean canSupport(Properties startParams)
	{
		return	Monitor.isDesiredType( startParams, org.apache.derby.iapi.reference.EngineType.NONE);
	}

	/**
		This Factory is expected to be booted relative to a
		LanguageConnectionFactory.

		@see org.apache.derby.iapi.sql.conn.LanguageConnectionFactory
	 * @exception StandardException Thrown on error
	 */
	public void boot(boolean create, Properties startParams)
		throws StandardException
	{
		// do we need to/ is there some way to check that
		// we are configured per database?

		/* Creation of the connection execution factories 
		 * for this database deferred until needed to reduce
		 * boot time.
		 */

		// REMIND: removed boot of LanguageFactory because
		// that is done in BasicDatabase.
	}

	public void stop() {
	}

	//
	// ExecutionFactory interface
	//
	/**
	 * Factories are generic and can be used by all connections.
	 * We defer instantiation until needed to reduce boot time.
	 * We may instantiate too many instances in rare multi-user
	 * situation, but consistency will be maintained and at some
	 * point, usually always, we will have 1 and only 1 instance
	 * of each factory because assignment is atomic.
	 */
	public ResultSetFactory getResultSetFactory() 
	{
		if (rsFactory == null)
		{
			rsFactory = new GenericResultSetFactory();
		}
		return rsFactory;
	}

	/**
	  *	Get the factory for constant actions.
	  *
	  *	@return	the factory for constant actions.
	  */
	public	GenericConstantActionFactory	getConstantActionFactory() 
	{ 
		if (genericConstantActionFactory == null)
		{
			genericConstantActionFactory = new GenericConstantActionFactory();
		}
		return genericConstantActionFactory; 
	}

	/**
		We want a dependency context so that we can push it onto
		the stack.  We could instead require the implementation
		push it onto the stack for us, but this way we know
		which context object exactly was pushed onto the stack.
	 */
	public ExecutionContext newExecutionContext(ContextManager cm)
	{
		/* Pass in nulls for execution factories.  GEC
		 * will call back to get factories when needed.
		 * This allows us to reduce boot time class loading.
		 * (Replication currently instantiates factories
		 * at boot time.)
		 */
		return new GenericExecutionContext(
							(ResultSetFactory) null,
							cm, this);
	}

	/*
	 * @see ExecutionFactory#getScanQualifier
	 */
	public ScanQualifier[][] getScanQualifier(int numQualifiers)
	{
		ScanQualifier[] sqArray = new GenericScanQualifier[numQualifiers];

		for (int ictr = 0; ictr < numQualifiers; ictr++)
		{
			sqArray[ictr] = new GenericScanQualifier();
		}

        ScanQualifier[][] ret_sqArray = { sqArray };

		return(ret_sqArray);
	}

	/**
		Make a result description
	 */
	public ResultDescription getResultDescription(
		ResultColumnDescriptor[] columns, String statementType) {
		return new GenericResultDescription(columns, statementType);
	}

	/**
	 * Create an execution time ResultColumnDescriptor from a 
	 * compile time RCD.
	 *
	 * @param compileRCD	The compile time RCD.
	 *
	 * @return The execution time ResultColumnDescriptor
	 */
	public ResultColumnDescriptor getResultColumnDescriptor(ResultColumnDescriptor compileRCD)
	{
		return new GenericColumnDescriptor(compileRCD);
	}

	/**
	 * @see ExecutionFactory#releaseScanQualifier
	 */
	public void releaseScanQualifier(ScanQualifier[][] qualifiers)
	{
	}

	/**
	 * @see ExecutionFactory#getQualifier
	 */
	public Qualifier getQualifier(
							int columnId,
							int operator,
							GeneratedMethod orderableGetter,
							Activation activation,
							boolean orderedNulls,
							boolean unknownRV,
							boolean negateCompareResult,
							int variantType)
	{
		return new GenericQualifier(columnId, operator, orderableGetter,
									activation, orderedNulls, unknownRV,
									negateCompareResult, variantType);
	}

	/**
	  @exception StandardException		Thrown on error
	  @see ExecutionFactory#getRowChanger
	  */
	public RowChanger
	getRowChanger(long heapConglom,
				  StaticCompiledOpenConglomInfo heapSCOCI,
				  DynamicCompiledOpenConglomInfo heapDCOCI,
				  IndexRowGenerator[] irgs,
				  long[] indexCIDS,
				  StaticCompiledOpenConglomInfo[] indexSCOCIs,
				  DynamicCompiledOpenConglomInfo[] indexDCOCIs,
				  int numberOfColumns,
				  TransactionController tc,
				  int[] changedColumnIds,
				  int[] streamStorableHeapColIds,
				  Activation activation) throws StandardException
	{
		return new RowChangerImpl( heapConglom, 
								   heapSCOCI, heapDCOCI, 
								   irgs, indexCIDS, indexSCOCIs, indexDCOCIs,
								   numberOfColumns, 
								   changedColumnIds, tc, null,
								   streamStorableHeapColIds, activation );
	}

	/**
	  @exception StandardException		Thrown on error
	  @see ExecutionFactory#getRowChanger
	  */
	public RowChanger getRowChanger(
			   long heapConglom,
			   StaticCompiledOpenConglomInfo heapSCOCI,
			   DynamicCompiledOpenConglomInfo heapDCOCI,
			   IndexRowGenerator[] irgs,
			   long[] indexCIDS,
			   StaticCompiledOpenConglomInfo[] indexSCOCIs,
			   DynamicCompiledOpenConglomInfo[] indexDCOCIs,
			   int numberOfColumns,
			   TransactionController tc,
			   int[] changedColumnIds,
			   FormatableBitSet	baseRowReadList,
			   int[] baseRowReadMap,
			   int[] streamStorableColIds,
			   Activation activation
			   )
		 throws StandardException
	{
		return new RowChangerImpl( heapConglom,
								   heapSCOCI, heapDCOCI, 
								   irgs, indexCIDS, indexSCOCIs, indexDCOCIs,
								   numberOfColumns, 
								   changedColumnIds, tc, baseRowReadList,
								   baseRowReadMap, activation );
	}


	/**
	 * Get a trigger execution context
	 *
	 * @exception StandardException		Thrown on error
	 */
	public InternalTriggerExecutionContext getTriggerExecutionContext
	(
		LanguageConnectionContext	lcc,
		ConnectionContext			cc,
		String 						statementText,
		int 						dmlType,
		int[]						changedColIds,
		String[]					changedColNames,
		UUID						targetTableId,
		String						targetTableName,
		Vector						aiCounters
	) throws StandardException
	{
		return new InternalTriggerExecutionContext(lcc, cc,
												   statementText, dmlType,
												   changedColIds,
												   changedColNames,
												   targetTableId,
												   targetTableName, 
												   aiCounters);
	}

	/*
		Old RowFactory interface
	 */

	public ExecRow getValueRow(int numColumns) {
		return new ValueRow(numColumns);
	}

	public ExecIndexRow getIndexableRow(int numColumns) {
		return new IndexRow(numColumns);
	}

	public ExecIndexRow getIndexableRow(ExecRow valueRow) {
		if (valueRow instanceof ExecIndexRow)
			return (ExecIndexRow)valueRow;
		return new IndexValueRow(valueRow);
	}

   /**
	 Packages up a clump of constants which the Plugin uses at execute()
	 time for COPY PUBLICATION.
    */
    public	Object	getJdbcCopyConstants
	(
		int[][]				paramReferences,
		TypeDescriptor[][]	columnTypes,
		int[][]				publishedTableSchemaCounts
    )
	{ return null; }

   /**
	 Packages up a clump of constants which the Plugin uses at execute()
	 time for CREATE PUBLICATION.
    */
    public	Object	getJdbcCreateConstants
	(
		UUID[]				publishedJarFileIDs,
		Object				publishedItems,
		int[][]				tableSchemaCounts
    )
	{ return null; }

	//
	// class interface
	//
	public GenericExecutionFactory() {
	}

	//
	// fields
	//
	public	  ResultSetFactory rsFactory;
    protected GenericConstantActionFactory	genericConstantActionFactory;
}
