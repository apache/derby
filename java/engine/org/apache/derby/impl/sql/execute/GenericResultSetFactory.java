/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericResultSetFactory

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

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.execute.ResultSetFactory;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import java.util.Properties;
/**
 * ResultSetFactory provides a wrapper around all of
 * the result sets used in this execution implementation.
 * This removes the need of generated classes to do a new
 * and of the generator to know about all of the result
 * sets.  Both simply know about this interface to getting
 * them.
 * <p>
 * In terms of modularizing, we can create just an interface
 * to this class and invoke the interface.  Different implementations
 * would get the same information provided but could potentially
 * massage/ignore it in different ways to satisfy their
 * implementations.  The practicality of this is to be seen.
 * <p>
 * The cost of this type of factory is that once you touch it,
 * you touch *all* of the possible result sets, not just
 * the ones you need.  So the first time you touch it could
 * be painful ... that might be a problem for execution.
 *
 * @author ames
 */
public class GenericResultSetFactory implements ResultSetFactory 
{
	//
	// ResultSetFactory interface
	//
	public GenericResultSetFactory()
	{
	}

	/**
		@see ResultSetFactory#getInsertResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getInsertResultSet(NoPutResultSet source, 
										GeneratedMethod checkGM,
										Activation activation)
		throws StandardException
	{
		getAuthorizer(activation).authorize(Authorizer.SQL_WRITE_OP);
		return new InsertResultSet(source, checkGM, activation );
	}

	/**
		@see ResultSetFactory#getInsertVTIResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getInsertVTIResultSet(NoPutResultSet source, 
										NoPutResultSet vtiRS,
										Activation activation)
		throws StandardException
	{

		getAuthorizer(activation).authorize(Authorizer.SQL_WRITE_OP);
		return new InsertVTIResultSet(source, vtiRS, activation );
	}

	/**
		@see ResultSetFactory#getDeleteVTIResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getDeleteVTIResultSet(NoPutResultSet source, 
										Activation activation)
		throws StandardException
	{
		getAuthorizer(activation).authorize(Authorizer.SQL_WRITE_OP);
		return new DeleteVTIResultSet(source, activation);
	}

	/**
		@see ResultSetFactory#getDeleteResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getDeleteResultSet(NoPutResultSet source,
										Activation activation)
			throws StandardException
	{
		getAuthorizer(activation).authorize(Authorizer.SQL_WRITE_OP);
		return new DeleteResultSet(source, activation );
	}


	/**
		@see ResultSetFactory#getDeleteCascadeResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getDeleteCascadeResultSet(NoPutResultSet source, 
											   Activation activation,	
											   int constantActionItem,
											   ResultSet[] dependentResultSets,
											   String resultSetId)
		throws StandardException
	{
			
		getAuthorizer(activation).authorize(Authorizer.SQL_WRITE_OP);
		return new DeleteCascadeResultSet(source, activation, 
										  constantActionItem,
										  dependentResultSets, 
										  resultSetId);
	}



	/**
		@see ResultSetFactory#getUpdateResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getUpdateResultSet(NoPutResultSet source,
										GeneratedMethod checkGM,
										Activation activation)
			throws StandardException
	{
		//The stress test failed with null pointer exception in here once and then
		//it didn't happen again. It can be a jit problem because after this null
		//pointer exception, the cleanup code in UpdateResultSet got a null
		//pointer exception too which can't happen since the cleanup code checks
		//for null value before doing anything.
		//In any case, if this ever happens again, hopefully the following
		//assertion code will catch it.
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(getAuthorizer(activation) != null, "Authorizer is null");
		}
		getAuthorizer(activation).authorize(Authorizer.SQL_WRITE_OP);
		return new UpdateResultSet(source, checkGM, activation);
	}

	/**
		@see ResultSetFactory#getUpdateVTIResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getUpdateVTIResultSet(NoPutResultSet source,
                                           Activation activation)
			throws StandardException
	{
		getAuthorizer(activation).authorize(Authorizer.SQL_WRITE_OP);
		return new UpdateVTIResultSet(source, activation);
	}



	/**
		@see ResultSetFactory#getDeleteCascadeUpdateResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getDeleteCascadeUpdateResultSet(NoPutResultSet source,
													 GeneratedMethod checkGM,
													 Activation activation,
													 int constantActionItem,
													 int rsdItem)
			throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(getAuthorizer(activation) != null, "Authorizer is null");
		}
		getAuthorizer(activation).authorize(Authorizer.SQL_WRITE_OP);
		return new UpdateResultSet(source, checkGM, activation,
								   constantActionItem, rsdItem);
	}


	/**
		@see ResultSetFactory#getCallStatementResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getCallStatementResultSet(GeneratedMethod methodCall,
				Activation activation)
			throws StandardException
	{
		getAuthorizer(activation).authorize(Authorizer.SQL_CALL_OP);
		return new CallStatementResultSet(methodCall, activation);
	}

	/**
		@see ResultSetFactory#getProjectRestrictResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getProjectRestrictResultSet(NoPutResultSet source,
		Activation activation, GeneratedMethod restriction, 
		GeneratedMethod projection, int resultSetNumber,
		GeneratedMethod constantRestriction,
		int mapRefItem,
		boolean reuseResult,
		boolean doesProjection,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new ProjectRestrictResultSet(source, activation, 
			restriction, projection, resultSetNumber, 
			constantRestriction, mapRefItem, 
			reuseResult,
			doesProjection,
		    optimizerEstimatedRowCount,
			optimizerEstimatedCost,
			closeCleanup);
	}

	/**
		@see ResultSetFactory#getHashTableResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getHashTableResultSet(NoPutResultSet source,
		Activation activation, GeneratedMethod singleTableRestriction, 
		Qualifier[][] equijoinQualifiers,
		GeneratedMethod projection, int resultSetNumber,
		int mapRefItem,
		boolean reuseResult,
		int keyColItem,
		boolean removeDuplicates,
		long maxInMemoryRowCount,
		int	initialCapacity,
		float loadFactor,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new HashTableResultSet(source, activation, 
			singleTableRestriction, 
            equijoinQualifiers,
			projection, resultSetNumber, 
			mapRefItem, 
			reuseResult,
			keyColItem, removeDuplicates,
			maxInMemoryRowCount,
			initialCapacity,
			loadFactor,
			true,		// Skip rows with 1 or more null key columns
		    optimizerEstimatedRowCount,
			optimizerEstimatedCost,
			closeCleanup);
	}

	/**
		@see ResultSetFactory#getSortResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getSortResultSet(NoPutResultSet source,
		boolean distinct, 
		boolean isInSortedOrder,
		int orderItem,
		Activation activation, 
		GeneratedMethod rowAllocator, 
		int maxRowSize,
		int resultSetNumber, 
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new SortResultSet(source, 
			distinct, 
			isInSortedOrder,
			orderItem,
			activation, 
			rowAllocator, 
			maxRowSize,
			resultSetNumber, 
		    optimizerEstimatedRowCount,
			optimizerEstimatedCost,
			closeCleanup);
	}

	/**
		@see ResultSetFactory#getScalarAggregateResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getScalarAggregateResultSet(NoPutResultSet source,
		boolean isInSortedOrder,
		int aggregateItem,
		int orderItem,
		Activation activation, 
		GeneratedMethod rowAllocator, 
		int maxRowSize,
		int resultSetNumber, 
		boolean singleInputRow,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup) 
			throws StandardException
	{
		return new ScalarAggregateResultSet(
						source, isInSortedOrder, aggregateItem, activation,
						rowAllocator, resultSetNumber, singleInputRow,
						optimizerEstimatedRowCount,
						optimizerEstimatedCost, closeCleanup);
	}

	/**
		@see ResultSetFactory#getDistinctScalarAggregateResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getDistinctScalarAggregateResultSet(NoPutResultSet source,
		boolean isInSortedOrder,
		int aggregateItem,
		int orderItem,
		Activation activation, 
		GeneratedMethod rowAllocator, 
		int maxRowSize,
		int resultSetNumber, 
		boolean singleInputRow,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup) 
			throws StandardException
	{
		return new DistinctScalarAggregateResultSet(
						source, isInSortedOrder, aggregateItem, orderItem, activation,
						rowAllocator, maxRowSize, resultSetNumber, singleInputRow,
						optimizerEstimatedRowCount,
						optimizerEstimatedCost, closeCleanup);
	}

	/**
		@see ResultSetFactory#getGroupedAggregateResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getGroupedAggregateResultSet(NoPutResultSet source,
		boolean isInSortedOrder,
		int aggregateItem,
		int orderItem,
		Activation activation, 
		GeneratedMethod rowAllocator, 
		int maxRowSize,
		int resultSetNumber, 
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup) 
			throws StandardException
	{
		return new GroupedAggregateResultSet(
						source, isInSortedOrder, aggregateItem, orderItem, activation,
						rowAllocator, maxRowSize, resultSetNumber, optimizerEstimatedRowCount,
						optimizerEstimatedCost, closeCleanup);
	}

	/**
		@see ResultSetFactory#getDistinctGroupedAggregateResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getDistinctGroupedAggregateResultSet(NoPutResultSet source,
		boolean isInSortedOrder,
		int aggregateItem,
		int orderItem,
		Activation activation, 
		GeneratedMethod rowAllocator, 
		int maxRowSize,
		int resultSetNumber, 
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup) 
			throws StandardException
	{
		return new DistinctGroupedAggregateResultSet(
						source, isInSortedOrder, aggregateItem, orderItem, activation,
						rowAllocator, maxRowSize, resultSetNumber, optimizerEstimatedRowCount,
						optimizerEstimatedCost, closeCleanup);
	}
											

	/**
		@see ResultSetFactory#getAnyResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getAnyResultSet(NoPutResultSet source,
		Activation activation, GeneratedMethod emptyRowFun, int resultSetNumber,
		int subqueryNumber, int pointOfAttachment,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost)
			throws StandardException
	{
		return new AnyResultSet(source,
					 activation, emptyRowFun, resultSetNumber,
					 subqueryNumber, pointOfAttachment,
					 optimizerEstimatedRowCount,
					 optimizerEstimatedCost);
	}

	/**
		@see ResultSetFactory#getOnceResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getOnceResultSet(NoPutResultSet source,
		Activation activation, GeneratedMethod emptyRowFun,
		int cardinalityCheck, int resultSetNumber,
		int subqueryNumber, int pointOfAttachment,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost)
			throws StandardException
	{
		return new OnceResultSet(source,
					 activation, emptyRowFun, 
					 cardinalityCheck, resultSetNumber,
					 subqueryNumber, pointOfAttachment,
				     optimizerEstimatedRowCount,
					 optimizerEstimatedCost);
	}

	/**
		@see ResultSetFactory#getRowResultSet
	 */
	public NoPutResultSet getRowResultSet(Activation activation, GeneratedMethod row,
									 boolean canCacheRow,
									 int resultSetNumber,
									 double optimizerEstimatedRowCount,
									 double optimizerEstimatedCost,
								     GeneratedMethod closeCleanup)
	{
		return new RowResultSet(activation, row, canCacheRow, resultSetNumber, 
							    optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								closeCleanup);
	}

	/**
		@see ResultSetFactory#getVTIResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getVTIResultSet(Activation activation, GeneratedMethod row,
									 int resultSetNumber,
									 GeneratedMethod constructor,
									 String javaClassName,
									 Qualifier[][] pushedQualifiers,
									 int erdNumber,
									 boolean version2,
									 boolean reuseablePs,
									 int ctcNumber,
									 boolean isTarget,
									 int scanIsolationLevel,
									 double optimizerEstimatedRowCount,
									 double optimizerEstimatedCost,
								     GeneratedMethod closeCleanup)
		throws StandardException
	{
		return new VTIResultSet(activation, row, resultSetNumber, 
								constructor,
								javaClassName,
								pushedQualifiers,
								erdNumber,
								version2, reuseablePs,
								ctcNumber,
								isTarget,
								scanIsolationLevel,
							    optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								closeCleanup);
	}

	/**
    	a hash scan generator, for ease of use at present.
		@see ResultSetFactory#getHashScanResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getHashScanResultSet(
									long conglomId,
									int scociItem,
									Activation activation,
									GeneratedMethod resultRowAllocator,
									int resultSetNumber,
									GeneratedMethod startKeyGetter,
									int startSearchOperator,
									GeneratedMethod stopKeyGetter,
									int stopSearchOperator,
									boolean sameStartStopPosition,
									Qualifier[][] scanQualifiers,
									Qualifier[][] nextQualifiers,
									int initialCapacity,
									float loadFactor,
									int maxCapacity,
									int hashKeyColumn,
									String tableName,
									String indexName,
									boolean isConstraint,
									boolean forUpdate,
									int colRefItem,
									int indexColItem,
									int lockMode,
									boolean tableLocked,
									int isolationLevel,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost,
									GeneratedMethod closeCleanup)
			throws StandardException
	{
        StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
						getSavedObject(scociItem));

		return new HashScanResultSet(
								conglomId,
								scoci,
								activation,
								resultRowAllocator,
								resultSetNumber,
								startKeyGetter,
								startSearchOperator,
								stopKeyGetter,
								stopSearchOperator,
								sameStartStopPosition,
								scanQualifiers,
								nextQualifiers,
								initialCapacity,
								loadFactor,
								maxCapacity,
								hashKeyColumn,
								tableName,
								indexName,
								isConstraint,
								forUpdate,
								colRefItem,
								lockMode,
								tableLocked,
								isolationLevel,
								true,		// Skip rows with 1 or more null key columns
								optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								closeCleanup);
	}

	/**
    	a distinct scan generator, for ease of use at present.
		@see ResultSetFactory#getHashScanResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getDistinctScanResultSet(
									long conglomId,
									int scociItem,
									Activation activation,
									GeneratedMethod resultRowAllocator,
									int resultSetNumber,
									int hashKeyColumn,
									String tableName,
									String indexName,
									boolean isConstraint,
									int colRefItem,
									int lockMode,
									boolean tableLocked,
									int isolationLevel,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost,
									GeneratedMethod closeCleanup)
			throws StandardException
	{
        StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
						getSavedObject(scociItem));
		return new DistinctScanResultSet(
								conglomId,
								scoci,
								activation,
								resultRowAllocator,
								resultSetNumber,
								hashKeyColumn,
								tableName,
								indexName,
								isConstraint,
								colRefItem,
								lockMode,
								tableLocked,
								isolationLevel,
								optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								closeCleanup);
	}

	/**
    	a minimal table scan generator, for ease of use at present.
		@see ResultSetFactory#getTableScanResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getTableScanResultSet(
									long conglomId,
									int scociItem,
									Activation activation,
									GeneratedMethod resultRowAllocator,
									int resultSetNumber,
									GeneratedMethod startKeyGetter,
									int startSearchOperator,
									GeneratedMethod stopKeyGetter,
									int stopSearchOperator,
									boolean sameStartStopPosition,
									Qualifier[][] qualifiers,
									String tableName,
									String indexName,
									boolean isConstraint,
									boolean forUpdate,
									int colRefItem,
									int indexColItem,
									int lockMode,
									boolean tableLocked,
									int isolationLevel,
									boolean oneRowScan,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost,
									GeneratedMethod closeCleanup)
			throws StandardException
	{
        StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
						getSavedObject(scociItem));
		return new TableScanResultSet(
								conglomId,
								scoci,
								activation,
								resultRowAllocator,
								resultSetNumber,
								startKeyGetter,
								startSearchOperator,
								stopKeyGetter,
								stopSearchOperator,
								sameStartStopPosition,
								qualifiers,
								tableName,
								indexName,
								isConstraint,
								forUpdate,
								colRefItem,
								indexColItem,
								lockMode,
								tableLocked,
								isolationLevel,
								1,	// rowsPerRead is 1 if not a bulkTableScan
								oneRowScan,
								optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								closeCleanup);
	}

	/**
    	Table/Index scan where rows are read in bulk
		@see ResultSetFactory#getBulkTableScanResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getBulkTableScanResultSet(
									long conglomId,
									int scociItem,
									Activation activation,
									GeneratedMethod resultRowAllocator,
									int resultSetNumber,
									GeneratedMethod startKeyGetter,
									int startSearchOperator,
									GeneratedMethod stopKeyGetter,
									int stopSearchOperator,
									boolean sameStartStopPosition,
									Qualifier[][] qualifiers,
									String tableName,
									String indexName,
									boolean isConstraint,
									boolean forUpdate,
									int colRefItem,
									int indexColItem,
									int lockMode,
									boolean tableLocked,
									int isolationLevel,
									int rowsPerRead,
									boolean oneRowScan,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost,
									GeneratedMethod closeCleanup)
			throws StandardException
	{
		//Prior to Cloudscape 10.0 release, holdability was false by default. Programmers had to explicitly
		//set the holdability to true using JDBC apis. Since holdability was not true by default, we chose to disable the
		//prefetching for RR and Serializable when holdability was explicitly set to true. 
		//But starting Cloudscape 10.0 release, in order to be DB2 compatible, holdability is set to true by default.
		//Because of that, we can not continue to disable the prefetching for RR and Serializable, since it causes
		//severe performance degradation - bug 5953.    

        StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
						getSavedObject(scociItem));
		return new BulkTableScanResultSet(
								conglomId,
								scoci,
								activation,
								resultRowAllocator,
								resultSetNumber,
								startKeyGetter,
								startSearchOperator,
								stopKeyGetter,
								stopSearchOperator,
								sameStartStopPosition,
								qualifiers,
								tableName,
								indexName,
								isConstraint,
								forUpdate,
								colRefItem,
								indexColItem,
								lockMode,
								tableLocked,
								isolationLevel,
								rowsPerRead,
								oneRowScan,
								optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								closeCleanup);
	}

	/**
		@see ResultSetFactory#getIndexRowToBaseRowResultSet
		@exception StandardException	Thrown on error
	 */
	public NoPutResultSet getIndexRowToBaseRowResultSet(
								long conglomId,
								int scociItem,
								Activation a,
								NoPutResultSet source,
								GeneratedMethod resultRowAllocator,
								int resultSetNumber,
								String indexName,
								int heapColRefItem,
								int indexColRefItem,
								int indexColMapItem,
								GeneratedMethod restriction,
								boolean forUpdate,
								double optimizerEstimatedRowCount,
								double optimizerEstimatedCost,
								GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new IndexRowToBaseRowResultSet(
								conglomId,
								scociItem,
								a,
								source,
								resultRowAllocator,
								resultSetNumber,
								indexName,
								heapColRefItem,
								indexColRefItem,
								indexColMapItem,
								restriction,
								forUpdate,
							    optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								closeCleanup);
	}

	/**
		@see ResultSetFactory#getNestedLoopJoinResultSet
		@exception StandardException thrown on error
	 */

    public NoPutResultSet getNestedLoopJoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod joinClause,
								   int resultSetNumber,
								   boolean oneRowRightSide,
								   boolean notExistsRightSide,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new NestedLoopJoinResultSet(leftResultSet, leftNumCols,
										   rightResultSet, rightNumCols,
										   activation, joinClause,
										   resultSetNumber, 
										   oneRowRightSide, 
										   notExistsRightSide, 
										   optimizerEstimatedRowCount,
										   optimizerEstimatedCost,
										   closeCleanup);
	}

	/**
		@see ResultSetFactory#getHashJoinResultSet
		@exception StandardException thrown on error
	 */

    public NoPutResultSet getHashJoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod joinClause,
								   int resultSetNumber,
								   boolean oneRowRightSide,
								   boolean notExistsRightSide,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new HashJoinResultSet(leftResultSet, leftNumCols,
										   rightResultSet, rightNumCols,
										   activation, joinClause,
										   resultSetNumber, 
										   oneRowRightSide, 
										   notExistsRightSide, 
										   optimizerEstimatedRowCount,
										   optimizerEstimatedCost,
										   closeCleanup);
	}

	/**
		@see ResultSetFactory#getNestedLoopLeftOuterJoinResultSet
		@exception StandardException thrown on error
	 */

    public NoPutResultSet getNestedLoopLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod joinClause,
								   int resultSetNumber,
								   GeneratedMethod emptyRowFun,
								   boolean wasRightOuterJoin,
								   boolean oneRowRightSide,
								   boolean notExistsRightSide,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new NestedLoopLeftOuterJoinResultSet(leftResultSet, leftNumCols,
										   rightResultSet, rightNumCols,
										   activation, joinClause,
										   resultSetNumber, 
										   emptyRowFun, 
										   wasRightOuterJoin,
										   oneRowRightSide,
										   notExistsRightSide,
										   optimizerEstimatedRowCount,
										   optimizerEstimatedCost,
										   closeCleanup);
	}

	/**
		@see ResultSetFactory#getHashLeftOuterJoinResultSet
		@exception StandardException thrown on error
	 */

    public NoPutResultSet getHashLeftOuterJoinResultSet(NoPutResultSet leftResultSet,
								   int leftNumCols,
								   NoPutResultSet rightResultSet,
								   int rightNumCols,
								   Activation activation,
								   GeneratedMethod joinClause,
								   int resultSetNumber,
								   GeneratedMethod emptyRowFun,
								   boolean wasRightOuterJoin,
								   boolean oneRowRightSide,
								   boolean notExistsRightSide,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new HashLeftOuterJoinResultSet(leftResultSet, leftNumCols,
										   rightResultSet, rightNumCols,
										   activation, joinClause,
										   resultSetNumber, 
										   emptyRowFun, 
										   wasRightOuterJoin,
										   oneRowRightSide,
										   notExistsRightSide,
										   optimizerEstimatedRowCount,
										   optimizerEstimatedCost,
										   closeCleanup);
	}

	/**
		@see ResultSetFactory#getSetTransactionResultSet
		@exception StandardException thrown when unable to create the
			result set
	 */
	public ResultSet getSetTransactionResultSet(Activation activation) 
		throws StandardException
	{
		getAuthorizer(activation).authorize(Authorizer.SQL_ARBITARY_OP);		
		return new SetTransactionResultSet(activation);
	}

	/**
		@see ResultSetFactory#getMaterializedResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getMaterializedResultSet(NoPutResultSet source,
							Activation activation, int resultSetNumber,
						    double optimizerEstimatedRowCount,
							double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
		throws StandardException
	{
		return new MaterializedResultSet(source, activation, 
									  resultSetNumber, 
									  optimizerEstimatedRowCount,
									  optimizerEstimatedCost,
									  closeCleanup);
	}

	/**
		@see ResultSetFactory#getScrollInsensitiveResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getScrollInsensitiveResultSet(NoPutResultSet source,
							Activation activation, int resultSetNumber,
							int sourceRowWidth,
							boolean scrollable,
						    double optimizerEstimatedRowCount,
							double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
		throws StandardException
	{
		/* ResultSet tree is dependent on whether or not this is
		 * for a scroll insensitive cursor.
		 */

		if (scrollable)
		{
			return new ScrollInsensitiveResultSet(source, activation, 
									  resultSetNumber, 
									  sourceRowWidth,
									  optimizerEstimatedRowCount,
									  optimizerEstimatedCost,
									  closeCleanup);
		}
		else
		{
			return source;
		}
	}

	/**
		@see ResultSetFactory#getNormalizeResultSet
		@exception StandardException thrown on error
	 */
	public NoPutResultSet getNormalizeResultSet(NoPutResultSet source,
							Activation activation, int resultSetNumber, 
							int erdNumber,
						    double optimizerEstimatedRowCount,
							double optimizerEstimatedCost,
							boolean forUpdate,					
							GeneratedMethod closeCleanup)
		throws StandardException
	{
		return new NormalizeResultSet(source, activation, 
									  resultSetNumber, erdNumber, 
									  optimizerEstimatedRowCount,
									  optimizerEstimatedCost, forUpdate,
									  closeCleanup);
	}

	/**
		@see ResultSetFactory#getCurrentOfResultSet
	 */
	public NoPutResultSet getCurrentOfResultSet(String cursorName, 
	    Activation activation, int resultSetNumber, String psName)
	{
		return new CurrentOfResultSet(cursorName, activation, resultSetNumber, psName);
	}

	/**
		@see ResultSetFactory#getDDLResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getDDLResultSet(Activation activation)
					throws StandardException
	{
		getAuthorizer(activation).authorize(Authorizer.SQL_DDL_OP);
		return getMiscResultSet( activation);
	}

	/**
		@see ResultSetFactory#getMiscResultSet
		@exception StandardException thrown on error
	 */
	public ResultSet getMiscResultSet(Activation activation)
					throws StandardException
	{
		getAuthorizer(activation).authorize(Authorizer.SQL_ARBITARY_OP);
		return new MiscResultSet(activation);
	}

	/**
    	a minimal union scan generator, for ease of use at present.
		@see ResultSetFactory#getUnionResultSet
		@exception StandardException thrown on error
	 */
    public NoPutResultSet getUnionResultSet(NoPutResultSet leftResultSet,
								   NoPutResultSet rightResultSet,
								   Activation activation,
								   int resultSetNumber,
								   double optimizerEstimatedRowCount,
								   double optimizerEstimatedCost,
								   GeneratedMethod closeCleanup)
			throws StandardException
	{
		return new UnionResultSet(leftResultSet, rightResultSet, 
								  activation,
								  resultSetNumber, 
								  optimizerEstimatedRowCount,
								  optimizerEstimatedCost,
								  closeCleanup);
	}

	/**
	 * A last index key sresult set returns the last row from
	 * the index in question.  It is used as an ajunct to max().
	 *
	 * @param activation 		the activation for this result set,
	 *		which provides the context for the row allocation operation.
	 * @param resultSetNumber	The resultSetNumber for the ResultSet
	 * @param resultRowAllocator a reference to a method in the activation
	 * 						that creates a holder for the result row of the scan.  May
	 *						be a partial row.  <verbatim>
	 *		ExecRow rowAllocator() throws StandardException; </verbatim>
	 * @param conglomId 		the conglomerate of the table to be scanned.
	 * @param tableName			The full name of the table
	 * @param indexName			The name of the index, if one used to access table.
	 * @param colRefItem		An saved item for a bitSet of columns that
	 *							are referenced in the underlying table.  -1 if
	 *							no item.
	 * @param lockMode			The lock granularity to use (see
	 *							TransactionController in access)
	 * @param tableLocked		Whether or not the table is marked as using table locking
	 *							(in sys.systables)
	 * @param isolationLevel	Isolation level (specified or not) to use on scans
	 * @param optimizerEstimatedRowCount	Estimated total # of rows by
	 * 										optimizer
	 * @param optimizerEstimatedCost		Estimated total cost by optimizer
	 * @param closeCleanup		any cleanup the activation needs to do on close.
	 *
	 * @return the scan operation as a result set.
 	 *
	 * @exception StandardException thrown when unable to create the
	 * 				result set
	 */
	public NoPutResultSet getLastIndexKeyResultSet
	(
		Activation 			activation,
		int 				resultSetNumber,
		GeneratedMethod 	resultRowAllocator,
		long 				conglomId,
		String 				tableName,
		String 				indexName,
		int 				colRefItem,
		int 				lockMode,
		boolean				tableLocked,
		int					isolationLevel,
		double				optimizerEstimatedRowCount,
		double 				optimizerEstimatedCost,
		GeneratedMethod 	closeCleanup
	) throws StandardException
	{
		return new LastIndexKeyResultSet(
					activation,
					resultSetNumber,
					resultRowAllocator,
					conglomId,
					tableName,
					indexName,
					colRefItem,
					lockMode,
					tableLocked,
					isolationLevel,
					optimizerEstimatedRowCount,
					optimizerEstimatedCost,
					closeCleanup);
	}



	/**
	 *	a referential action dependent table scan generator.
	 *  @see ResultSetFactory#getTableScanResultSet
	 *	@exception StandardException thrown on error
	 */
	public NoPutResultSet getRaDependentTableScanResultSet(
									long conglomId,
									int scociItem,
									Activation activation,
									GeneratedMethod resultRowAllocator,
									int resultSetNumber,
									GeneratedMethod startKeyGetter,
									int startSearchOperator,
									GeneratedMethod stopKeyGetter,
									int stopSearchOperator,
									boolean sameStartStopPosition,
									Qualifier[][] qualifiers,
									String tableName,
									String indexName,
									boolean isConstraint,
									boolean forUpdate,
									int colRefItem,
									int indexColItem,
									int lockMode,
									boolean tableLocked,
									int isolationLevel,
									boolean oneRowScan,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost,
									GeneratedMethod closeCleanup,
									String parentResultSetId,
									long fkIndexConglomId,
									int fkColArrayItem,
									int rltItem)
			throws StandardException
	{
        StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
						getSavedObject(scociItem));
		return new DependentResultSet(
								conglomId,
								scoci,
								activation,
								resultRowAllocator,
								resultSetNumber,
								startKeyGetter,
								startSearchOperator,
								stopKeyGetter,
								stopSearchOperator,
								sameStartStopPosition,
								qualifiers,
								tableName,
								indexName,
								isConstraint,
								forUpdate,
								colRefItem,
								lockMode,
								tableLocked,
								isolationLevel,
								1,
								oneRowScan,
								optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								closeCleanup,
								parentResultSetId,
								fkIndexConglomId,
								fkColArrayItem,
								rltItem);
	}
	
	static private Authorizer getAuthorizer(Activation activation)
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		return lcc.getAuthorizer();
	}


   /////////////////////////////////////////////////////////////////
   //
   //	PUBLIC MINIONS
   //
   /////////////////////////////////////////////////////////////////

}
