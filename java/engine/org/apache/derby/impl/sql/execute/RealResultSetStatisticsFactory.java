/*

   Derby - Class org.apache.derby.impl.sql.execute.RealResultSetStatisticsFactory

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.PreparedStatement;

import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ResultSetFactory;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.impl.sql.execute.AnyResultSet;
import org.apache.derby.impl.sql.execute.CurrentOfResultSet;
import org.apache.derby.impl.sql.execute.DeleteResultSet;
import org.apache.derby.impl.sql.execute.DeleteCascadeResultSet;
import org.apache.derby.impl.sql.execute.DeleteVTIResultSet;
import org.apache.derby.impl.sql.execute.DistinctScalarAggregateResultSet;
import org.apache.derby.impl.sql.execute.DistinctScanResultSet;
import org.apache.derby.impl.sql.execute.GroupedAggregateResultSet;
import org.apache.derby.impl.sql.execute.HashJoinResultSet;
import org.apache.derby.impl.sql.execute.HashLeftOuterJoinResultSet;
import org.apache.derby.impl.sql.execute.HashScanResultSet;
import org.apache.derby.impl.sql.execute.HashTableResultSet;
import org.apache.derby.impl.sql.execute.IndexRowToBaseRowResultSet;
import org.apache.derby.impl.sql.execute.InsertResultSet;
import org.apache.derby.impl.sql.execute.InsertVTIResultSet;
import org.apache.derby.impl.sql.execute.LastIndexKeyResultSet;
import org.apache.derby.impl.sql.execute.MaterializedResultSet;
import org.apache.derby.impl.sql.execute.NestedLoopJoinResultSet;
import org.apache.derby.impl.sql.execute.NestedLoopLeftOuterJoinResultSet;
import org.apache.derby.impl.sql.execute.NormalizeResultSet;
import org.apache.derby.impl.sql.execute.OnceResultSet;
import org.apache.derby.impl.sql.execute.ProjectRestrictResultSet;
import org.apache.derby.impl.sql.execute.RowResultSet;
import org.apache.derby.impl.sql.execute.ScalarAggregateResultSet;
import org.apache.derby.impl.sql.execute.ScrollInsensitiveResultSet;
import org.apache.derby.impl.sql.execute.SortResultSet;
import org.apache.derby.impl.sql.execute.TableScanResultSet;
import org.apache.derby.impl.sql.execute.UnionResultSet;
import org.apache.derby.impl.sql.execute.UpdateResultSet;
import org.apache.derby.impl.sql.execute.VTIResultSet;
import org.apache.derby.impl.sql.execute.DependentResultSet;

import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import org.apache.derby.impl.sql.execute.rts.RealAnyResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealCurrentOfStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDeleteResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDeleteCascadeResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDeleteVTIResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDistinctScalarAggregateStatistics;
import org.apache.derby.impl.sql.execute.rts.RealDistinctScanStatistics;
import org.apache.derby.impl.sql.execute.rts.RealGroupedAggregateStatistics;
import org.apache.derby.impl.sql.execute.rts.RealHashJoinStatistics;
import org.apache.derby.impl.sql.execute.rts.RealHashLeftOuterJoinStatistics;
import org.apache.derby.impl.sql.execute.rts.RealHashScanStatistics;
import org.apache.derby.impl.sql.execute.rts.RealHashTableStatistics;
import org.apache.derby.impl.sql.execute.rts.RealIndexRowToBaseRowStatistics;
import org.apache.derby.impl.sql.execute.rts.RealInsertResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealInsertVTIResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealJoinResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealLastIndexKeyScanStatistics;
import org.apache.derby.impl.sql.execute.rts.RealMaterializedResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealNestedLoopJoinStatistics;
import org.apache.derby.impl.sql.execute.rts.RealNestedLoopLeftOuterJoinStatistics;
import org.apache.derby.impl.sql.execute.rts.RealNormalizeResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealOnceResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealProjectRestrictStatistics;
import org.apache.derby.impl.sql.execute.rts.RealRowResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealScalarAggregateStatistics;
import org.apache.derby.impl.sql.execute.rts.RealScrollInsensitiveResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealSortStatistics;
import org.apache.derby.impl.sql.execute.rts.RealTableScanStatistics;
import org.apache.derby.impl.sql.execute.rts.RealUnionResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealUpdateResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RealVTIStatistics;
import org.apache.derby.impl.sql.execute.rts.ResultSetStatistics;
import org.apache.derby.impl.sql.execute.rts.RunTimeStatisticsImpl;

import org.apache.derby.iapi.reference.SQLState;

import java.util.Properties;

/**
 * ResultSetStatisticsFactory provides a wrapper around all of
 * objects associated with run time statistics.
 * <p>
 * This implementation of the protocol is for returning the "real"
 * run time statistics.  We have modularized this so that we could
 * have an implementation that just returns null for each of the
 * objects should we decided to provide a configuration without
 * the run time statistics feature.
 *
 * @author jerry
 */
public class RealResultSetStatisticsFactory 
		implements ResultSetStatisticsFactory
{

	//
	// ExecutionFactory interface
	//
	//
	// ResultSetStatisticsFactory interface
	//

	/**
		@see ResultSetStatisticsFactory#getRunTimeStatistics
	 */
	public RunTimeStatistics getRunTimeStatistics(
			Activation activation, 
			ResultSet rs,
			NoPutResultSet[] subqueryTrackingArray)
		throws StandardException
	{
		PreparedStatement preStmt = activation.getPreparedStatement();

		// If the prepared statement is null then the result set is being
		// finished as a result of a activation being closed during a recompile.
		// In this case statistics should not be generated.
		if (preStmt == null)
			return null;




		ResultSetStatistics topResultSetStatistics;

		if (rs instanceof NoPutResultSet)
		{
			topResultSetStatistics =
									getResultSetStatistics((NoPutResultSet) rs);
		}
		else
		{
			topResultSetStatistics = getResultSetStatistics(rs);
		}

		/* Build up the info on the materialized subqueries */
		int subqueryTrackingArrayLength =
				(subqueryTrackingArray == null) ? 0 :
					subqueryTrackingArray.length;
		ResultSetStatistics[] subqueryRSS =
				new ResultSetStatistics[subqueryTrackingArrayLength];
		boolean anyAttached = false;
		for (int index = 0; index < subqueryTrackingArrayLength; index++)
		{
			if (subqueryTrackingArray[index] != null &&
				subqueryTrackingArray[index].getPointOfAttachment() == -1)
			{
				subqueryRSS[index] =
						getResultSetStatistics(subqueryTrackingArray[index]);
				anyAttached = true;
			}
		}
		if (anyAttached == false)
		{
			subqueryRSS = null;
		}

		// Get the info on all of the materialized subqueries (attachment point = -1)
		return new RunTimeStatisticsImpl(
								preStmt.getSPSName(),
								activation.getCursorName(),
								preStmt.getSource(),
								preStmt.getCompileTimeInMillis(),
								preStmt.getParseTimeInMillis(),
								preStmt.getBindTimeInMillis(),
								preStmt.getOptimizeTimeInMillis(),
								preStmt.getGenerateTimeInMillis(),
								rs.getExecuteTime(),
								preStmt.getBeginCompileTimestamp(),
								preStmt.getEndCompileTimestamp(),
								rs.getBeginExecutionTimestamp(),
								rs.getEndExecutionTimestamp(),
								subqueryRSS,
								topResultSetStatistics);
	}

	/**
		@see ResultSetStatisticsFactory#getResultSetStatistics
	 */
	public ResultSetStatistics getResultSetStatistics(ResultSet rs)
	{
		if (!rs.returnsRows())
		{
			return getNoRowsResultSetStatistics(rs);
		}
		else if (rs instanceof NoPutResultSet)
		{
			return getResultSetStatistics((NoPutResultSet) rs);
		}
		else
		{
			return null;
		}
	}

	public ResultSetStatistics getNoRowsResultSetStatistics(ResultSet rs)
	{
		ResultSetStatistics retval = null;

		/* We need to differentiate based on instanceof in order
		 * to find the right constructor to call.  This is ugly,
		 * but if we don't do instanceof then rs is always seen as an
		 * interface instead of a class when we try to overload 
		 * a method with both.
		 */
		if( rs instanceof InsertResultSet)
		{
			InsertResultSet irs = (InsertResultSet) rs;

			retval = new RealInsertResultSetStatistics(
									irs.rowCount,
									irs.constants.deferred,
									irs.constants.irgs.length,
									irs.userSpecifiedBulkInsert,
									irs.bulkInsertPerformed,
									irs.constants.lockMode ==
										TransactionController.MODE_TABLE,
									irs.getExecuteTime(), 
									getResultSetStatistics(irs.savedSource)
									);

			irs.savedSource = null;
		}
		else if( rs instanceof InsertVTIResultSet)
		{
			InsertVTIResultSet iVTIrs = (InsertVTIResultSet) rs;

			retval = new RealInsertVTIResultSetStatistics(
									iVTIrs.rowCount,
									iVTIrs.constants.deferred,
									iVTIrs.getExecuteTime(), 
									getResultSetStatistics(iVTIrs.savedSource)
									);

			iVTIrs.savedSource = null;
		}
		else if( rs instanceof UpdateResultSet)
		{
			UpdateResultSet urs = (UpdateResultSet) rs;

			retval = new RealUpdateResultSetStatistics(
									urs.rowCount,
									urs.constants.deferred,
									urs.constants.irgs.length,
									urs.constants.lockMode ==
										TransactionController.MODE_TABLE,
									urs.getExecuteTime(),
									getResultSetStatistics(urs.savedSource)
									);

			urs.savedSource = null;
		}
		else if( rs instanceof DeleteCascadeResultSet)
		{
			DeleteCascadeResultSet dcrs = (DeleteCascadeResultSet) rs;
			int dependentTrackingArrayLength =
				(dcrs.dependentResultSets == null) ? 0 :
					dcrs.dependentResultSets.length;
			ResultSetStatistics[] dependentTrackingArray =
				new ResultSetStatistics[dependentTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < dependentTrackingArrayLength; index++)
			{
				if (dcrs.dependentResultSets[index] != null)
				{
					dependentTrackingArray[index] =
										getResultSetStatistics(
											dcrs.dependentResultSets[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				dependentTrackingArray = null;
			}

			retval = new RealDeleteCascadeResultSetStatistics(
									dcrs.rowCount,
									dcrs.constants.deferred,
									dcrs.constants.irgs.length,
									dcrs.constants.lockMode ==
										TransactionController.MODE_TABLE,
									dcrs.getExecuteTime(),
									getResultSetStatistics(dcrs.savedSource),
									dependentTrackingArray
									);

			dcrs.savedSource = null;
		}
		else if( rs instanceof DeleteResultSet)
		{
			DeleteResultSet drs = (DeleteResultSet) rs;

			retval = new RealDeleteResultSetStatistics(
									drs.rowCount,
									drs.constants.deferred,
									drs.constants.irgs.length,
									drs.constants.lockMode ==
										TransactionController.MODE_TABLE,
									drs.getExecuteTime(),
									getResultSetStatistics(drs.savedSource)
									);

			drs.savedSource = null;
		}
		else if( rs instanceof DeleteVTIResultSet)
		{
			DeleteVTIResultSet dVTIrs = (DeleteVTIResultSet) rs;

			retval = new RealDeleteVTIResultSetStatistics(
									dVTIrs.rowCount,
									dVTIrs.getExecuteTime(), 
									getResultSetStatistics(dVTIrs.savedSource)
									);

			dVTIrs.savedSource = null;
		}


		return retval;
	}

	public ResultSetStatistics getResultSetStatistics(NoPutResultSet rs)
	{
		/* We need to differentiate based on instanceof in order
		 * to find the right constructor to call.  This is ugly,
		 * but if we don't do instanceof then rs is always seen as an
		 * interface instead of a class when we try to overload 
		 * a method with both.
		 */
		if( rs instanceof ProjectRestrictResultSet)
		{
			ProjectRestrictResultSet prrs = (ProjectRestrictResultSet) rs;
			int subqueryTrackingArrayLength =
				(prrs.subqueryTrackingArray == null) ? 0 :
					prrs.subqueryTrackingArray.length;
			ResultSetStatistics[] subqueryTrackingArray =
				new ResultSetStatistics[subqueryTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < subqueryTrackingArrayLength; index++)
			{
				if (prrs.subqueryTrackingArray[index] != null &&
					prrs.subqueryTrackingArray[index].getPointOfAttachment() ==
						prrs.resultSetNumber)
				{
					subqueryTrackingArray[index] =
										getResultSetStatistics(
											prrs.subqueryTrackingArray[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				subqueryTrackingArray = null;
			}

			return new RealProjectRestrictStatistics(
											prrs.numOpens,
											prrs.rowsSeen,
											prrs.rowsFiltered,
											prrs.constructorTime,
											prrs.openTime,
											prrs.nextTime,
											prrs.closeTime,
											prrs.resultSetNumber,
											prrs.restrictionTime,
											prrs.projectionTime,
											subqueryTrackingArray,
											(prrs.restriction != null),
											prrs.doesProjection,
											prrs.optimizerEstimatedRowCount,
											prrs.optimizerEstimatedCost,
											getResultSetStatistics(prrs.source)
											);
		}
		else if (rs instanceof SortResultSet)
		{
			SortResultSet srs = (SortResultSet) rs;

			return new RealSortStatistics(
											srs.numOpens,
											srs.rowsSeen,
											srs.rowsFiltered,
											srs.constructorTime,
											srs.openTime,
											srs.nextTime,
											srs.closeTime,
											srs.resultSetNumber,
											srs.rowsInput,
											srs.rowsReturned,
											srs.distinct,
											srs.isInSortedOrder,
											srs.sortProperties,
											srs.optimizerEstimatedRowCount,
											srs.optimizerEstimatedCost,
											getResultSetStatistics(srs.source)
										);
		}
		else if (rs instanceof DistinctScalarAggregateResultSet)
		{
			DistinctScalarAggregateResultSet dsars = (DistinctScalarAggregateResultSet) rs;

			return new RealDistinctScalarAggregateStatistics(
											dsars.numOpens,
											dsars.rowsSeen,
											dsars.rowsFiltered,
											dsars.constructorTime,
											dsars.openTime,
											dsars.nextTime,
											dsars.closeTime,
											dsars.resultSetNumber,
											dsars.rowsInput,
											dsars.optimizerEstimatedRowCount,
											dsars.optimizerEstimatedCost,
											getResultSetStatistics(dsars.source)
										);
		}
		else if (rs instanceof ScalarAggregateResultSet)
		{
			ScalarAggregateResultSet sars = (ScalarAggregateResultSet) rs;

			return new RealScalarAggregateStatistics(
											sars.numOpens,
											sars.rowsSeen,
											sars.rowsFiltered,
											sars.constructorTime,
											sars.openTime,
											sars.nextTime,
											sars.closeTime,
											sars.resultSetNumber,
											sars.singleInputRow,
											sars.rowsInput,
											sars.optimizerEstimatedRowCount,
											sars.optimizerEstimatedCost,
											getResultSetStatistics(sars.source)
										);
		}
		else if (rs instanceof GroupedAggregateResultSet)
		{
			GroupedAggregateResultSet gars = (GroupedAggregateResultSet) rs;

			return new RealGroupedAggregateStatistics(
											gars.numOpens,
											gars.rowsSeen,
											gars.rowsFiltered,
											gars.constructorTime,
											gars.openTime,
											gars.nextTime,
											gars.closeTime,
											gars.resultSetNumber,
											gars.rowsInput,
											gars.hasDistinctAggregate,
											gars.isInSortedOrder,
											gars.sortProperties,
											gars.optimizerEstimatedRowCount,
											gars.optimizerEstimatedCost,
											getResultSetStatistics(gars.source)
										);
		}
		else if (rs instanceof TableScanResultSet)
		{
			boolean instantaneousLocks = false;
			TableScanResultSet tsrs = (TableScanResultSet) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;
			String lockRequestString = null;

			switch (tsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_READ_UNCOMMITTED);
					break;
			}

			if (tsrs.forUpdate)
			{
				lockString = MessageService.getTextMessage(
												SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (tsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_ROW);
					break;
			}

			if (tsrs.indexName != null)
			{
				/* Start and stop position strings will be non-null
			 	* if the TSRS has been closed.  Otherwise, we go off
			 	* and build the strings now.
			 	*/
				startPosition = tsrs.startPositionString;
				if (startPosition == null)
				{
					startPosition = tsrs.printStartPosition();
				}
				stopPosition = tsrs.stopPositionString;
				if (stopPosition == null)
				{
					stopPosition = tsrs.printStopPosition();
				}
			}

			return new 
                RealTableScanStatistics(
                    tsrs.numOpens,
                    tsrs.rowsSeen,
                    tsrs.rowsFiltered,
                    tsrs.constructorTime,
                    tsrs.openTime,
                    tsrs.nextTime,
                    tsrs.closeTime,
                    tsrs.resultSetNumber,
                    tsrs.tableName,
                    tsrs.indexName,
                    tsrs.isConstraint,
                    tsrs.printQualifiers(tsrs.qualifiers),
                    tsrs.getScanProperties(),
                    startPosition,
                    stopPosition,
                    isolationLevel,
                    lockRequestString,
                    tsrs.rowsPerRead,
                    tsrs.coarserLock,
                    tsrs.optimizerEstimatedRowCount,
                    tsrs.optimizerEstimatedCost);
		}

		else if (rs instanceof LastIndexKeyResultSet )
		{
			LastIndexKeyResultSet lrs = (LastIndexKeyResultSet) rs;
			String isolationLevel =  null;
			String lockRequestString = null;

			switch (lrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_UNCOMMITTED);
                    break;
			}

			switch (lrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					lockRequestString = MessageService.getTextMessage(
													SQLState.LANG_SHARE_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					lockRequestString = MessageService.getTextMessage(
													SQLState.LANG_SHARE_ROW);
					break;
			}

			return new RealLastIndexKeyScanStatistics(
											lrs.numOpens,
											lrs.constructorTime,
											lrs.openTime,
											lrs.nextTime,
											lrs.closeTime,
											lrs.resultSetNumber,
											lrs.tableName,
											lrs.indexName,
											isolationLevel,
											lockRequestString,
											lrs.optimizerEstimatedRowCount,
											lrs.optimizerEstimatedCost);
		}
		else if (rs instanceof HashLeftOuterJoinResultSet)
		{
			HashLeftOuterJoinResultSet hlojrs =
				(HashLeftOuterJoinResultSet) rs;

			return new RealHashLeftOuterJoinStatistics(
											hlojrs.numOpens,
											hlojrs.rowsSeen,
											hlojrs.rowsFiltered,
											hlojrs.constructorTime,
											hlojrs.openTime,
											hlojrs.nextTime,
											hlojrs.closeTime,
											hlojrs.resultSetNumber,
											hlojrs.rowsSeenLeft,
											hlojrs.rowsSeenRight,
											hlojrs.rowsReturned,
											hlojrs.restrictionTime,
											hlojrs.optimizerEstimatedRowCount,
											hlojrs.optimizerEstimatedCost,
											getResultSetStatistics(
												hlojrs.leftResultSet),
											getResultSetStatistics(
												hlojrs.rightResultSet),
											hlojrs.emptyRightRowsReturned);
		}
		else if (rs instanceof NestedLoopLeftOuterJoinResultSet)
		{
			NestedLoopLeftOuterJoinResultSet nllojrs =
				(NestedLoopLeftOuterJoinResultSet) rs;

			return new RealNestedLoopLeftOuterJoinStatistics(
											nllojrs.numOpens,
											nllojrs.rowsSeen,
											nllojrs.rowsFiltered,
											nllojrs.constructorTime,
											nllojrs.openTime,
											nllojrs.nextTime,
											nllojrs.closeTime,
											nllojrs.resultSetNumber,
											nllojrs.rowsSeenLeft,
											nllojrs.rowsSeenRight,
											nllojrs.rowsReturned,
											nllojrs.restrictionTime,
											nllojrs.optimizerEstimatedRowCount,
											nllojrs.optimizerEstimatedCost,
											getResultSetStatistics(
												nllojrs.leftResultSet),
											getResultSetStatistics(
												nllojrs.rightResultSet),
											nllojrs.emptyRightRowsReturned);
		}
		else if (rs instanceof HashJoinResultSet)
		{
			HashJoinResultSet hjrs = (HashJoinResultSet) rs;

			return new RealHashJoinStatistics(
											hjrs.numOpens,
											hjrs.rowsSeen,
											hjrs.rowsFiltered,
											hjrs.constructorTime,
											hjrs.openTime,
											hjrs.nextTime,
											hjrs.closeTime,
											hjrs.resultSetNumber,
											hjrs.rowsSeenLeft,
											hjrs.rowsSeenRight,
											hjrs.rowsReturned,
											hjrs.restrictionTime,
											hjrs.oneRowRightSide,
											hjrs.optimizerEstimatedRowCount,
											hjrs.optimizerEstimatedCost,
											getResultSetStatistics(
												hjrs.leftResultSet),
											getResultSetStatistics(
												hjrs.rightResultSet)
											);
		}
		else if (rs instanceof NestedLoopJoinResultSet)
		{
			NestedLoopJoinResultSet nljrs = (NestedLoopJoinResultSet) rs;

			return new RealNestedLoopJoinStatistics(
											nljrs.numOpens,
											nljrs.rowsSeen,
											nljrs.rowsFiltered,
											nljrs.constructorTime,
											nljrs.openTime,
											nljrs.nextTime,
											nljrs.closeTime,
											nljrs.resultSetNumber,
											nljrs.rowsSeenLeft,
											nljrs.rowsSeenRight,
											nljrs.rowsReturned,
											nljrs.restrictionTime,
											nljrs.oneRowRightSide,
											nljrs.optimizerEstimatedRowCount,
											nljrs.optimizerEstimatedCost,
											getResultSetStatistics(
												nljrs.leftResultSet),
											getResultSetStatistics(
												nljrs.rightResultSet)
											);
		}
		else if (rs instanceof IndexRowToBaseRowResultSet)
		{
			IndexRowToBaseRowResultSet irtbrrs =
											(IndexRowToBaseRowResultSet) rs;

			return new RealIndexRowToBaseRowStatistics(
											irtbrrs.numOpens,
											irtbrrs.rowsSeen,
											irtbrrs.rowsFiltered,
											irtbrrs.constructorTime,
											irtbrrs.openTime,
											irtbrrs.nextTime,
											irtbrrs.closeTime,
											irtbrrs.resultSetNumber,
											irtbrrs.indexName,
											irtbrrs.accessedHeapCols,
											irtbrrs.optimizerEstimatedRowCount,
											irtbrrs.optimizerEstimatedCost,
											getResultSetStatistics(
																irtbrrs.source)
											);
		}
		else if (rs instanceof RowResultSet)
		{
			RowResultSet rrs = (RowResultSet) rs;

			return new RealRowResultSetStatistics(
											rrs.numOpens,
											rrs.rowsSeen,
											rrs.rowsFiltered,
											rrs.constructorTime,
											rrs.openTime,
											rrs.nextTime,
											rrs.closeTime,
											rrs.resultSetNumber,
											rrs.rowsReturned,
											rrs.optimizerEstimatedRowCount,
											rrs.optimizerEstimatedCost);
		}
		else if (rs instanceof UnionResultSet)
		{
			UnionResultSet urs = (UnionResultSet) rs;

			return new RealUnionResultSetStatistics(
											urs.numOpens,
											urs.rowsSeen,
											urs.rowsFiltered,
											urs.constructorTime,
											urs.openTime,
											urs.nextTime,
											urs.closeTime,
											urs.resultSetNumber,
											urs.rowsSeenLeft,
											urs.rowsSeenRight,
											urs.rowsReturned,
											urs.optimizerEstimatedRowCount,
											urs.optimizerEstimatedCost,
											getResultSetStatistics(urs.source1),
											getResultSetStatistics(urs.source2)
											);
		}
		else if (rs instanceof AnyResultSet)
		{
			AnyResultSet ars = (AnyResultSet) rs;

			return new RealAnyResultSetStatistics(
											ars.numOpens,
											ars.rowsSeen,
											ars.rowsFiltered,
											ars.constructorTime,
											ars.openTime,
											ars.nextTime,
											ars.closeTime,
											ars.resultSetNumber,
											ars.subqueryNumber,
											ars.pointOfAttachment,
											ars.optimizerEstimatedRowCount,
											ars.optimizerEstimatedCost,
											getResultSetStatistics(ars.source)
											);
		}
		else if (rs instanceof OnceResultSet)
		{
			OnceResultSet ors = (OnceResultSet) rs;

			return new RealOnceResultSetStatistics(
											ors.numOpens,
											ors.rowsSeen,
											ors.rowsFiltered,
											ors.constructorTime,
											ors.openTime,
											ors.nextTime,
											ors.closeTime,
											ors.resultSetNumber,
											ors.subqueryNumber,
											ors.pointOfAttachment,
											ors.optimizerEstimatedRowCount,
											ors.optimizerEstimatedCost,
											getResultSetStatistics(ors.source)
											);
		}
		else if (rs instanceof NormalizeResultSet)
		{
			NormalizeResultSet nrs = (NormalizeResultSet) rs;

			return new RealNormalizeResultSetStatistics(
											nrs.numOpens,
											nrs.rowsSeen,
											nrs.rowsFiltered,
											nrs.constructorTime,
											nrs.openTime,
											nrs.nextTime,
											nrs.closeTime,
											nrs.resultSetNumber,
											nrs.optimizerEstimatedRowCount,
											nrs.optimizerEstimatedCost,
											getResultSetStatistics(nrs.source)
											);
		}
		else if (rs instanceof MaterializedResultSet)
		{
			MaterializedResultSet mrs = (MaterializedResultSet) rs;

			return new RealMaterializedResultSetStatistics(
											mrs.numOpens,
											mrs.rowsSeen,
											mrs.rowsFiltered,
											mrs.constructorTime,
											mrs.openTime,
											mrs.nextTime,
											mrs.closeTime,
											mrs.createTCTime,
											mrs.fetchTCTime,
											mrs.resultSetNumber,
											mrs.optimizerEstimatedRowCount,
											mrs.optimizerEstimatedCost,
											getResultSetStatistics(mrs.source)
											);
		}
		else if (rs instanceof ScrollInsensitiveResultSet)
		{
			ScrollInsensitiveResultSet sirs = (ScrollInsensitiveResultSet) rs;

			return new RealScrollInsensitiveResultSetStatistics(
											sirs.numOpens,
											sirs.rowsSeen,
											sirs.rowsFiltered,
											sirs.constructorTime,
											sirs.openTime,
											sirs.nextTime,
											sirs.closeTime,
											sirs.numFromHashTable,
											sirs.numToHashTable,
											sirs.resultSetNumber,
											sirs.optimizerEstimatedRowCount,
											sirs.optimizerEstimatedCost,
											getResultSetStatistics(sirs.source)
											);
		}
		else if (rs instanceof CurrentOfResultSet)
		{
			CurrentOfResultSet cors = (CurrentOfResultSet) rs;

			return new RealCurrentOfStatistics(
											cors.numOpens,
											cors.rowsSeen,
											cors.rowsFiltered,
											cors.constructorTime,
											cors.openTime,
											cors.nextTime,
											cors.closeTime,
											cors.resultSetNumber
											);
		}
		else if (rs instanceof HashScanResultSet)
		{
			boolean instantaneousLocks = false;
			HashScanResultSet hsrs = (HashScanResultSet) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;

			switch (hsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

			}

			if (hsrs.forUpdate)
			{
				lockString = MessageService.getTextMessage(
													SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (hsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockString = lockString + " " +
									MessageService.getTextMessage(
														SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockString = lockString + " " +
									MessageService.getTextMessage(
															SQLState.LANG_ROW);
					break;
			}

			if (hsrs.indexName != null)
			{
				/* Start and stop position strings will be non-null
			 	* if the HSRS has been closed.  Otherwise, we go off
			 	* and build the strings now.
			 	*/
				startPosition = hsrs.startPositionString;
				if (startPosition == null)
				{
					startPosition = hsrs.printStartPosition();
				}
				stopPosition = hsrs.stopPositionString;
				if (stopPosition == null)
				{
					stopPosition = hsrs.printStopPosition();
				}
			}

			// DistinctScanResultSet is simple sub-class of
			// HashScanResultSet
			if (rs instanceof DistinctScanResultSet)
			{
				return new RealDistinctScanStatistics(
											hsrs.numOpens,
											hsrs.rowsSeen,
											hsrs.rowsFiltered,
											hsrs.constructorTime,
											hsrs.openTime,
											hsrs.nextTime,
											hsrs.closeTime,
											hsrs.resultSetNumber,
											hsrs.tableName,
											hsrs.indexName,
											hsrs.isConstraint,
											hsrs.hashtableSize,
											hsrs.keyColumns,
											hsrs.printQualifiers(
												hsrs.scanQualifiers),
											hsrs.printQualifiers(
												hsrs.nextQualifiers),
											hsrs.getScanProperties(),
											startPosition,
											stopPosition,
											isolationLevel,
											lockString,
											hsrs.optimizerEstimatedRowCount,
											hsrs.optimizerEstimatedCost
											);
			}
			else
			{
				return new RealHashScanStatistics(
											hsrs.numOpens,
											hsrs.rowsSeen,
											hsrs.rowsFiltered,
											hsrs.constructorTime,
											hsrs.openTime,
											hsrs.nextTime,
											hsrs.closeTime,
											hsrs.resultSetNumber,
											hsrs.tableName,
											hsrs.indexName,
											hsrs.isConstraint,
											hsrs.hashtableSize,
											hsrs.keyColumns,
											hsrs.printQualifiers(
												hsrs.scanQualifiers),
											hsrs.printQualifiers(
												hsrs.nextQualifiers),
											hsrs.getScanProperties(),
											startPosition,
											stopPosition,
											isolationLevel,
											lockString,
											hsrs.optimizerEstimatedRowCount,
											hsrs.optimizerEstimatedCost
											);
			}
		}
		else if (rs instanceof HashTableResultSet)
		{
			HashTableResultSet htrs = (HashTableResultSet) rs;
			int subqueryTrackingArrayLength =
				(htrs.subqueryTrackingArray == null) ? 0 :
					htrs.subqueryTrackingArray.length;
			ResultSetStatistics[] subqueryTrackingArray =
				new ResultSetStatistics[subqueryTrackingArrayLength];
			boolean anyAttached = false;
			for (int index = 0; index < subqueryTrackingArrayLength; index++)
			{
				if (htrs.subqueryTrackingArray[index] != null &&
					htrs.subqueryTrackingArray[index].getPointOfAttachment() ==
						htrs.resultSetNumber)
				{
					subqueryTrackingArray[index] =
										getResultSetStatistics(
											htrs.subqueryTrackingArray[index]);
					anyAttached = true;
				}
			}
			if (! anyAttached)
			{
				subqueryTrackingArray = null;
			}

			return new 
                RealHashTableStatistics(
                    htrs.numOpens,
                    htrs.rowsSeen,
                    htrs.rowsFiltered,
                    htrs.constructorTime,
                    htrs.openTime,
                    htrs.nextTime,
                    htrs.closeTime,
                    htrs.resultSetNumber,
                    htrs.hashtableSize,
                    htrs.keyColumns,
                    HashScanResultSet.printQualifiers(
                        htrs.nextQualifiers),
                    htrs.scanProperties,
                    htrs.optimizerEstimatedRowCount,
                    htrs.optimizerEstimatedCost,
                    subqueryTrackingArray,
                    getResultSetStatistics(htrs.source)
                    );
		}
		else if (rs instanceof VTIResultSet)
		{
			VTIResultSet vtirs = (VTIResultSet) rs;

			return new RealVTIStatistics(
										vtirs.numOpens,
										vtirs.rowsSeen,
										vtirs.rowsFiltered,
										vtirs.constructorTime,
										vtirs.openTime,
										vtirs.nextTime,
										vtirs.closeTime,
										vtirs.resultSetNumber,
										vtirs.javaClassName,
										vtirs.optimizerEstimatedRowCount,
										vtirs.optimizerEstimatedCost
										);
		}

		else if (rs instanceof DependentResultSet)
		{
			boolean instantaneousLocks = false;
			DependentResultSet dsrs = (DependentResultSet) rs;
			String startPosition = null;
			String stopPosition = null;
			String isolationLevel =  null;
			String lockString = null;
			String lockRequestString = null;

			switch (dsrs.isolationLevel)
			{
				case TransactionController.ISOLATION_SERIALIZABLE:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_SERIALIZABLE);
					break;

				case TransactionController.ISOLATION_REPEATABLE_READ:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_REPEATABLE_READ);
					break;

				case TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK:
					instantaneousLocks = true;
					//fall through
				case TransactionController.ISOLATION_READ_COMMITTED:
					isolationLevel = MessageService.getTextMessage(
												SQLState.LANG_READ_COMMITTED);
					break;

				case TransactionController.ISOLATION_READ_UNCOMMITTED:
					isolationLevel = 
                        MessageService.getTextMessage(
                            SQLState.LANG_READ_UNCOMMITTED);
					break;
			}

			if (dsrs.forUpdate)
			{
				lockString = MessageService.getTextMessage(
												SQLState.LANG_EXCLUSIVE);
			}
			else
			{
				if (instantaneousLocks)
				{
					lockString = MessageService.getTextMessage(
											SQLState.LANG_INSTANTANEOUS_SHARE);
				}
				else
				{
					lockString = MessageService.getTextMessage(
														SQLState.LANG_SHARE);
				}
			}

			switch (dsrs.lockMode)
			{
				case TransactionController.MODE_TABLE:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_TABLE);
					break;

				case TransactionController.MODE_RECORD:
					// RESOLVE: Not sure this will really work, as we
					// are tacking together English words to make a phrase.
					// Will this work in other languages?
					lockRequestString = lockString + " " +
										MessageService.getTextMessage(
											SQLState.LANG_ROW);
					break;
			}

			/* Start and stop position strings will be non-null
			 * if the dSRS has been closed.  Otherwise, we go off
			 * and build the strings now.
			 */
			startPosition = dsrs.startPositionString;
			if (startPosition == null)
			{
				startPosition = dsrs.printStartPosition();
			}
			stopPosition = dsrs.stopPositionString;
			if (stopPosition == null)
			{
				stopPosition = dsrs.printStopPosition();
			}
		
			return new 
                RealTableScanStatistics(
                    dsrs.numOpens,
                    dsrs.rowsSeen,
                    dsrs.rowsFiltered,
                    dsrs.constructorTime,
                    dsrs.openTime,
                    dsrs.nextTime,
                    dsrs.closeTime,
                    dsrs.resultSetNumber,
                    dsrs.tableName,
                    dsrs.indexName,
                    dsrs.isConstraint,
                    dsrs.printQualifiers(),
                    dsrs.getScanProperties(),
                    startPosition,
                    stopPosition,
                    isolationLevel,
                    lockRequestString,
                    dsrs.rowsPerRead,
                    dsrs.coarserLock,
                    dsrs.optimizerEstimatedRowCount,
                    dsrs.optimizerEstimatedCost);
		}
		else
		{
			return null;
		}
	}

	//
	// class interface
	//
	public RealResultSetStatisticsFactory() 
	{
	}

}
