/*

   Derby - Class org.apache.derby.impl.sql.compile.UpdateNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.sql.execute.FKInfo;
import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.CheckConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.vti.DeferModification;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Vector;

/**
 * An UpdateNode represents an UPDATE statement.  It is the top node of the
 * query tree for that statement.
 * For positioned update, there may be no from table specified.
 * The from table will be derived from the cursor specification of
 * the named cursor.
 *
 * @author Jeff Lichtman
 */

public final class UpdateNode extends DMLModStatementNode
{
	//Note: These are public so they will be visible to
	//the RepUpdateNode.
	public int[]				changedColumnIds;
	public ExecRow				emptyHeapRow;
	public boolean				deferred;
	public ValueNode			checkConstraints;
	public FKInfo				fkInfo;
	
	protected FromTable			targetTable;
	protected FormatableBitSet 			readColsBitSet;
	protected boolean 			positionedUpdate;

	/* Column name for the RowLocation in the ResultSet */
	public static final String COLUMNNAME = "###RowLocationToUpdate";

	/**
	 * Initializer for an UpdateNode.
	 *
	 * @param targetTableName	The name of the table to update
	 * @param resultSet		The ResultSet that will generate
	 *				the rows to update from the given table
	 */

	public void init(
			   Object targetTableName,
			   Object resultSet)
	{
		super.init(resultSet);
		this.targetTableName = (TableName) targetTableName;
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
			return targetTableName.toString() + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "UPDATE";
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (targetTableName != null)
			{
				printLabel(depth, "targetTableName: ");
				targetTableName.treePrint(depth + 1);
			}

			/* RESOLVE - need to print out targetTableDescriptor */
		}
	}

	/**
	 * Bind this UpdateNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 * <p>
	 * Binding an update will also massage the tree so that
	 * the ResultSetNode has a set of columns to contain the old row
	 * value, followed by a set of columns to contain the new row
	 * value, followed by a column to contain the RowLocation of the
	 * row to be updated.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		FromList	fromList = (FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());
		ResultColumn				rowLocationColumn = null;
		ValueNode		            rowLocationNode = null;
		TableName					cursorTargetTableName = null;
		CurrentOfNode       		currentOfNode = null;
		FromList					resultFromList;
		ResultColumnList			afterColumns = null;

		DataDictionary dataDictionary = getDataDictionary();

		bindTables(dataDictionary);

		// wait to bind named target table until the cursor
		// binding is done, so that we can get it from the
		// cursor if this is a positioned update.

		// for positioned update, get the cursor's target table.
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((resultSet!=null && resultSet instanceof SelectNode), 
				"Update must have a select result set");
		}

		SelectNode sel;
		sel = (SelectNode)resultSet;
		targetTable = (FromTable) sel.fromList.elementAt(0);
		if (targetTable instanceof CurrentOfNode) 
		{	
			positionedUpdate = true;
			currentOfNode = (CurrentOfNode) targetTable;
			cursorTargetTableName = currentOfNode.getBaseCursorTargetTableName();

			// instead of an assert, we might say the cursor is not updatable.
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(cursorTargetTableName != null);
			}
		}

		if (targetTable instanceof FromVTI)
		{
			targetVTI = (FromVTI) targetTable;
			targetVTI.setTarget();
		}
		else
		{
			// positioned update can leave off the target table.
			// we get it from the cursor supplying the position.
			if (targetTableName == null)
			{
				// verify we have current of
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(cursorTargetTableName!=null);

				targetTableName = cursorTargetTableName;
			}
			// for positioned update, we need to verify that
			// the named table is the same as the cursor's target.
			else if (cursorTargetTableName != null)
			{
				// this match requires that the named table in the update
				// be the same as a correlation name in the cursor.
				if ( !targetTableName.equals(cursorTargetTableName))
				{
					throw StandardException.newException(SQLState.LANG_CURSOR_UPDATE_MISMATCH, 
						targetTableName,
						currentOfNode.getCursorName());
				}
			}

			// because we verified that the tables match
			// and we already bound the cursor or the select,
			// the table descriptor should always be found.
			verifyTargetTable();
		}


		/* OVERVIEW - We generate a new ResultColumn, CurrentRowLocation(), and
		 * prepend it to the beginning of the source ResultColumnList.  This
		 * will tell us which row(s) to update at execution time.  However,
		 * we must defer prepending this generated column until the other
		 * ResultColumns are bound since there will be no ColumnDescriptor
		 * for the generated column.  Thus, the sequence of actions is:
		 *
		 *		o  Bind existing ResultColumnList (columns in SET clause)
		 *		o  If this is a positioned update with a FOR UPDATE OF list,
		 *		   then verify that all of the target columns are in the
		 *		   FOR UPDATE OF list.
		 *		o  Get the list of indexes that need to be updated.
		 *		o  Create a ResultColumnList of all the columns in the target
		 *		   table - this represents the old row.
		 *		o  If we don't know which columns are being updated, 
	 	 *		   expand the original ResultColumnList to include all the
		 *		   columns in the target table, and sort it to be in the
		 *		   order of the columns in the target table.  This represents
		 *		   the new row.  Append it to the ResultColumnList representing
		 *		   the old row.
		 *		o  Construct the changedColumnIds array sorted by column position.
		 *		o  Generate the read column bit map and append any columns
		 *		   needed for index maint, etc.
		 *		o  Generate a new ResultColumn for CurrentRowLocation() and 
		 *		   mark it as a generated column.
		 *		o  Append the new ResultColumn to the ResultColumnList
		 *		   (This must be done before binding the expressions, so
		 *		   that the proper type info gets propagated to the new 
		 *		   ResultColumn.)
		 *		o  Bind the expressions.
		 *		o  Bind the generated ResultColumn.
		 */

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList.size() == 0,
				"fromList.size() is expected to be 0, not " + 
				fromList.size() +
				" on return from RS.bindExpressions()");
		}

		/*
		** The current result column list is the one supplied by the user.
		** Mark these columns as "updated", so we can tell later which
		** columns are really being updated, and which have been added
		** but are not really being updated.
		*/
		resultSet.getResultColumns().markUpdated();

		/* Prepend CurrentRowLocation() to the select's result column list. */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT((resultSet.resultColumns != null),	
							  "resultColumns is expected not to be null at bind time");

		/*
		** Get the result FromTable, which should be the only table in the
	 	** from list.
		*/
		resultFromList = resultSet.getFromList();
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(resultFromList.size() == 1,
			"More than one table in result from list in an update.");

		/* Bind the original result columns by column name */
 		resultSet.bindResultColumns(targetTableDescriptor,
									targetVTI,
 									resultSet.resultColumns, this,
 									fromList);

		LanguageConnectionContext lcc = getLanguageConnectionContext();
		if (lcc.getAutoincrementUpdate() == false)
			resultSet.getResultColumns().checkAutoincrement(null);

		/*
		** Mark the columns in this UpdateNode's result column list as
		** updateable in the ResultColumnList of the table being updated.
		** only do this for FromBaseTables - if the result table is a
		** CurrentOfNode, it already knows what columns in its cursor
		** are updateable.
		*/
		boolean allColumns = false;
		if (targetTable instanceof FromBaseTable)
		{
			((FromBaseTable) targetTable).markUpdated(
												resultSet.getResultColumns());
		}
		else if (targetTable instanceof FromVTI)
		{
            resultColumnList = resultSet.getResultColumns();
		}
		else
		{
			/*
			** Positioned update: WHERE CURRENT OF
			*/
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(currentOfNode != null, "currentOfNode is null");
			}

			ExecPreparedStatement	 cursorStmt = currentOfNode.getCursorStatement();
			String[] ucl = cursorStmt.getUpdateColumns();

			/*
			** If there is no update column list, we need to build
			** out the result column list to have all columns.
			*/
			if (ucl == null || (ucl.length == 0))
			{
				/*
				** Get the resultColumnList representing ALL of the columns in the 
				** base table.  This is the "before" portion of the result row.
				*/
				getResultColumnList();

				/*
				** Add the "after" portion of the result row.  This is the update
				** list augmented to include every column in the target table.
				** Those columns that are not being updated are set to themselves.
				** The expanded list will be in the order of the columns in the base
				** table.
				*/
				afterColumns = resultSet.getResultColumns().expandToAll(
													targetTableDescriptor,
													targetTable.getTableName());
	
				/*
				** Need to get all indexes here since we aren't calling
				** getReadMap().
				*/
				getAffectedIndexes(targetTableDescriptor, 
									(ResultColumnList)null, (FormatableBitSet)null); 
				allColumns = true;
			}
			else
			{
				/* Check the updatability */
				resultSet.getResultColumns().checkColumnUpdateability(ucl,
								currentOfNode.getCursorName());
			}
		}

		changedColumnIds = getChangedColumnIds(resultSet.getResultColumns());

		/*
		** We need to add in all the columns that are needed
		** by the constraints on this table.  
		*/
		if (!allColumns && targetVTI == null)
		{
 			readColsBitSet = new FormatableBitSet();
			FromBaseTable fbt = getResultColumnList(resultSet.getResultColumns());
			afterColumns = resultSet.getResultColumns().copyListAndObjects();

			readColsBitSet = getReadMap(dataDictionary, 
										targetTableDescriptor, 
										afterColumns);

			afterColumns = fbt.addColsToList(afterColumns, readColsBitSet);
			resultColumnList = fbt.addColsToList(resultColumnList, readColsBitSet);

			/*
			** If all bits are set, then behave as if we chose all
			** in the first place
			*/
			int i = 1;
			int size = targetTableDescriptor.getMaxColumnID();
			for (; i <= size; i++)
			{
				if (!readColsBitSet.get(i))
				{
					break;
				}
			}

			if (i > size)
			{
				readColsBitSet = null;
				allColumns = true;
			}	
		}

		if (targetVTI == null)
		{
			/*
			** Construct an empty heap row for use in our constant action.
			*/
			emptyHeapRow = targetTableDescriptor.getEmptyExecRow(getContextManager());

			/* Append the list of "after" columns to the list of "before" columns,
			 * preserving the afterColumns list.  (Necessary for binding
			 * check constraints.)
			 */
			resultColumnList.appendResultColumns(afterColumns, false);

			/* Generate the RowLocation column */
			rowLocationNode = (CurrentRowLocationNode) getNodeFactory().getNode(
										C_NodeTypes.CURRENT_ROW_LOCATION_NODE,
										getContextManager());
        }
        else
        {
			rowLocationNode = (NumericConstantNode) getNodeFactory().getNode(
										C_NodeTypes.INT_CONSTANT_NODE,
                                        ReuseFactory.getInteger( 0),
										getContextManager());
        }
            
        rowLocationColumn =
          (ResultColumn) getNodeFactory().getNode(
              C_NodeTypes.RESULT_COLUMN,
              COLUMNNAME,
              rowLocationNode,
              getContextManager());
        rowLocationColumn.markGenerated();

			/* Append to the ResultColumnList */
        resultColumnList.addResultColumn(rowLocationColumn);

		/* The last thing that we do to the generated RCL is to clear
		 * the table name out from each RC.  The table name is
		 * unnecessary for an update.  More importantly, though, it
		 * creates a problem in the degenerate case with a positioned
		 * update.  The user must specify the base table name for a
		 * positioned update.  If a correlation name was specified for
		 * the cursor, then a match for the ColumnReference would not
		 * be found if we didn't null out the name.  (Aren't you
		 * glad you asked?)
		 */
		resultColumnList.clearTableNames();

		/* Set the new result column list in the result set */
		resultSet.setResultColumns(resultColumnList);

		/* Bind the expressions */
		super.bindExpressions();

		/* Bind untyped nulls directly under the result columns */
		resultSet.
			getResultColumns().
				bindUntypedNullsToResultColumns(resultColumnList);

		if (null != rowLocationColumn)
		{
			/* Bind the new ResultColumn */
			rowLocationColumn.bindResultColumnToExpression();
		}

		resultColumnList.checkStorableExpressions();

		/* Insert a NormalizeResultSetNode above the source if the source
		 * and target column types and lengths do not match.
		 */
		if (! resultColumnList.columnTypesAndLengthsMatch())
 		{
			resultSet = resultSet.genNormalizeResultSetNode(resultSet, true);
			resultColumnList.copyTypesAndLengthsToSource(resultSet.getResultColumns());
								
 			if (hasCheckConstraints(dataDictionary, targetTableDescriptor))
 			{
 				/* Get and bind all check constraints on the columns
	 			 * being updated.  We want to bind the check constraints against
	 			 * the after columns.  We need to bind against the portion of the
	 			 * resultColumns in the new NormalizeResultSet that point to 
	 			 * afterColumns.  Create an RCL composed of just those RCs in
	 			 * order to bind the check constraints.
	 			 */
	 			int afterColumnsSize = afterColumns.size();
	 			afterColumns = (ResultColumnList) getNodeFactory().getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());
	 			ResultColumnList normalizedRCs = resultSet.getResultColumns();
	 			for (int index = 0; index < afterColumnsSize; index++)
	 			{
	 				afterColumns.addElement(normalizedRCs.elementAt(index + afterColumnsSize));
	 			}
			}
		}

        if( null != targetVTI)
		{
            deferred = VTIDeferModPolicy.deferIt( DeferModification.UPDATE_STATEMENT,
                                                  targetVTI,
                                                  resultColumnList.getColumnNames(),
                                                  sel.getWhereClause());
		}
        else // not VTI
        {
            /* we always include triggers in core language */
            boolean hasTriggers = (getAllRelevantTriggers(dataDictionary, targetTableDescriptor, 
                                                          changedColumnIds, true).size() > 0);

            /* Get and bind all constraints on the columns being updated */
            checkConstraints = bindConstraints( dataDictionary,
                                                getNodeFactory(),
                                                targetTableDescriptor,
                                                null,
                                                hasTriggers ? resultColumnList : afterColumns,
                                                changedColumnIds,
                                                readColsBitSet,
                                                false,
                                                true); /* we always include triggers in core language */

            /* If the target table is also a source table, then
             * the update will have to be in deferred mode
             * For updates, this means that the target table appears in a
             * subquery.  Also, self referencing foreign keys are
             * deferred.  And triggers cause an update to be deferred.
             */
            if (resultSet.subqueryReferencesTarget(
                targetTableDescriptor.getName(), true) ||
                requiresDeferredProcessing())
            {
                deferred = true;
            }
        }

		return this;
	} // end of bind()

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If this node references a SESSION schema table, then return true. 
		return(resultSet.referencesSessionSchema());

	}

	/**
	 * Compile constants that Execution will use
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		/*
		** Updates are also deferred if they update a column in the index
		** used to scan the table being updated.
		*/
		if (! deferred )
		{
			ConglomerateDescriptor updateCD =
										targetTable.
											getTrulyTheBestAccessPath().
												getConglomerateDescriptor();

			if (updateCD != null && updateCD.isIndex())
			{
				int [] baseColumns =
						updateCD.getIndexDescriptor().baseColumnPositions();

				if (resultSet.
						getResultColumns().
										updateOverlaps(baseColumns))
				{
					deferred = true;
				}
			}
		}

        if( null == targetTableDescriptor)
		{
			/* Return constant action for VTI
			 * NOTE: ConstantAction responsible for preserving instantiated
			 * VTIs for in-memory queries and for only preserving VTIs
			 * that implement Serializable for SPSs.
			 */
			return	getGenericConstantActionFactory().getUpdatableVTIConstantAction( DeferModification.UPDATE_STATEMENT,
						deferred, changedColumnIds);
		}

		int lockMode = resultSet.updateTargetLockMode();
		long heapConglomId = targetTableDescriptor.getHeapConglomerateId();
		TransactionController tc = 
			getLanguageConnectionContext().getTransactionCompile();
		StaticCompiledOpenConglomInfo[] indexSCOCIs = 
			new StaticCompiledOpenConglomInfo[indexConglomerateNumbers.length];

		for (int index = 0; index < indexSCOCIs.length; index++)
		{
			indexSCOCIs[index] = tc.getStaticCompiledConglomInfo(indexConglomerateNumbers[index]);
		}

		/*
		** Do table locking if the table's lock granularity is
		** set to table.
		*/
		if (targetTableDescriptor.getLockGranularity() == TableDescriptor.TABLE_LOCK_GRANULARITY)
		{
			lockMode = TransactionController.MODE_TABLE;
		}


		return	getGenericConstantActionFactory().getUpdateConstantAction
			( heapConglomId,
			  targetTableDescriptor.getTableType(),
			  tc.getStaticCompiledConglomInfo(heapConglomId),
			  indicesToMaintain,
			  indexConglomerateNumbers,
			  indexSCOCIs,
			  indexNames,
			  emptyHeapRow,
			  deferred,
			  targetTableDescriptor.getUUID(),
			  lockMode,
			  false,
			  changedColumnIds, null, null, 
			  getFKInfo(),
			  getTriggerInfo(),
			  (readColsBitSet == null) ? (FormatableBitSet)null : new FormatableBitSet(readColsBitSet),
			  getReadColMap(targetTableDescriptor.getNumberOfColumns(),readColsBitSet),
			  resultColumnList.getStreamStorableColIds(targetTableDescriptor.getNumberOfColumns()),
			  (readColsBitSet == null) ? 
				  targetTableDescriptor.getNumberOfColumns() :
				  readColsBitSet.getNumBitsSet(),			
			  positionedUpdate,
			  resultSet.isOneRowResultSet()
			  );
	}

	/**
	 * Updates are deferred if they update a column in the index
	 * used to scan the table being updated.
	 */
	protected void setDeferredForUpdateOfIndexColumn()
	{
		/* Don't bother checking if we're already deferred */
		if (! deferred )
		{
			/* Get the conglomerate descriptor for the target table */
			ConglomerateDescriptor updateCD =
										targetTable.
											getTrulyTheBestAccessPath().
												getConglomerateDescriptor();

			/* If it an index? */
			if (updateCD != null && updateCD.isIndex())
			{
				int [] baseColumns =
						updateCD.getIndexDescriptor().baseColumnPositions();

				/* Are any of the index columns updated? */
				if (resultSet.
						getResultColumns().
										updateOverlaps(baseColumns))
				{
					deferred = true;
				}
			}
		}
	}

	/**
	 * Code generation for update.
	 * The generated code will contain:
	 *		o  A static member for the (xxx)ResultSet with the RowLocations	and
	 *		   new update values
	 *		o  The static member will be assigned the appropriate ResultSet within
	 *		   the nested calls to get the ResultSets.  (The appropriate cast to the
	 *		   (xxx)ResultSet will be generated.)
	 *		o  The CurrentRowLocation() in SelectNode's select list will generate
	 *		   a new method for returning the RowLocation as well as a call to
	 *		   that method when generating the (xxx)ResultSet.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The method for the execute() method to be built
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		//If the DML is on the temporary table, generate the code to mark temporary table as modified in the current UOW
		generateCodeForTemporaryTable(acb, mb);

		/* generate the parameters */
		if(!isDependentTable)
			generateParameterValueSet(acb);


		/* Create the static declaration for the scan ResultSet which generates the
		 * RowLocations to be updated
		 * RESOLVE - Need to deal with the type of the static member.
		 */
		acb.newFieldDeclaration(Modifier.PRIVATE, 
								ClassName.CursorResultSet, 
								acb.newRowLocationScanResultSetName());

		/*
		** Generate the update result set, giving it either the original
		** source or the normalize result set, the constant action,
		** and "this".
		*/

		acb.pushGetResultSetFactoryExpression(mb);
		resultSet.generate(acb, mb); // arg 1

        if( null != targetVTI)
        {
			targetVTI.assignCostEstimate(resultSet.getNewCostEstimate());
            acb.pushThisAsActivation(mb); // arg 2
            mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getUpdateVTIResultSet", ClassName.ResultSet, 2);
		}
        else
        {
            // generate code to evaluate CHECK CONSTRAINTS
            generateCheckConstraints( checkConstraints, acb, mb ); // arg 2

            acb.pushThisAsActivation(mb);

            if(isDependentTable)
            {
                mb.push(acb.addItem(makeConstantAction()));
                mb.push(acb.addItem(makeResultDescription()));
                mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getDeleteCascadeUpdateResultSet",
                              ClassName.ResultSet, 5);
            }else
            {
                mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getUpdateResultSet",
                              ClassName.ResultSet, 3);
            }
        }
		/*
		** ensure all parameters have been generated
		*/
		if(!isDependentTable)
			generateParameterHolders(acb);

	}

	/**
	 * Return the type of statement, something from
	 * StatementType.
	 *
	 * @return the type of statement
	 */
	protected final int getStatementType()
	{
		return StatementType.UPDATE;
	}


	/**
	 * Gets the map of all columns which must be read out of the base table.
	 * These are the columns needed to<UL>:
	 *		<LI>maintain indices</LI>
	 *		<LI>maintain foreign keys</LI>
	 *		<LI>support Replication's Delta Optimization</LI></UL>
	 * <p>
	 * The returned map is a FormatableBitSet with 1 bit for each column in the
	 * table plus an extra, unsued 0-bit. If a 1-based column id must
	 * be read from the base table, then the corresponding 1-based bit
	 * is turned ON in the returned FormatableBitSet.
	 * <p> 
	 * <B>NOTE</B>: this method is not expected to be called when
	 * all columns are being updated (i.e. updateColumnList is null).
	 *
	 * @param dd				the data dictionary to look in
	 * @param baseTable		the base table descriptor
	 * @param updateColumnList the rcl for the update. CANNOT BE NULL
	 *
	 * @return a FormatableBitSet of columns to be read out of the base table
	 *
	 * @exception StandardException		Thrown on error
	 */
	public	FormatableBitSet	getReadMap
	(
		DataDictionary		dd,
		TableDescriptor		baseTable,
		ResultColumnList	updateColumnList
	)
		throws StandardException
	{
		boolean[]	needsDeferredProcessing = new boolean[1];
		needsDeferredProcessing[0] = requiresDeferredProcessing();

		Vector		conglomVector = new Vector();
		relevantCdl = new ConstraintDescriptorList();
		relevantTriggers =  new GenericDescriptorList();

		FormatableBitSet	columnMap = UpdateNode.getUpdateReadMap(baseTable,
			updateColumnList, conglomVector, relevantCdl, relevantTriggers, needsDeferredProcessing );

		markAffectedIndexes( conglomVector );

		adjustDeferredFlag( needsDeferredProcessing[0] );

		return	columnMap;
	}


	/**
	 * Construct the changedColumnIds array. Note we sort its entries by
	 * columnId.
	 */
	private int[] getChangedColumnIds(ResultColumnList rcl)
	{
		if (rcl == null) { return (int[])null; }
		else { return rcl.sortMe(); }
	}
    /**
	  *	Builds a bitmap of all columns which should be read from the
	  *	Store in order to satisfy an UPDATE statement.
	  *
	  *	Is passed a list of updated columns. Does the following:
	  *
	  *	1)	finds all indices which overlap the updated columns
	  *	2)	adds the index columns to a bitmap of affected columns
	  *	3)	adds the index descriptors to a list of conglomerate
	  *		descriptors.
	  *	4)	finds all constraints which overlap the updated columns
	  *		and adds the constrained columns to the bitmap
	  *	5)	finds all triggers which overlap the updated columns.
	  *	6)	if there are any triggers, marks all columns in the bitmap
	  *	7)	adds the triggers to an evolving list of triggers
	  *
	  *	@param	updateColumnList	a list of updated columns
	  *	@param	conglomVector		OUT: vector of affected indices
	  *	@param	relevantConstraints	IN/OUT. Empty list is passed in. We hang constraints on it as we go.
	  *	@param	relevantTriggers	IN/OUT. Passed in as an empty list. Filled in as we go.
	  *	@param	needsDeferredProcessing	IN/OUT. true if the statement already needs
	  *									deferred processing. set while evaluating this
	  *									routine if a trigger or constraint requires
	  *									deferred processing
	  *
	  * @return a FormatableBitSet of columns to be read out of the base table
	  *
	  * @exception StandardException		Thrown on error
	  */
	public static FormatableBitSet getUpdateReadMap
	(
		TableDescriptor				baseTable,
		ResultColumnList			updateColumnList,
		Vector						conglomVector,
		ConstraintDescriptorList	relevantConstraints,
		GenericDescriptorList		relevantTriggers,
		boolean[]					needsDeferredProcessing
	)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(updateColumnList != null, "updateColumnList is null");
		}

		int		columnCount = baseTable.getMaxColumnID();
		FormatableBitSet	columnMap = new FormatableBitSet(columnCount + 1);

		/*
		** Add all the changed columns.  We don't strictly
		** need the before image of the changed column in all cases,
		** but it makes life much easier since things are set
		** up around the assumption that we have the before
		** and after image of the column.
		*/
		int[]	changedColumnIds = updateColumnList.sortMe();

		for (int ix = 0; ix < changedColumnIds.length; ix++)
		{
			columnMap.set(changedColumnIds[ix]);
		}

		/* 
		** Get a list of the indexes that need to be 
		** updated.  ColumnMap contains all indexed
		** columns where 1 or more columns in the index
		** are going to be modified.
		*/
		DMLModStatementNode.getXAffectedIndexes(baseTable, updateColumnList, columnMap, conglomVector );
 
		/* 
		** Add all columns needed for constraints.  We don't
		** need to bother with foreign key/primary key constraints
		** because they are added as a side effect of adding
		** their indexes above.
		*/
		baseTable.getAllRelevantConstraints
			( StatementType.UPDATE, false, changedColumnIds, needsDeferredProcessing, relevantConstraints );

		int rclSize = relevantConstraints.size();
		for (int index = 0; index < rclSize; index++)
		{
			ConstraintDescriptor cd = relevantConstraints.elementAt(index);
			if (cd.getConstraintType() != DataDictionary.CHECK_CONSTRAINT)
			{
				continue;
			}

			int[] refColumns = ((CheckConstraintDescriptor)cd).getReferencedColumns();
			for (int i = 0; i < refColumns.length; i++)
			{
				columnMap.set(refColumns[i]);
			}
		}

		/*
	 	** If we have any triggers, then get all the columns
		** because we don't know what the user will ultimately
		** reference.
	 	*/

		baseTable.getAllRelevantTriggers( StatementType.UPDATE, changedColumnIds, relevantTriggers );
		if ( relevantTriggers.size() > 0 ) { needsDeferredProcessing[0] = true; }

		if (relevantTriggers.size() > 0)
		{
			for (int i = 1; i <= columnCount; i++)
			{
				columnMap.set(i);
			}
		}

		return	columnMap;
	}
} // end of UpdateNode
