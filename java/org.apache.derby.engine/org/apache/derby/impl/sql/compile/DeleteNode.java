/*

   Derby - Class org.apache.derby.impl.sql.compile.DeleteNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableProperties;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.IgnoreFilter;
import org.apache.derby.iapi.sql.compile.ScopeFilter;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptorList;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.vti.DeferModification;


/**
 * A DeleteNode represents a DELETE statement. It is the top-level node
 * for the statement.
 *
 * For positioned delete, there may be no from table specified.
 * The from table will be derived from the cursor specification of
 * the named cursor.
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class DeleteNode extends DMLModStatementNode
{
	/* Column name for the RowLocation column in the ResultSet */
	private static final String COLUMNNAME = "###RowLocationToDelete";

	/* Filled in by bind. */
    private boolean deferred;
    private FromTable targetTable;
    private FormatableBitSet readColsBitSet;

	private ConstantAction[] dependentConstantActions;
	private boolean cascadeDelete;
	private StatementNode[] dependentNodes;

	/**
     * Constructor for a DeleteNode.
	 *
	 * @param targetTableName	The name of the table to delete from
	 * @param queryExpression	The query expression that will generate
	 *				the rows to delete from the given table
     * @param matchingClause   Non-null if this DML is part of a MATCHED clause of a MERGE statement.
     * @param cm                The context manager
	 */

    DeleteNode
        (
         TableName targetTableName,
         ResultSetNode queryExpression,
         MatchingClauseNode matchingClause,
         ContextManager cm
         )
    {
        super( queryExpression, matchingClause, cm );
        this.targetTableName = targetTableName;
	}

    @Override
    String statementToString()
	{
		return "DELETE";
	}

	/**
	 * Bind this DeleteNode.  This means looking up tables and columns and
	 * getting their types, and figuring out the result types of all
	 * expressions, as well as doing view resolution, permissions checking,
	 * etc.
	 * <p>
	 * If any indexes need to be updated, we add all the columns in the
	 * base table to the result column list, so that we can use the column
	 * values as look-up keys for the index rows to be deleted.  Binding a
	 * delete will also massage the tree so that the ResultSetNode has 
	 * column containing the RowLocation of the base row.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void bindStatement() throws StandardException
	{
		// We just need select privilege on the where clause tables
//IC see: https://issues.apache.org/jira/browse/DERBY-464
		getCompilerContext().pushCurrentPrivType( Authorizer.SELECT_PRIV);
		try
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            FromList fromList = new FromList(
                    getOptimizerFactory().doJoinOrderOptimization(),
                    getContextManager());

            ResultColumn                rowLocationColumn = null;
			CurrentRowLocationNode		rowLocationNode;
			TableName					cursorTargetTableName = null;
			CurrentOfNode       		currentOfNode = null;

            //
            // Don't add privilege requirements for the UDT types of columns.
            // The compiler will attempt to add these when generating the full column list during
            // binding of the tables.
            //
//IC see: https://issues.apache.org/jira/browse/DERBY-6434
            IgnoreFilter    ignorePermissions = new IgnoreFilter();
            getCompilerContext().addPrivilegeFilter( ignorePermissions );
            
			DataDictionary dataDictionary = getDataDictionary();
            // for DELETE clause of a MERGE statement, the tables have already been bound
			if ( !inMatchingClause() ) { super.bindTables(dataDictionary); }

			// wait to bind named target table until the underlying
			// cursor is bound, so that we can get it from the
			// cursor if this is a positioned delete.

			// for positioned delete, get the cursor's target table.
			if (SanityManager.DEBUG)
				SanityManager.ASSERT(resultSet != null && resultSet instanceof SelectNode,
				"Delete must have a select result set");

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            SelectNode sel = (SelectNode)resultSet;
			targetTable = (FromTable) sel.fromList.elementAt(0);
			if (targetTable instanceof CurrentOfNode)
			{
				currentOfNode = (CurrentOfNode) targetTable;

				cursorTargetTableName = inMatchingClause() ?
                    targetTableName : currentOfNode.getBaseCursorTargetTableName();
				// instead of an assert, we might say the cursor is not updatable.
				if (SanityManager.DEBUG)
					SanityManager.ASSERT(cursorTargetTableName != null);
			}

			if (targetTable instanceof FromVTI)
			{
				targetVTI = (FromVTI) targetTable;
				targetVTI.setTarget();
			}
			else
			{
				// positioned delete can leave off the target table.
				// we get it from the cursor supplying the position.
				if (targetTableName == null)
				{
					// verify we have current of
					if (SanityManager.DEBUG)
						SanityManager.ASSERT(cursorTargetTableName!=null);

				targetTableName = cursorTargetTableName;
				}
				// for positioned delete, we need to verify that
				// the named table is the same as the cursor's target (base table name).
				else if (cursorTargetTableName != null)
				{
					// this match requires that the named table in the delete
					// be the same as a base name in the cursor.
					if ( !targetTableName.equals(cursorTargetTableName))
					{
						throw StandardException.newException(SQLState.LANG_CURSOR_DELETE_MISMATCH, 
							targetTableName,
							currentOfNode.getCursorName());
					}
				}
			}
		
			// descriptor must exist, tables already bound.
			verifyTargetTable();
//IC see: https://issues.apache.org/jira/browse/DERBY-714
//IC see: https://issues.apache.org/jira/browse/DERBY-571

			/* Generate a select list for the ResultSetNode - CurrentRowLocation(). */
			if ( SanityManager.DEBUG )
            {
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
				SanityManager.ASSERT((resultSet.getResultColumns() == null),
							  "resultColumns is expected to be null until bind time");
            }


			if (targetTable instanceof FromVTI)
			{
				getResultColumnList();
				resultColumnList = targetTable.getResultColumnsForList(null, 
								resultColumnList, null);

				/* Set the new result column list in the result set */
				resultSet.setResultColumns(resultColumnList);
			}
			else
			{
            
				/*
				** Start off assuming no columns from the base table
				** are needed in the rcl.
				*/

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                resultColumnList =
                        new ResultColumnList(getContextManager());

				FromBaseTable fbt = getResultColumnList(resultColumnList);

				readColsBitSet = getReadMap(dataDictionary,
										targetTableDescriptor);

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
				}

				/* Generate the RowLocation column */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                rowLocationNode =
                        new CurrentRowLocationNode(getContextManager());
                rowLocationColumn =
                        new ResultColumn(COLUMNNAME,
                                         rowLocationNode,
                                         getContextManager());
				rowLocationColumn.markGenerated();

				/* Append to the ResultColumnList */
				resultColumnList.addResultColumn(rowLocationColumn);

				/* Force the added columns to take on the table's correlation name, if any */
				correlateAddedColumns( resultColumnList, targetTable );
			
                /* Add the new result columns to the driving result set */
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
                ResultColumnList    originalRCL = resultSet.getResultColumns();
                if ( originalRCL != null )
                {
                    originalRCL.appendResultColumns( resultColumnList, false );
                    resultColumnList = originalRCL;
                }
				resultSet.setResultColumns(resultColumnList);
			}

            // done excluding column types from privilege checking
            getCompilerContext().removePrivilegeFilter( ignorePermissions );

			/* Bind the expressions before the ResultColumns are bound */

            // only add privileges when we're inside the WHERE clause
            ScopeFilter scopeFilter = new ScopeFilter( getCompilerContext(), CompilerContext.WHERE_SCOPE, 1 );
            getCompilerContext().addPrivilegeFilter( scopeFilter );
			super.bindExpressions();
            getCompilerContext().removePrivilegeFilter( scopeFilter );

			/* Bind untyped nulls directly under the result columns */
			resultSet.getResultColumns().
				bindUntypedNullsToResultColumns(resultColumnList);

			if (! (targetTable instanceof FromVTI))
			{
				/* Bind the new ResultColumn */
				rowLocationColumn.bindResultColumnToExpression();
				bindConstraints(dataDictionary,
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
//IC see: https://issues.apache.org/jira/browse/DERBY-532
                        getOptimizerFactory(),
                        targetTableDescriptor,
                        null,
                        resultColumnList,
                        (int[]) null,
                        readColsBitSet,
                        true, // we alway include triggers in core language
                        new boolean[1]); // dummy

				/* If the target table is also a source table, then
			 	* the delete will have to be in deferred mode
			 	* For deletes, this means that the target table appears in a
			 	* subquery.  Also, self-referencing foreign key deletes
		 	 	* are deferred.  And triggers cause the delete to be deferred.
			 	*/
//IC see: https://issues.apache.org/jira/browse/DERBY-464
				if (resultSet.subqueryReferencesTarget(
									targetTableDescriptor.getName(), true) ||
					requiresDeferredProcessing())
				{
					deferred = true;
				}
			}
			else
			{
            	deferred = VTIDeferModPolicy.deferIt( DeferModification.DELETE_STATEMENT,
                                                  targetVTI,
                                                  null,
                                                  sel.getWhereClause());
			}

			/* Verify that all underlying ResultSets reclaimed their FromList */
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(fromList.size() == 0,
					"fromList.size() is expected to be 0, not " +
					fromList.size() +
					" on return from RS.bindExpressions()");
			}

			//In case of cascade delete , create nodes for
			//the ref action  dependent tables and bind them.
			if(fkTableNames != null)
			{
				String currentTargetTableName = targetTableDescriptor.getSchemaName() +
						 "." + targetTableDescriptor.getName();

				if(!isDependentTable){
					//graph node
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
                    dependentTables = new HashSet<String>();
				}

				/*Check whether the current target has already been explored.
			 	*If we are seeing the same table name which we binded earlier
			 	*means we have cyclic references.
			 	*/
				if (dependentTables.add(currentTargetTableName))
				{
					cascadeDelete = true;
					int noDependents = fkTableNames.length;
//IC see: https://issues.apache.org/jira/browse/DERBY-2096
					dependentNodes = new StatementNode[noDependents];
					for(int i =0 ; i < noDependents ; i ++)
					{
						dependentNodes[i] = getDependentTableNode(
															  fkSchemaNames[i],
															  fkTableNames[i],
															  fkRefActions[i],
															  fkColDescriptors[i]);
						dependentNodes[i].bindStatement();
					}
				}
			}
			else
			{
				//case where current dependent table does not have dependent tables
				if(isDependentTable)
				{
					String currentTargetTableName = targetTableDescriptor.getSchemaName()
							 + "." + targetTableDescriptor.getName();
                    dependentTables.add(currentTargetTableName);
//IC see: https://issues.apache.org/jira/browse/DERBY-6075

				}
			}

            // add need for DELETE privilege on the target table
//IC see: https://issues.apache.org/jira/browse/DERBY-1330
//IC see: https://issues.apache.org/jira/browse/DERBY-6434
            getCompilerContext().pushCurrentPrivType( getPrivType());
            getCompilerContext().addRequiredTablePriv( targetTableDescriptor);
            getCompilerContext().popCurrentPrivType();
		}
		finally
		{
			getCompilerContext().popCurrentPrivType();
		}
	} // end of bind

    @Override
	int getPrivType()
	{
		return Authorizer.DELETE_PRIV;
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If delete table is on a SESSION schema table, then return true. 
		return resultSet.referencesSessionSchema();
	}

	/**
	 * Compile constants that Execution will use
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{

		/* Different constant actions for base tables and updatable VTIs */
		if (targetTableDescriptor != null)
		{
			// Base table
            int lckMode = resultSet.updateTargetLockMode();
			long heapConglomId = targetTableDescriptor.getHeapConglomerateId();
			TransactionController tc = getLanguageConnectionContext().getTransactionCompile();
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
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                lckMode = TransactionController.MODE_TABLE;
			}

			ResultDescription resultDescription = null;
			if(isDependentTable)
			{
				//triggers need the result description ,
				//dependent tables  don't have a source from generation time
				//to get the result description
				resultDescription = makeResultDescription();
			}


			return	getGenericConstantActionFactory().getDeleteConstantAction
				( heapConglomId,
				  targetTableDescriptor.getTableType(),
				  tc.getStaticCompiledConglomInfo(heapConglomId),
				  indicesToMaintain,
				  indexConglomerateNumbers,
				  indexSCOCIs,
				  deferred,
				  false,
				  targetTableDescriptor.getUUID(),
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                  lckMode,
				  null, null, null, 0, null, null, 
				  resultDescription,
				  getFKInfo(), 
				  getTriggerInfo(), 
				  (readColsBitSet == null) ? (FormatableBitSet)null : new FormatableBitSet(readColsBitSet),
				  getReadColMap(targetTableDescriptor.getNumberOfColumns(),readColsBitSet),
				  resultColumnList.getStreamStorableColIds(targetTableDescriptor.getNumberOfColumns()),
 				  (readColsBitSet == null) ? 
					  targetTableDescriptor.getNumberOfColumns() :
					  readColsBitSet.getNumBitsSet(),			
				  (UUID) null,
				  resultSet.isOneRowResultSet(),
				  dependentConstantActions,
                  inMatchingClause());
		}
		else
		{
			/* Return constant action for VTI
			 * NOTE: ConstantAction responsible for preserving instantiated
			 * VTIs for in-memory queries and for only preserving VTIs
			 * that implement Serializable for SPSs.
			 */
			return	getGenericConstantActionFactory().getUpdatableVTIConstantAction( DeferModification.DELETE_STATEMENT,
						deferred);
		}
	}

	/**
	 * Code generation for delete.
	 * The generated code will contain:
	 *		o  A static member for the (xxx)ResultSet with the RowLocations
	 *		o  The static member will be assigned the appropriate ResultSet within
	 *		   the nested calls to get the ResultSets.  (The appropriate cast to the
	 *		   (xxx)ResultSet will be generated.)
	 *		o  The CurrentRowLocation() in SelectNode's select list will generate
	 *		   a new method for returning the RowLocation as well as a call to
	 *		   that method which will be stuffed in the call to the 
	 *		    ProjectRestrictResultSet.
	 *      o In case of referential actions, this function generate an
	 *        array of resultsets on its dependent tables.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	The execute() method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		// If the DML is on the temporary table, generate the code to
		// mark temporary table as modified in the current UOW. After
		// DERBY-827 this must be done in execute() since
		// createResultSet() will only be called once.
		generateCodeForTemporaryTable(acb);
//IC see: https://issues.apache.org/jira/browse/DERBY-5947

		/* generate the parameters */
		if(!isDependentTable)
			generateParameterValueSet(acb);

		acb.pushGetResultSetFactoryExpression(mb); 
		acb.newRowLocationScanResultSetName();

        // arg 1
        if ( inMatchingClause() )
        {
            matchingClause.generateResultSetField( acb, mb );
        }
        else
        {
            resultSet.generate( acb, mb );
        }

		String resultSetGetter;
		int argCount;
		String parentResultSetId;

		// Base table
		if (targetTableDescriptor != null)
		{
			/* Create the declaration for the scan ResultSet which generates the
			 * RowLocations to be deleted.
	 		 * Note that the field cannot be static because there
			 * can be multiple activations of the same activation class,
			 * and they can't share this field.  Only exprN fields can
			 * be shared (or, more generally, read-only fields).
			 * RESOLVE - Need to deal with the type of the field.
			 */

			acb.newFieldDeclaration(Modifier.PRIVATE, 
									ClassName.CursorResultSet, 
									acb.getRowLocationScanResultSetName());

			if(cascadeDelete || isDependentTable)
			{
				resultSetGetter = "getDeleteCascadeResultSet";
				argCount = 4;
			}		
			else
			{
				resultSetGetter = "getDeleteResultSet";
				argCount = 1;
			}
			
		} else {
			argCount = 1;
			resultSetGetter = "getDeleteVTIResultSet";
		}

		if(isDependentTable)
		{
			mb.push(acb.addItem(makeConstantAction()));
		
		}else
		{
			if(cascadeDelete)
			{
				mb.push(-1); //root table.
			}
		}		

		String		resultSetArrayType = ClassName.ResultSet + "[]";
		if(cascadeDelete)
		{
			parentResultSetId = targetTableDescriptor.getSchemaName() +
			                       "." + targetTableDescriptor.getName();
			// Generate the code to build the array
			LocalField arrayField =
				acb.newFieldDeclaration(Modifier.PRIVATE, resultSetArrayType);
			mb.pushNewArray(ClassName.ResultSet, dependentNodes.length);  // new ResultSet[size]
//IC see: https://issues.apache.org/jira/browse/DERBY-176
			mb.setField(arrayField);
			for(int index=0 ; index <  dependentNodes.length ; index++)
			{
				dependentNodes[index].setRefActionInfo(fkIndexConglomNumbers[index],
													   fkColArrays[index],
													   parentResultSetId,
													   true);
				mb.getField(arrayField); // first arg (resultset array reference)
				/*beetle:5360 : if too many statements are added  to a  method, 
				 *size of method can hit  65k limit, which will
				 *lead to the class format errors at load time.
				 *To avoid this problem, when number of statements added 
				 *to a method is > 2048, remaing statements are added to  a new function
				 *and called from the function which created the function.
				 *See Beetle 5135 or 4293 for further details on this type of problem.
				*/
				if(mb.statementNumHitLimit(10))
				{
					MethodBuilder dmb = acb.newGeneratedFun(ClassName.ResultSet, Modifier.PRIVATE);
					dependentNodes[index].generate(acb,dmb); //generates the resultset expression
					dmb.methodReturn();
					dmb.complete();
					/* Generate the call to the new method */
					mb.pushThis(); 
					//second arg will be generated by this call
					mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, dmb.getName(), ClassName.ResultSet, 0);
				}else
				{
					dependentNodes[index].generate(acb,mb); //generates the resultset expression
				}

				mb.setArrayElement(index);
			}	
			mb.getField(arrayField); // fourth argument - array reference
		}
		else
		{
			if(isDependentTable)
			{
				mb.pushNull(resultSetArrayType); //No dependent tables for this table
			}
		}


		if(cascadeDelete || isDependentTable)
		{
			parentResultSetId = targetTableDescriptor.getSchemaName() +
			                       "." + targetTableDescriptor.getName();
			mb.push(parentResultSetId);

		}
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, resultSetGetter, ClassName.ResultSet, argCount);


		if(!isDependentTable && cascadeDelete)
		{
			int numResultSets = acb.getRowCount();
			if(numResultSets > 0)
			{
				//generate activation.raParentResultSets = new NoPutResultSet[size]
				MethodBuilder constructor = acb.getConstructor();
				constructor.pushThis();
				constructor.pushNewArray(ClassName.CursorResultSet, numResultSets);
				constructor.putField(ClassName.BaseActivation,
									 "raParentResultSets",
									 ClassName.CursorResultSet + "[]");
				constructor.endStatement();
			}
		}
	}


	/**
	 * Return the type of statement, something from
	 * StatementType.
	 *
	 * @return the type of statement
	 */
    @Override
	protected final int getStatementType()
	{
		return StatementType.DELETE;
	}


	/**
	  *	Gets the map of all columns which must be read out of the base table.
	  * These are the columns needed to:
	  *
	  *		o	maintain indices
	  *		o	maintain foreign keys
	  *
	  *	The returned map is a FormatableBitSet with 1 bit for each column in the
	  * table plus an extra, unsued 0-bit. If a 1-based column id must
	  * be read from the base table, then the corresponding 1-based bit
	  * is turned ON in the returned FormatableBitSet.
	  *
	  *	@param	dd				the data dictionary to look in
	  *	@param	baseTable		the base table descriptor
	  *
	  *	@return	a FormatableBitSet of columns to be read out of the base table
	  *
	  * @exception StandardException		Thrown on error
	  */
	public	FormatableBitSet	getReadMap
	(
		DataDictionary		dd,
		TableDescriptor		baseTable
	)
		throws StandardException
	{
		boolean[]	needsDeferredProcessing = new boolean[1];
		needsDeferredProcessing[0] = requiresDeferredProcessing();

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        ArrayList<ConglomerateDescriptor> conglomerates = new ArrayList<ConglomerateDescriptor>();
        relevantTriggers = new TriggerDescriptorList();
//IC see: https://issues.apache.org/jira/browse/DERBY-673

//IC see: https://issues.apache.org/jira/browse/DERBY-6075
        FormatableBitSet columnMap = DeleteNode.getDeleteReadMap(baseTable,
                conglomerates, relevantTriggers, needsDeferredProcessing);

        markAffectedIndexes(conglomerates);

		adjustDeferredFlag( needsDeferredProcessing[0] );

		return	columnMap;
	}

	/**
	 * In case of referential actions, we require to perform
	 * DML (UPDATE or DELETE) on the dependent tables. 
	 * Following function returns the DML Node for the dependent table.
	 */
	private StatementNode getDependentTableNode(String schemaName, String tableName, int refAction,
												ColumnDescriptorList cdl) throws StandardException
	{
        DMLModStatementNode node = null;
//IC see: https://issues.apache.org/jira/browse/DERBY-6075

		if(refAction == StatementType.RA_CASCADE)
		{
			node = getEmptyDeleteNode(schemaName , tableName);
		}

		if(refAction == StatementType.RA_SETNULL)
		{
			node = getEmptyUpdateNode(schemaName , tableName, cdl);
		}

        // The dependent node should be marked as such, and it should inherit
        // the set of dependent tables from the parent so that it can break
        // out of cycles in the dependency graph.
//IC see: https://issues.apache.org/jira/browse/DERBY-6075
        if (node != null) {
            node.isDependentTable = true;
            node.dependentTables = dependentTables;
        }

		return node;
	}


    private DeleteNode getEmptyDeleteNode(String schemaName, String targetTableName)
        throws StandardException
    {
        ValueNode whereClause = null;

        TableName tableName =
            new TableName(schemaName , targetTableName, getContextManager());

        FromList fromList = new FromList(getContextManager());

        FromTable fromTable = new FromBaseTable(
                tableName,
                null,
                FromBaseTable.DELETE,
                null,
                getContextManager());

		//we would like to use references index & table scan instead of 
		//what optimizer says for the dependent table scan.
		Properties targetProperties = new FormatableProperties();
		targetProperties.put("index", "null");
		((FromBaseTable) fromTable).setTableProperties(targetProperties);

        fromList.addFromTable(fromTable);
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        SelectNode rs = new SelectNode(null,
                                       fromList, /* FROM list */
                                       whereClause, /* WHERE clause */
                                       null, /* GROUP BY list */
                                       null, /* having clause */
                                       null, /* windows */
                                       null, /* optimizer override plan */
                                       getContextManager());

        return new DeleteNode(tableName, rs, null, getContextManager());
    }


	
    private UpdateNode getEmptyUpdateNode(String schemaName,
											 String targetTableName,
											 ColumnDescriptorList cdl)
        throws StandardException
    {

        ValueNode whereClause = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        TableName tableName =
            new TableName(schemaName , targetTableName, getContextManager());

        FromList fromList = new FromList(getContextManager());

        FromTable fromTable = new FromBaseTable(
                tableName,
                null,
//IC see: https://issues.apache.org/jira/browse/DERBY-6885
                FromBaseTable.DELETE,
                null,
                getContextManager());


		//we would like to use references index & table scan instead of 
		//what optimizer says for the dependent table scan.
		Properties targetProperties = new FormatableProperties();
		targetProperties.put("index", "null");
		((FromBaseTable) fromTable).setTableProperties(targetProperties);

        fromList.addFromTable(fromTable);

//IC see: https://issues.apache.org/jira/browse/DERBY-673
        SelectNode sn = new SelectNode(getSetClause(cdl),
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                                              fromList, /* FROM list */
                                              whereClause, /* WHERE clause */
                                              null, /* GROUP BY list */
//IC see: https://issues.apache.org/jira/browse/DERBY-3634
//IC see: https://issues.apache.org/jira/browse/DERBY-4069
                                              null, /* having clause */
//IC see: https://issues.apache.org/jira/browse/DERBY-3634
//IC see: https://issues.apache.org/jira/browse/DERBY-4069
                                              null, /* windows */
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
                                              null, /* optimizer override plan */
                                              getContextManager());

        return new UpdateNode(tableName, sn, null, getContextManager());
    }


 
    private ResultColumnList getSetClause(ColumnDescriptorList cdl)
		throws StandardException
	{
		ResultColumn resultColumn;
		ValueNode	 valueNode;

        ResultColumnList columnList = new ResultColumnList(getContextManager());

        valueNode = new UntypedNullConstantNode(getContextManager());
		for(int index =0 ; index < cdl.size() ; index++)
		{
            ColumnDescriptor cd = cdl.elementAt(index);
			//only columns that are nullable need to be set to 'null' for ON
			//DELETE SET NULL
			if((cd.getType()).isNullable())
			{
                resultColumn =
                        new ResultColumn(cd, valueNode, getContextManager());

				columnList.addResultColumn(resultColumn);
			}
		}
		return columnList;
	}

    @Override
	public void optimizeStatement() throws StandardException
	{
        // Don't add any more permissions during pre-processing
        IgnoreFilter    ignorePermissions = new IgnoreFilter();
        getCompilerContext().addPrivilegeFilter( ignorePermissions );
        
		if(cascadeDelete)
		{
			for(int index=0 ; index < dependentNodes.length ; index++)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-2096
				dependentNodes[index].optimizeStatement();
			}
		}

        super.optimizeStatement();
//IC see: https://issues.apache.org/jira/browse/DERBY-2096

        // allow more permissions to be added in case we're just one action
        // of a MERGE statement
        getCompilerContext().removePrivilegeFilter( ignorePermissions );
	}

    /**
	  *	Builds a bitmap of all columns which should be read from the
	  *	Store in order to satisfy an DELETE statement.
	  *
	  *
	  *	1)	finds all indices on this table
	  *	2)	adds the index columns to a bitmap of affected columns
	  *	3)	adds the index descriptors to a list of conglomerate
	  *		descriptors.
	  *	4)	finds all DELETE triggers on the table
	  *	5)	if there are any DELETE triggers, then do one of the following
	  *     a)If all of the triggers have MISSING referencing clause, then that
	  *      means that the trigger actions do not have access to before and
	  *      after values. In that case, there is no need to blanketly decide 
	  *      to include all the columns in the read map just because there are
	  *      triggers defined on the table.
	  *     b)Since one/more triggers have REFERENCING clause on them, get all
	  *      the columns because we don't know what the user will ultimately 
	  *      reference.
	  *	6)	adds the triggers to an evolving list of triggers
	  *
      * @param  conglomerates       OUT: list of affected indices
	  *	@param	relevantTriggers	IN/OUT. Passed in as an empty list. Filled in as we go.
	  *	@param	needsDeferredProcessing			IN/OUT. true if the statement already needs
	  *											deferred processing. set while evaluating this
	  *											routine if a trigger requires
	  *											deferred processing
	  *
	  * @return a FormatableBitSet of columns to be read out of the base table
	  *
	  * @exception StandardException		Thrown on error
	  */
	private static FormatableBitSet getDeleteReadMap
	(
		TableDescriptor				baseTable,
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        List<ConglomerateDescriptor>  conglomerates,
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        TriggerDescriptorList       relevantTriggers,
		boolean[]					needsDeferredProcessing
	)
		throws StandardException
	{
		int		columnCount = baseTable.getMaxColumnID();
		FormatableBitSet	columnMap = new FormatableBitSet(columnCount + 1);

		/* 
		** Get a list of the indexes that need to be 
		** updated.  ColumnMap contains all indexed
		** columns where 1 or more columns in the index
		** are going to be modified.
		**
		** Notice that we don't need to add constraint
		** columns.  This is because we add all key constraints
		** (e.g. foreign keys) as a side effect of adding their
		** indexes above.  And we don't need to deal with
		** check constraints on a delete.
		**
		** Adding indexes also takes care of the replication 
		** requirement of having the primary key.
		*/
//IC see: https://issues.apache.org/jira/browse/DERBY-6075
        DMLModStatementNode.getXAffectedIndexes(
                baseTable, null, columnMap, conglomerates);

		/*
	 	** If we have any DELETE triggers, then do one of the following
	 	** 1)If all of the triggers have MISSING referencing clause, then that
	 	** means that the trigger actions do not have access to before and 
	 	** after values. In that case, there is no need to blanketly decide to
	 	** include all the columns in the read map just because there are
	 	** triggers defined on the table.
	 	** 2)Since one/more triggers have REFERENCING clause on them, get all 
	 	** the columns because we don't know what the user will ultimately reference.
	 	*/
		baseTable.getAllRelevantTriggers( StatementType.DELETE, (int[])null, relevantTriggers );

		if (relevantTriggers.size() > 0)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-4538
			needsDeferredProcessing[0] = true;
			
			boolean needToIncludeAllColumns = false;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
            for (TriggerDescriptor trd : relevantTriggers) {
				//Does this trigger have REFERENCING clause defined on it.
				//If yes, then read all the columns from the trigger table.
				if (!trd.getReferencingNew() && !trd.getReferencingOld())
					continue;
				else
				{
					needToIncludeAllColumns = true;
					break;
				}
			}

			if (needToIncludeAllColumns) {
				for (int i = 1; i <= columnCount; i++)
				{
					columnMap.set(i);
				}
			}
		}

		return	columnMap;
	}
    
	/*
	 * Force column references (particularly those added by the compiler)
	 * to use the correlation name on the base table, if any.
	 */
	private	void	correlateAddedColumns( ResultColumnList rcl, FromTable fromTable )
		throws StandardException
	{
		String		correlationName = fromTable.getCorrelationName();

		if ( correlationName == null ) { return; }

		TableName	correlationNameNode = makeTableName( null, correlationName );

//IC see: https://issues.apache.org/jira/browse/DERBY-673
        for (ResultColumn column : rcl)
		{
			ValueNode		expression = column.getExpression();

			if ( (expression != null) && (expression instanceof ColumnReference) )
			{
				ColumnReference	reference = (ColumnReference) expression;
				
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
				reference.setQualifiedTableName( correlationNameNode );
			}
		}
		
	}
	
}
