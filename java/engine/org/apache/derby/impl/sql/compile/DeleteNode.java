/*

   Derby - Class org.apache.derby.impl.sql.compile.DeleteNode

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

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;


import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.vti.DeferModification;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.impl.sql.execute.DeleteConstantAction;
import org.apache.derby.impl.sql.execute.FKInfo;

import java.lang.reflect.Modifier;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.io.FormatableProperties;
import java.util.Vector;
import java.util.Hashtable;
import java.util.Properties;
import org.apache.derby.iapi.sql.compile.NodeFactory;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.services.compiler.LocalField;


/**
 * A DeleteNode represents a DELETE statement. It is the top-level node
 * for the statement.
 *
 * For positioned delete, there may be no from table specified.
 * The from table will be derived from the cursor specification of
 * the named cursor.
 *
 * @author Jeff Lichtman
 */

public class DeleteNode extends DMLModStatementNode
{
	/* Column name for the RowLocation column in the ResultSet */
	public static final String COLUMNNAME = "###RowLocationToDelete";

	/* Filled in by bind. */
	protected boolean				deferred;
	protected ExecRow				emptyHeapRow;
	protected FromTable				targetTable;
	protected FKInfo				fkInfo;
	protected FormatableBitSet readColsBitSet;

	private ConstantAction[] dependentConstantActions;
	private boolean cascadeDelete;
	private QueryTreeNode[] dependentNodes;

	/**
	 * Initializer for a DeleteNode.
	 *
	 * @param targetTableName	The name of the table to delete from
	 * @param queryExpresssion	The query expression that will generate
	 *				the rows to delete from the given table
	 */

	public void init(Object targetTableName,
					  Object queryExpression)
	{
		super.init(queryExpression);
		this.targetTableName = (TableName) targetTableName;
	}

	public String statementToString()
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
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		FromList					fromList =
								(FromList) getNodeFactory().getNode(
									C_NodeTypes.FROM_LIST,
									getNodeFactory().doJoinOrderOptimization(),
									getContextManager());
		ResultColumn				rowLocationColumn = null;
		CurrentRowLocationNode		rowLocationNode;
		TableName					cursorTargetTableName = null;
		CurrentOfNode       		currentOfNode = null;

                DataDictionary dataDictionary = getDataDictionary();
		super.bindTables(dataDictionary);

		// wait to bind named target table until the underlying
		// cursor is bound, so that we can get it from the
		// cursor if this is a positioned delete.

		// for positioned delete, get the cursor's target table.
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(resultSet != null && resultSet instanceof SelectNode,
			"Delete must have a select result set");

		SelectNode sel;
		sel = (SelectNode)resultSet;
		targetTable = (FromTable) sel.fromList.elementAt(0);
		if (targetTable instanceof CurrentOfNode)
		{
			currentOfNode = (CurrentOfNode) targetTable;

			cursorTargetTableName = currentOfNode.getBaseCursorTargetTableName();
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

			/* descriptor must exist, tables already bound.
			 * No need to do this for VTI as VTI was bound in
			 * super.bindTables() above.
			 */
			verifyTargetTable();
		}

		/* Generate a select list for the ResultSetNode - CurrentRowLocation(). */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT((resultSet.resultColumns == null),
							  "resultColumns is expected to be null until bind time");


		if (targetTable instanceof FromVTI)
		{
			getResultColumnList();
			resultColumnList = targetTable.getResultColumnsForList(null, resultColumnList, null);

			/* Set the new result column list in the result set */
			resultSet.setResultColumns(resultColumnList);
		}
		else
		{
			/*
			** Start off assuming no columns from the base table
			** are needed in the rcl.
			*/
			resultColumnList = new ResultColumnList();

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

			/*
			** Construct an empty heap row for use in our constant action.
			*/
			emptyHeapRow = targetTableDescriptor.getEmptyExecRow(getContextManager());

			/* Generate the RowLocation column */
			rowLocationNode = (CurrentRowLocationNode) getNodeFactory().getNode(
										C_NodeTypes.CURRENT_ROW_LOCATION_NODE,
										getContextManager());
			rowLocationColumn =
				(ResultColumn) getNodeFactory().getNode(
									C_NodeTypes.RESULT_COLUMN,
									COLUMNNAME,
									rowLocationNode,
									getContextManager());
			rowLocationColumn.markGenerated();

			/* Append to the ResultColumnList */
			resultColumnList.addResultColumn(rowLocationColumn);

			/* Set the new result column list in the result set */
			resultSet.setResultColumns(resultColumnList);
		}

		/* Bind the expressions before the ResultColumns are bound */
		super.bindExpressions();

		/* Bind untyped nulls directly under the result columns */
		resultSet.
			getResultColumns().
				bindUntypedNullsToResultColumns(resultColumnList);

		if (! (targetTable instanceof FromVTI))
		{
			/* Bind the new ResultColumn */
			rowLocationColumn.bindResultColumnToExpression();

			bindConstraints(dataDictionary,
							getNodeFactory(),
							targetTableDescriptor,
							null,
							resultColumnList,
							(int[]) null,
							readColsBitSet,
							false,
							true);  /* we alway include triggers in core language */

			/* If the target table is also a source table, then
			 * the delete will have to be in deferred mode
			 * For deletes, this means that the target table appears in a
			 * subquery.  Also, self-referencing foreign key deletes
		 	 * are deferred.  And triggers cause the delete to be deferred.
			 */
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
        sel = null; // done with sel

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
			String currentTargetTableName =
				targetTableDescriptor.getSchemaName() + "." + targetTableDescriptor.getName();

			if(!isDependentTable){
				//graph node
				graphHashTable = new Hashtable();
			}

			/*Check whether the current tatget is already been explored.
			 *If we are seeing the same table name which we binded earlier
			 *means we have cyclic references.
			 */
			if(!graphHashTable.containsKey(currentTargetTableName))
			{
				cascadeDelete = true;
				int noDependents = fkTableNames.length;
				dependentNodes = new QueryTreeNode[noDependents];
				graphHashTable.put(currentTargetTableName, new Integer(noDependents));
				for(int i =0 ; i < noDependents ; i ++)
				{
					dependentNodes[i] = getDependentTableNode(fkTableNames[i],
															  fkRefActions[i],
															  fkColDescriptors[i]);
					dependentNodes[i].bind();
				}
			}
		}else
		{
			//case where current dependent table does not have dependent tables
			if(isDependentTable)
			{
				String currentTargetTableName =
					targetTableDescriptor.getSchemaName() + "." + targetTableDescriptor.getName();
				graphHashTable.put(currentTargetTableName, new Integer(0));

			}

		}
		return this;
	} // end of bind

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
		//If delete table is on a SESSION schema table, then return true. 
		return resultSet.referencesSessionSchema();
	}

	/**
	 * Compile constants that Execution will use
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{

		/* Different constant actions for base tables and updatable VTIs */
		if (targetTableDescriptor != null)
		{
			// Base table
			int lockMode = resultSet.updateTargetLockMode();
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
				lockMode = TransactionController.MODE_TABLE;
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
				  emptyHeapRow,
				  deferred,
				  false,
				  targetTableDescriptor.getUUID(),
				  lockMode,
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
				  dependentConstantActions);
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
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{

		//If the DML is on the temporary table, generate the code to mark temporary table as modified in the current UOW
		generateCodeForTemporaryTable(acb, mb);

		/* generate the parameters */
		if(!isDependentTable)
			generateParameterValueSet(acb);

		acb.pushGetResultSetFactoryExpression(mb); 
		acb.newRowLocationScanResultSetName();
		resultSet.generate(acb, mb); // arg 1

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
			}		
			else
			{
				resultSetGetter = "getDeleteResultSet";
			}
			argCount = 2;
		} else {
			argCount = 2;
			resultSetGetter = "getDeleteVTIResultSet";
		}


		acb.pushThisAsActivation(mb);

		if(isDependentTable)
		{
			argCount = 3;
			mb.push(acb.addItem(makeConstantAction()));
		
		}else
		{
			if(cascadeDelete)
			{
				argCount = 3;
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
			mb.putField(arrayField);
			mb.endStatement();

			argCount = 4;
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
				argCount =4;
				mb.pushNull(resultSetArrayType); //No dependent tables for this table
			}
		}


		if(cascadeDelete || isDependentTable)
		{
			parentResultSetId = targetTableDescriptor.getSchemaName() +
			                       "." + targetTableDescriptor.getName();
			argCount = 5;
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

		Vector		conglomVector = new Vector();
		relevantTriggers = new GenericDescriptorList();

		FormatableBitSet	columnMap = DeleteNode.getDeleteReadMap(baseTable, conglomVector, relevantTriggers, needsDeferredProcessing );

		markAffectedIndexes( conglomVector );

		adjustDeferredFlag( needsDeferredProcessing[0] );

		return	columnMap;
	}

	/**
	 * In case of referential actions, we require to perform
	 * DML (UPDATE or DELETE) on the dependent tables. 
	 * Following function returns the DML Node for the dependent table.
	 */
	private QueryTreeNode getDependentTableNode(String tableName, int refAction,
												ColumnDescriptorList cdl) throws StandardException
	{
		QueryTreeNode node=null;

		int index = tableName.indexOf('.');
		String schemaName = tableName.substring(0 , index);
		String tName = tableName.substring(index+1);
		if(refAction == StatementType.RA_CASCADE)
		{
			node = getEmptyDeleteNode(schemaName , tName);
			((DeleteNode)node).isDependentTable = true;
			((DeleteNode)node).graphHashTable = graphHashTable;
		}

		if(refAction == StatementType.RA_SETNULL)
		{
			node = getEmptyUpdateNode(schemaName , tName, cdl);
			((UpdateNode)node).isDependentTable = true;
			((UpdateNode)node).graphHashTable = graphHashTable;
		}

		return node;
	}


    private QueryTreeNode getEmptyDeleteNode(String schemaName, String targetTableName)
        throws StandardException
    {

        ValueNode whereClause = null;
        TableName tableName = null;
        FromTable fromTable = null;
        QueryTreeNode retval;
        SelectNode resultSet;

        tableName = new TableName();
        tableName.init(schemaName , targetTableName);

        NodeFactory nodeFactory = getNodeFactory();
        FromList   fromList = (FromList) nodeFactory.getNode(C_NodeTypes.FROM_LIST, getContextManager());
        fromTable = (FromTable) nodeFactory.getNode(
                                                    C_NodeTypes.FROM_BASE_TABLE,
                                                    tableName,
                                                    null,
                                                    ReuseFactory.getInteger(FromBaseTable.DELETE),
                                                    null,
                                                    getContextManager());

		//we would like to use references index & table scan instead of 
		//what optimizer says for the dependent table scan.
		Properties targetProperties = new FormatableProperties();
		targetProperties.put("index", "null");
		((FromBaseTable) fromTable).setTableProperties(targetProperties);

        fromList.addFromTable(fromTable);
        resultSet = (SelectNode) nodeFactory.getNode(
                                                     C_NodeTypes.SELECT_NODE,
                                                     null,
                                                     null,   /* AGGREGATE list */
                                                     fromList, /* FROM list */
                                                     whereClause, /* WHERE clause */
                                                     null, /* GROUP BY list */
                                                     getContextManager());

        retval =(QueryTreeNode) nodeFactory.getNode(
                                                    C_NodeTypes.DELETE_NODE,
                                                    tableName,
                                                    resultSet,
                                                    getContextManager());

        return retval;
    }


	
    private QueryTreeNode getEmptyUpdateNode(String schemaName, 
											 String targetTableName,
											 ColumnDescriptorList cdl)
        throws StandardException
    {

        ValueNode whereClause = null;
        TableName tableName = null;
        FromTable fromTable = null;
        QueryTreeNode retval;
        SelectNode resultSet;

        tableName = new TableName();
        tableName.init(schemaName , targetTableName);

        NodeFactory nodeFactory = getNodeFactory();
        FromList   fromList = (FromList) nodeFactory.getNode(C_NodeTypes.FROM_LIST, getContextManager());
        fromTable = (FromTable) nodeFactory.getNode(
                                                    C_NodeTypes.FROM_BASE_TABLE,
                                                    tableName,
                                                    null,
                                                    ReuseFactory.getInteger(FromBaseTable.DELETE),
                                                    null,
                                                    getContextManager());


		//we would like to use references index & table scan instead of 
		//what optimizer says for the dependent table scan.
		Properties targetProperties = new FormatableProperties();
		targetProperties.put("index", "null");
		((FromBaseTable) fromTable).setTableProperties(targetProperties);

        fromList.addFromTable(fromTable);

		resultSet = (SelectNode) nodeFactory.getNode(
                                                     C_NodeTypes.SELECT_NODE,
                                                     getSetClause(tableName, cdl),
                                                     null,   /* AGGREGATE list */
                                                     fromList, /* FROM list */
                                                     whereClause, /* WHERE clause */
                                                     null, /* GROUP BY list */
                                                     getContextManager());

        retval =(QueryTreeNode) nodeFactory.getNode(
                                                    C_NodeTypes.UPDATE_NODE,
                                                    tableName,
                                                    resultSet,
                                                    getContextManager());

        return retval;
    }


 
	private ResultColumnList getSetClause(TableName tabName,
										  ColumnDescriptorList cdl)
		throws StandardException
	{
		ResultColumn resultColumn;
		ValueNode	 valueNode;

		NodeFactory nodeFactory = getNodeFactory();
		ResultColumnList	columnList = (ResultColumnList) nodeFactory.getNode(
												C_NodeTypes.RESULT_COLUMN_LIST,
												getContextManager());

		valueNode =  (ValueNode) nodeFactory.getNode(C_NodeTypes.UNTYPED_NULL_CONSTANT_NODE,
															 getContextManager());
		for(int index =0 ; index < cdl.size() ; index++)
		{
			ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);
			//only columns that are nullable need to be set to 'null' for ON
			//DELETE SET NULL
			if((cd.getType()).isNullable())
			{
				resultColumn = (ResultColumn) nodeFactory.getNode(
   									    C_NodeTypes.RESULT_COLUMN,
										cd,
										valueNode,
										getContextManager());

				columnList.addResultColumn(resultColumn);
			}
		}
		return columnList;
	}


	public QueryTreeNode optimize() throws StandardException
	{
		if(cascadeDelete)
		{
			for(int index=0 ; index < dependentNodes.length ; index++)
			{
				dependentNodes[index] =  dependentNodes[index].optimize();
			}
		}

		return super.optimize();
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
	  *	5)	if there are any DELETE triggers, marks all columns in the bitmap
	  *	6)	adds the triggers to an evolving list of triggers
	  *
	  *	@param	conglomVector		OUT: vector of affected indices
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
		Vector						conglomVector,
		GenericDescriptorList		relevantTriggers,
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
		DMLModStatementNode.getXAffectedIndexes(baseTable,  null, columnMap, conglomVector );

		/*
	 	** If we have any triggers, then get all the columns
		** because we don't know what the user will ultimately
		** reference.
	 	*/
		baseTable.getAllRelevantTriggers( StatementType.DELETE, (int[])null, relevantTriggers );
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
    
}
