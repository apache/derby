/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateViewNode

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

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.NodeFactory;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.depend.ProviderList;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.DB2Limit;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.catalog.UUID;

/**
 * A CreateViewNode is the root of a QueryTree that represents a CREATE VIEW
 * statement.
 *
 * @author Jerry Brenner
 */

public class CreateViewNode extends CreateStatementNode
{
	Dependent			currentDependent;
	ResultColumnList	resultColumns;
	ResultSetNode		queryExpression;
	String				qeText;
	int					checkOption;
	ProviderInfo[]		providerInfos;
	ColumnInfo[]		colInfos;


	/**
	 * Initializer for a CreateViewNode
	 *
	 * @param newObjectName		The name of the table to be created
	 * @param resultColumns		The column list from the view definition, 
	 *							if specified
	 * @param queryExpression	The query expression for the view
	 * @param checkOption		The type of WITH CHECK OPTION that was specified
	 *							(NONE for now)
	 * @param qeText			The text for the queryExpression
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(Object newObjectName,
				   Object resultColumns,
				   Object	 queryExpression,
				   Object checkOption,
				   Object qeText)
		throws StandardException
	{
		initAndCheck(newObjectName);
		this.resultColumns = (ResultColumnList) resultColumns;
		this.queryExpression = (ResultSetNode) queryExpression;
		this.checkOption = ((Integer) checkOption).intValue();
		this.qeText = ((String) qeText).trim();

		implicitCreateSchema = true;
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
			return super.toString() +
				"checkOption: " + checkOption + "\n" +
				"qeText: " + qeText + "\n";
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "CREATE VIEW";
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

			if (resultColumns != null)
			{
				printLabel(depth, "resultColumns: ");
				resultColumns.treePrint(depth + 1);
			}

			printLabel(depth, "queryExpression: ");
			queryExpression.treePrint(depth + 1);
		}
	}

	// accessors

	public	int				getCheckOption() { return checkOption; }

	public	ProviderInfo[]	getProviderInfo() { return providerInfos; }

	public	ColumnInfo[]	getColumnInfo() { return colInfos; }

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateViewNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the ResultColumnList does not
	 * contain any duplicate column names.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		CompilerContext				cc = getCompilerContext();
		DataDictionary				dataDictionary = getDataDictionary();
		ResultColumnList			qeRCL;
		String						duplicateColName;

		// bind the query expression

		providerInfos = bindViewDefinition
			( dataDictionary, cc, getLanguageConnectionContext(),
			  getNodeFactory(), 
			  queryExpression,
			  getContextManager()
			);

		qeRCL = queryExpression.getResultColumns();

		/* If there is an RCL for the view definition then
		 * copy the names to the queryExpression's RCL after verifying
		 * that they both have the same size.
		 */
		if (resultColumns != null)
		{
			if (resultColumns.size() != qeRCL.size())
			{
				throw StandardException.newException(SQLState.LANG_VIEW_DEFINITION_R_C_L_MISMATCH,
								getFullName());
			}
			qeRCL.copyResultColumnNames(resultColumns);
		}

		/* Check to make sure the queryExpression's RCL has unique names. If target column
		 * names not specified, raise error if there are any un-named columns to match DB2
		 */
		duplicateColName = qeRCL.verifyUniqueNames((resultColumns == null) ? true : false);
		if (duplicateColName != null)
		{
			throw StandardException.newException(SQLState.LANG_DUPLICATE_COLUMN_NAME_CREATE_VIEW, duplicateColName);
		}

		/* Remember the current Dependent (the statement) here (since we
		 * already have the CompilerContext).  We will use this info
		 * during generate() to create the dependencies for the view.
		 */
		currentDependent = cc.getCurrentDependent();


		/* Only 5000 columns allowed per view */
		if (queryExpression.getResultColumns().size() > DB2Limit.DB2_MAX_COLUMNS_IN_VIEW)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
				String.valueOf(queryExpression.getResultColumns().size()),
				getRelativeName(),
				String.valueOf(DB2Limit.DB2_MAX_COLUMNS_IN_VIEW));
		}

		// for each column, stuff system.column
		colInfos = new ColumnInfo[queryExpression.getResultColumns().size()];
		genColumnInfos(colInfos);

		return this;
	}

	/**
	 * Bind the query expression for a view definition. 
	 *
	 * @param dataDictionary	The DataDictionary to use to look up
	 *				columns, tables, etc.
	 *
	 * @return	Array of providers that this view depends on.
	 *
	 * @exception StandardException		Thrown on error
	 */

	private ProviderInfo[] bindViewDefinition( DataDictionary 	dataDictionary,
											 CompilerContext	compilerContext,
											 LanguageConnectionContext lcc,
											 NodeFactory		nodeFactory,
											 ResultSetNode		queryExpr,
											 ContextManager		cm)
		throws StandardException
	{
		FromList	fromList = (FromList) nodeFactory.getNode(
										C_NodeTypes.FROM_LIST,
										nodeFactory.doJoinOrderOptimization(),
										cm);

		ProviderList 	prevAPL = compilerContext.getCurrentAuxiliaryProviderList();
		ProviderList 	apl = new ProviderList();

		try {
			compilerContext.setCurrentAuxiliaryProviderList(apl);

			/* Bind the tables in the queryExpression */
			queryExpr = queryExpr.bindNonVTITables(dataDictionary, fromList);
			queryExpr = queryExpr.bindVTITables(fromList);

			/* Bind the expressions under the resultSet */
			queryExpr.bindExpressions(fromList);

			//cannot define views on temporary tables
			if (queryExpr instanceof SelectNode)
			{
				//If attempting to reference a SESSION schema table (temporary or permanent) in the view, throw an exception
				if (queryExpr.referencesSessionSchema())
					throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
			}

			// bind the query expression
			queryExpr.bindResultColumns(fromList);
		}
		finally
		{
			compilerContext.setCurrentAuxiliaryProviderList(prevAPL);
		}

		DependencyManager 		dm = dataDictionary.getDependencyManager();
		ProviderInfo[]			providerInfos = dm.getPersistentProviderInfos(apl);
		// need to clear the column info in case the same table descriptor
		// is reused, eg., in multiple target only view definition
		dm.clearColumnInfoInProviders(apl);

		/* Verify that all underlying ResultSets reclaimed their FromList */
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(fromList.size() == 0,
				"fromList.size() is expected to be 0, not " + fromList.size() +
				" on return from RS.bindExpressions()");
		}

		return providerInfos;
	}

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
		//If create view is part of create statement and the view references SESSION schema tables, then it will
		//get caught in the bind phase of the view and exception will be thrown by the view bind. 
		return (queryExpression.referencesSessionSchema());
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		/* RESOLVE - need to build up dependendencies and store them away through
		 * the constant action.
		 */
		return	getGenericConstantActionFactory().getCreateViewConstantAction(getSchemaDescriptor().getSchemaName(),
											  getRelativeName(),
											  TableDescriptor.VIEW_TYPE,
											  qeText,
											  checkOption,
											  colInfos,
											  providerInfos,
											  (UUID)null); 	// compilation schema, filled
															// in when we create the view
	}

	/**
	 * Fill in the ColumnInfo[] for this create view.
	 * 
	 * @param colInfos	The ColumnInfo[] to be filled in.
	 *
	 * @return Nothing.
	 */
	private void genColumnInfos(ColumnInfo[] colInfos)
	{
		ResultColumnList rcl = 	queryExpression.getResultColumns();
		int			 	 rclSize = rcl.size();

		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn rc = (ResultColumn) rcl.elementAt(index);

			//RESOLVEAUTOINCREMENT
			colInfos[index] = new ColumnInfo(rc.getName(),
											 rc.getType(),
											 null,
											 null,
											 null,
											 null,
											 ColumnInfo.CREATE,
											 0, 0);
		}
	}

	/*
	 * class interface
	 */

	/**
	  *	Get the parsed query expression (the SELECT statement).
	  *
	  *	@return	the parsed query expression.
	  */
	public	ResultSetNode	getParsedQueryExpression() { return queryExpression; }

	/**
	  *	Get the bound result column list.
	  *
	  *	@return	the bound result column list.
	  */
	public	ResultColumnList	getBoundResultColumnList()
	{
		return queryExpression.getResultColumns();
	}


	/*
	 * These methods are used by execution
	 * to get information for storing into
	 * the system catalogs.
	 */


	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public Visitable accept(Visitor v) 
		throws StandardException
	{
		Visitable returnNode = v.visit(this);

		if (v.skipChildren(this))
		{
			return returnNode;
		}

		if (!v.stopTraversal())
		{
			super.accept(v);
		}

		if (queryExpression != null && !v.stopTraversal())
		{
			queryExpression = (ResultSetNode)queryExpression.accept(v);
		}

		return returnNode;
	}

}
