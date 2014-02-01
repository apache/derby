/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateViewNode

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

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.OptimizerFactory;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.impl.sql.execute.ColumnInfo;

/**
 * A CreateViewNode is the root of a QueryTree that represents a CREATE VIEW
 * statement.
 *
 */

class CreateViewNode extends DDLStatementNode
{
    private ResultColumnList resultColumns;
    private ResultSetNode    queryExpression;
    private String           qeText;
    private int              checkOption;
    private ProviderInfo[]   providerInfos;
    private ColumnInfo[]     colInfos;
	private OrderByList orderByList;
    private ValueNode   offset;
    private ValueNode   fetchFirst;
    private boolean hasJDBClimitClause; // true if using JDBC limit/offset escape syntax

	/**
     * Constructor for a CreateViewNode
	 *
     * @param viewName          The name of the table to be created
	 * @param resultColumns		The column list from the view definition, 
	 *							if specified
	 * @param queryExpression	The query expression for the view
	 * @param checkOption		The type of WITH CHECK OPTION that was specified
	 *							(NONE for now)
	 * @param qeText			The text for the queryExpression
	 * @param orderCols         ORDER BY list
     * @param offset            OFFSET if any, or null
     * @param fetchFirst        FETCH FIRST if any, or null
	 * @param hasJDBClimitClause True if the offset/fetchFirst clauses come from JDBC limit/offset escape syntax
     * @param cm                Context manager
	 * @exception StandardException		Thrown on error
	 */
    CreateViewNode(TableName viewName,
                   ResultColumnList resultColumns,
                   ResultSetNode queryExpression,
                   int checkOption,
                   String qeText,
                   OrderByList orderCols,
                   ValueNode offset,
                   ValueNode fetchFirst,
                   boolean hasJDBClimitClause,
                   ContextManager cm)
		throws StandardException
	{
        super(viewName, cm);
        this.resultColumns = resultColumns;
        this.queryExpression = queryExpression;
        this.checkOption = checkOption;
        this.qeText = qeText.trim();
        this.orderByList = orderCols;
        this.offset = offset;
        this.fetchFirst = fetchFirst;
        this.hasJDBClimitClause = hasJDBClimitClause;
        this.implicitCreateSchema = true;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
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

    String statementToString()
	{
		return "CREATE VIEW";
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
    void printSubNodes(int depth)
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

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateViewNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the ResultColumnList does not
	 * contain any duplicate column names.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	public void bindStatement() throws StandardException
	{
		CompilerContext				cc = getCompilerContext();
		DataDictionary				dataDictionary = getDataDictionary();
		ResultColumnList			qeRCL;
		String						duplicateColName;

		// bind the query expression

		providerInfos = bindViewDefinition
			( dataDictionary, cc, getLanguageConnectionContext(),
              getOptimizerFactory(),
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
			if (resultColumns.size() != qeRCL.visibleSize())
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

		/* Only 5000 columns allowed per view */
		if (queryExpression.getResultColumns().size() > Limits.DB2_MAX_COLUMNS_IN_VIEW)
		{
			throw StandardException.newException(SQLState.LANG_TOO_MANY_COLUMNS_IN_TABLE_OR_VIEW,
				String.valueOf(queryExpression.getResultColumns().size()),
				getRelativeName(),
				String.valueOf(Limits.DB2_MAX_COLUMNS_IN_VIEW));
		}

		// for each column, stuff system.column
		// System columns should only include visible columns DERBY-4230
		colInfos = new ColumnInfo[queryExpression.getResultColumns().visibleSize()];
		genColumnInfos(colInfos);
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

    private ProviderInfo[] bindViewDefinition(
        DataDictionary      dataDictionary,
        CompilerContext     compilerContext,
        LanguageConnectionContext lcc,
        OptimizerFactory    optimizerFactory,
        ResultSetNode       queryExpr,
        ContextManager      cm) throws StandardException
	{
        FromList fromList =
                new FromList(optimizerFactory.doJoinOrderOptimization(), cm);

		ProviderList 	prevAPL = compilerContext.getCurrentAuxiliaryProviderList();
		ProviderList 	apl = new ProviderList();

		try {
			compilerContext.setCurrentAuxiliaryProviderList(apl);
			compilerContext.pushCurrentPrivType(Authorizer.SELECT_PRIV);

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
			
			// rejects any untyped nulls in the RCL
			// e.g.:  CREATE VIEW v1 AS VALUES NULL
			queryExpr.bindUntypedNullsToResultColumns(null);
		}
		finally
		{
			compilerContext.popCurrentPrivType();
			compilerContext.setCurrentAuxiliaryProviderList(prevAPL);
		}

		DependencyManager 		dm = dataDictionary.getDependencyManager();
        ProviderInfo[]          provInfo = dm.getPersistentProviderInfos(apl);
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

        return provInfo;
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
		//If create view is part of create statement and the view references SESSION schema tables, then it will
		//get caught in the bind phase of the view and exception will be thrown by the view bind. 
		return (queryExpression.referencesSessionSchema());
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
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
	 */
	private void genColumnInfos(ColumnInfo[] colInfos)
	{
		ResultColumnList rcl = 	queryExpression.getResultColumns();

		for (int index = 0; index < colInfos.length; index++)
		{
            ResultColumn rc = rcl.elementAt(index);
			// The colInfo array has been initialized to be of length 
			// visibleSize() (DERBY-4230).  This code assumes that all the visible
			// columns are at the beginning of the rcl. Throw an assertion 
			// if we hit a generated column in what we think is the visible
			// range.
			if (SanityManager.DEBUG) {
				if (rc.isGenerated())
					SanityManager.THROWASSERT("Encountered generated column in expected visible range at rcl[" + index +"]");
			}
			//RESOLVEAUTOINCREMENT
			colInfos[index] = new ColumnInfo(rc.getName(),
											 rc.getType(),
											 null,
											 null,
											 null,
											 null,
											 null,
											 ColumnInfo.CREATE,
											 0, 0, 0);
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
	ResultSetNode	getParsedQueryExpression() { return queryExpression; }


	/*
	 * These methods are used by execution
	 * to get information for storing into
	 * the system catalogs.
	 */


	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

		if (queryExpression != null)
		{
			queryExpression = (ResultSetNode)queryExpression.accept(v);
		}
	}

    public OrderByList getOrderByList() {
        return orderByList;
    }

    public ValueNode getOffset() {
        return offset;
    }

    public ValueNode getFetchFirst() {
        return fetchFirst;
    }
    
    public boolean hasJDBClimitClause() { return hasJDBClimitClause; }

}
