/*

   Derby - Class org.apache.derby.impl.sql.compile.FromSubquery

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

import java.util.Properties;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.util.JBitSet;

/**
 * A FromSubquery represents a subquery in the FROM list of a DML statement.
 *
 * The current implementation of this class is only
 * sufficient for Insert's need to push a new
 * select on top of the one the user specified,
 * to make the selected structure match that
 * of the insert target table.
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class FromSubquery extends FromTable
{
	ResultSetNode	subquery;
	private OrderByList orderByList;
    private ValueNode offset;
    private ValueNode fetchFirst;
    private boolean hasJDBClimitClause; // true if using JDBC limit/offset escape syntax

	/**
	 * DERBY-3270: If this subquery represents an expanded view, this holds the
	 * current compilation schema at view definition time.
	 */
	private SchemaDescriptor origCompilationSchema = null;

	/**
     * Constructor for a table in a FROM list.
	 *
	 * @param subquery		The subquery
	 * @param orderByList   ORDER BY list if any, or null
     * @param offset        OFFSET if any, or null
     * @param fetchFirst    FETCH FIRST if any, or null
     * @param hasJDBClimitClause True if the offset/fetchFirst clauses come
     *                      from JDBC limit/offset escape syntax
	 * @param correlationName	The correlation name
	 * @param derivedRCL		The derived column list
	 * @param tableProperties	Properties list associated with the table
     * @param cm            The context manager
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    FromSubquery(ResultSetNode subquery,
                 OrderByList orderByList,
                 ValueNode offset,
                 ValueNode fetchFirst,
                 boolean hasJDBClimitClause,
                 String correlationName,
                 ResultColumnList derivedRCL,
                 Properties tableProperties,
                 ContextManager cm)
	{
        super(correlationName, tableProperties, cm);
        this.subquery = subquery;
        this.orderByList = orderByList;
        this.offset = offset;
        this.fetchFirst = fetchFirst;
        this.hasJDBClimitClause = hasJDBClimitClause;
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
        setResultColumns( derivedRCL );
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG) {
			super.printSubNodes(depth);

			if (subquery != null)
			{
				printLabel(depth, "subquery: ");
				subquery.treePrint(depth + 1);
			}

//IC see: https://issues.apache.org/jira/browse/DERBY-4397
//IC see: https://issues.apache.org/jira/browse/DERBY-4
//IC see: https://issues.apache.org/jira/browse/DERBY-4398
            if (orderByList != null)
            {
                printLabel(depth, "orderByList: ");
                orderByList.treePrint(depth + 1);
            }

            if (offset != null)
            {
                printLabel(depth, "offset: ");
                offset.treePrint(depth + 1);
            }

            if (fetchFirst != null)
            {
                printLabel(depth, "fetchFirst: ");
                fetchFirst.treePrint(depth + 1);
            }
        }
	}

	/** 
	 * Return the "subquery" from this node.
	 *
	 * @return ResultSetNode	The "subquery" from this node.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode getSubquery()
	{
		return subquery;
	}

	/** 
	 * Determine whether or not the specified name is an exposed name in
	 * the current query block.
	 *
	 * @param name	The specified name to search for as an exposed name.
	 * @param schemaName	Schema name, if non-null.
	 * @param exactMatch	Whether or not we need an exact match on specified schema and table
	 *						names or match on table id.
	 *
	 * @return The FromTable, if any, with the exposed name.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    FromTable getFromTableByName(String name, String schemaName, boolean exactMatch)
		throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-5005
        if (schemaName != null && origTableName != null) {
            // View can have schema
            if (!schemaName.equals(origTableName.schemaName)) {
                return null;
            }
            // So far, so good, now go on to compare table name
        }

        if (getExposedName().equals(name)) {
            return this;
        }

        return null;
	}

	/**
	 * Bind this subquery that appears in the FROM list.
	 *
	 * @param dataDictionary	The DataDictionary to use for binding
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode		The bound FromSubquery.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
						  FromList fromListParam) 
							throws StandardException
	{
		/* Assign the tableNumber */
		if (tableNumber == -1)  // allow re-bind, in which case use old number
			tableNumber = getCompilerContext().getNextTableNumber();

		subquery = subquery.bindNonVTITables(dataDictionary, fromListParam);

		return this;
	}

	/**
	 * Bind this subquery that appears in the FROM list.
	 *
	 * @param fromListParam		FromList to use/append to.
	 *
	 * @return	ResultSetNode		The bound FromSubquery.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode bindVTITables(FromList fromListParam)
							throws StandardException
	{
		subquery = subquery.bindVTITables(fromListParam);

		return this;
	}

	/**
	 * Check for (and reject) ? parameters directly under the ResultColumns.
	 * This is done for SELECT statements.  For FromSubquery, we
	 * simply pass the check through to the subquery.
	 *
	 * @exception StandardException		Thrown if a ? parameter found
	 *									directly under a ResultColumn
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void rejectParameters() throws StandardException
	{
		subquery.rejectParameters();
	}

	/**
	 * Bind the expressions in this FromSubquery.  This means 
	 * binding the sub-expressions, as well as figuring out what the return 
	 * type is for each expression.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void bindExpressions(FromList fromListParam)
					throws StandardException
	{
        FromList            emptyFromList = new FromList(
                getOptimizerFactory().doJoinOrderOptimization(),
                getContextManager());
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		ResultColumnList	derivedRCL = getResultColumns();
		ResultColumnList	subqueryRCL;
		FromList			nestedFromList;

		/* From subqueries cannot be correlated, so we pass an empty FromList
		 * to subquery.bindExpressions() and .bindResultColumns()
		 */
//IC see: https://issues.apache.org/jira/browse/DERBY-4397
//IC see: https://issues.apache.org/jira/browse/DERBY-4
		if (orderByList != null) {
			orderByList.pullUpOrderByColumns(subquery);
		}

		nestedFromList = emptyFromList;
//IC see: https://issues.apache.org/jira/browse/DERBY-2442

		CompilerContext compilerContext = getCompilerContext();
//IC see: https://issues.apache.org/jira/browse/DERBY-3270

		if (origCompilationSchema != null) {
			// View expansion needs the definition time schema
			compilerContext.pushCompilationSchema(origCompilationSchema);
		}

        // Nested VTI/tableFunctions will want to know whether their arguments
        // reference tables in the FROM list which contains this subquery. Those
        // references are illegal. See DERBY-5554 and DERBY-5779.
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        CollectNodesVisitor<FromVTI> nestedVTIs =
                new CollectNodesVisitor<FromVTI>(FromVTI.class);
		subquery.accept( nestedVTIs );
        for (FromVTI ref : nestedVTIs.getList())
		{
            ref.addOuterFromList( fromListParam );
		}
        
		try {
			subquery.bindExpressions(nestedFromList);
			subquery.bindResultColumns(nestedFromList);
		} finally {
			if (origCompilationSchema != null) {
				compilerContext.popCompilationSchema();
			}
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-4397
//IC see: https://issues.apache.org/jira/browse/DERBY-4
		if (orderByList != null) {
			orderByList.bindOrderByColumns(subquery);
		}

        bindOffsetFetch(offset, fetchFirst);
//IC see: https://issues.apache.org/jira/browse/DERBY-4398

        /* NOTE: If the size of the derived column list is less than
		 * the size of the subquery's RCL and the derived column list is marked
		 * for allowing a size mismatch, then we have a select * view
		 * on top of a table that has had columns added to it via alter table.
		 * In this case, we trim out the columns that have been added to
		 * the table since the view was created.
		 */
		subqueryRCL = subquery.getResultColumns();
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if (getResultColumns() != null && getResultColumns().getCountMismatchAllowed() &&
			getResultColumns().size() < subqueryRCL.size())
		{
			for (int index = subqueryRCL.size() - 1; 
				 index >= getResultColumns().size(); 
				 index--)
			{
				subqueryRCL.removeElementAt(index);
			}
		}

        /*
         * Create RCL based on subquery, adding a level of VCNs.
         */
//IC see: https://issues.apache.org/jira/browse/DERBY-4397
         ResultColumnList newRcl = subqueryRCL.copyListAndObjects();
         newRcl.genVirtualColumnNodes(subquery, subquery.getResultColumns());
         setResultColumns( newRcl );
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		/* Propagate the name info from the derived column list */
		if (derivedRCL != null)
		{
            getResultColumns().propagateDCLInfo(derivedRCL, correlationName);
		}
	}

	/**
	 * Try to find a ResultColumn in the table represented by this FromBaseTable
	 * that matches the name in the given ColumnReference.
	 *
	 * @param columnReference	The columnReference whose name we're looking
	 *				for in the given table.
	 *
	 * @return	A ResultColumn whose expression is the ColumnNode
	 *			that matches the ColumnReference.
	 *		Returns null if there is no match.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultColumn getMatchingColumn(ColumnReference columnReference)
            throws StandardException
	{
		ResultColumn	resultColumn = null;
		String			columnsTableName;

		/*
		** RESOLVE: When we add support for schemas, check to see if
		** the column name specifies a schema, and if so, if this
		** table is in that schema.
		*/

		columnsTableName = columnReference.getTableName();

		// post 681, 1 may be no longer needed. 5 is the default case
		// now but what happens if the condition is false? Investigate.
		if (columnReference.getGeneratedToReplaceAggregate()) // 1
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
			resultColumn = getResultColumns().getResultColumn(columnReference.getColumnName());
		}
		else if (columnsTableName == null || columnsTableName.equals(correlationName)) // 5?
		{
		    resultColumn = getResultColumns().getAtMostOneResultColumn(columnReference, correlationName, false);
		}
		    

		if (resultColumn != null)
		{
			columnReference.setTableNumber(tableNumber);
//IC see: https://issues.apache.org/jira/browse/DERBY-4695
            columnReference.setColumnNumber(resultColumn.getColumnPosition());
		}

		return resultColumn;
	}

	/**
	 * Preprocess a ResultSetNode - this currently means:
	 *	o  Generating a referenced table map for each ResultSetNode.
	 *  o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
	 *  o  Converting the WHERE and HAVING clauses into PredicateLists and
	 *	   classifying them.
	 *  o  Ensuring that a ProjectRestrictNode is generated on top of every 
	 *     FromBaseTable and generated in place of every FromSubquery.  
	 *  o  Pushing single table predicates down to the new ProjectRestrictNodes.
	 *
	 * @param numTables			The number of tables in the DML Statement
	 * @param gbl				The group by list, if any
	 * @param fromList			The from list, if any
	 *
	 * @return ResultSetNode at top of preprocessed tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode preprocess(int numTables,
									GroupByList gbl,
									FromList fromList)
								throws StandardException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6378
        subquery.pushQueryExpressionSuffix();
		// Push the order by list down to the ResultSet
//IC see: https://issues.apache.org/jira/browse/DERBY-4397
//IC see: https://issues.apache.org/jira/browse/DERBY-4
		if (orderByList != null)
		{
			// If we have more than 1 ORDERBY columns, we may be able to
			// remove duplicate columns, e.g., "ORDER BY 1, 1, 2".
			if (orderByList.size() > 1)
			{
				orderByList.removeDupColumns();
			}

			subquery.pushOrderByList(orderByList);
			orderByList = null;
		}

        subquery.pushOffsetFetchFirst( offset, fetchFirst, hasJDBClimitClause );

		/* We want to chop out the FromSubquery from the tree and replace it 
		 * with a ProjectRestrictNode.  One complication is that there may be 
		 * ColumnReferences above us which point to the FromSubquery's RCL.
		 * What we want to return is a tree with a PRN with the
		 * FromSubquery's RCL on top.  (In addition, we don't want to be
		 * introducing any redundant ProjectRestrictNodes.)
		 * Another complication is that we want to be able to only push
		 * projections and restrictions down to this ProjectRestrict, but
		 * we want to be able to push them through as well.
		 * So, we:
		 *		o call subquery.preprocess() which returns a tree with
		 *		  a SelectNode or a RowResultSetNode on top.  
		 *		o If the FSqry is flattenable(), then we return (so that the
		 *		  caller can then call flatten()), otherwise we:
		 *		o generate a PRN, whose RCL is the FSqry's RCL, on top of the result.
		 *		o create a referencedTableMap for the PRN which represents 
		 *		  the FSqry's tableNumber, since ColumnReferences in the outer
		 *		  query block would be referring to that one.  
		 *		  (This will allow us to push restrictions down to the PRN.)
		 */

		subquery = subquery.preprocess(numTables, gbl, fromList);

		/* Return if the FSqry is flattenable() 
		 * NOTE: We can't flatten a FromSubquery if there is a group by list
		 * because the group by list must be ColumnReferences.  For:
		 *	select c1 from v1 group by c1,
		 *	where v1 is select 1 from t1
		 * The expression under the last redundant ResultColumn is an IntConstantNode,
		 * not a ColumnReference.
		 * We also do not flatten a subquery if tableProperties is non-null,
		 * as the user is specifying 1 or more properties for the derived table,
		 * which could potentially be lost on the flattening.
		 * RESOLVE - this is too restrictive.
		 */
		if ((gbl == null || gbl.size() == 0) &&
			tableProperties == null &&
		    subquery.flattenableInFromSubquery(fromList))
		{
			/* Set our table map to the subquery's table map. */
			setReferencedTableMap(subquery.getReferencedTableMap());
			return this;
		}

		return extractSubquery(numTables);
	}

	/**
	 * Extract out and return the subquery, with a PRN on top.
	 * (See FromSubquery.preprocess() for more details.)
	 *
	 * @param numTables			The number of tables in the DML Statement
	 *
	 * @return ResultSetNode at top of extracted tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultSetNode extractSubquery(int numTables)
		throws StandardException
	{
		JBitSet		  newJBS;
		ResultSetNode newPRN;

        newPRN = new ProjectRestrictNode(
								subquery,		/* Child ResultSet */
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
								getResultColumns(),	/* Projection */
								null,			/* Restriction */
								null,			/* Restriction as PredicateList */
								null,			/* Subquerys in Projection */
								null,			/* Subquerys in Restriction */
								tableProperties,
								getContextManager()	 );

		/* Set up the PRN's referencedTableMap */
		newJBS = new JBitSet(numTables);
		newJBS.set(tableNumber);
		newPRN.setReferencedTableMap(newJBS);
		((FromTable) newPRN).setTableNumber(tableNumber);

		return newPRN;
	}

	/**
	 * Flatten this FSqry into the outer query block. The steps in
	 * flattening are:
	 *	o  Mark all ResultColumns as redundant, so that they are "skipped over"
	 *	   at generate().
	 *	o  Append the wherePredicates to the outer list.
	 *	o  Return the fromList so that the caller will merge the 2 lists 
	 *  RESOLVE - FSqrys with subqueries are currently not flattenable.  Some of
	 *  them can be flattened, however.  We need to merge the subquery list when
	 *  we relax this restriction.
	 *
	 * NOTE: This method returns NULL when flattening RowResultSetNodes
	 * (the node for a VALUES clause).  The reason is that no reference
	 * is left to the RowResultSetNode after flattening is done - the
	 * expressions point directly to the ValueNodes in the RowResultSetNode's
	 * ResultColumnList.
	 *
	 * @param rcl				The RCL from the outer query
	 * @param outerPList	PredicateList to append wherePredicates to.
	 * @param sql				The SubqueryList from the outer query
	 * @param gbl				The group by list, if any
     * @param havingClause      The HAVING clause, if any
	 *
	 * @return FromList		The fromList from the underlying SelectNode.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    FromList flatten(ResultColumnList rcl,
							PredicateList outerPList,
							SubqueryList sql,
//IC see: https://issues.apache.org/jira/browse/DERBY-4698
//IC see: https://issues.apache.org/jira/browse/DERBY-3880
                            GroupByList gbl,
                            ValueNode havingClause)

			throws StandardException
	{
		FromList	fromList = null;
		SelectNode	selectNode;

		getResultColumns().setRedundant();
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		subquery.getResultColumns().setRedundant();

		/*
		** RESOLVE: Each type of result set should know how to remap itself.
		*/
		if (subquery instanceof SelectNode)
		{
			selectNode = (SelectNode) subquery;
			fromList = selectNode.getFromList();

			// selectNode.getResultColumns().setRedundant();

			if (selectNode.getWherePredicates().size() > 0)
			{
				outerPList.destructiveAppend(selectNode.getWherePredicates());
			}

			if (selectNode.getWhereSubquerys().size() > 0)
			{
				sql.destructiveAppend(selectNode.getWhereSubquerys());
			}
		}
		else if ( ! (subquery instanceof RowResultSetNode))
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("subquery expected to be either a SelectNode or a RowResultSetNode, but is a " + subquery.getClass().getName());
			}
		}

		/* Remap all ColumnReferences from the outer query to this node.
		 * (We replace those ColumnReferences with clones of the matching
		 * expression in the SELECT's RCL.
		 */
		rcl.remapColumnReferencesToExpressions();
		outerPList.remapColumnReferencesToExpressions();
		if (gbl != null)
		{
			gbl.remapColumnReferencesToExpressions();
		}

//IC see: https://issues.apache.org/jira/browse/DERBY-4698
//IC see: https://issues.apache.org/jira/browse/DERBY-3880
        if (havingClause != null) {
            havingClause.remapColumnReferencesToExpressions();
        }

		return fromList;
	}

	/**
	 * Get the exposed name for this table, which is the name that can
	 * be used to refer to it in the rest of the query.
	 *
	 * @return	The exposed name for this table.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getExposedName()
	{
		return correlationName;
	}

	/**
	 * Expand a "*" into a ResultColumnList with all of the
	 * result columns from the subquery.
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ResultColumnList getAllResultColumns(TableName allTableName)
			throws StandardException
	{
		TableName		 exposedName;
        TableName        toCompare;

//IC see: https://issues.apache.org/jira/browse/DERBY-13

		if(allTableName != null)
             toCompare = makeTableName(allTableName.getSchemaName(),correlationName);
        else
            toCompare = makeTableName(null,correlationName);
        
        if ( allTableName != null &&
             ! allTableName.equals(toCompare))
        {
            return null;
        }

		/* Cache exposed name for this table.
		 * The exposed name becomes the qualifier for each column
		 * in the expanded list.
		 */
		exposedName = makeTableName(null, correlationName);

        ResultColumnList rcList = new ResultColumnList((getContextManager()));
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973

		/* Build a new result column list based off of resultColumns.
		 * NOTE: This method will capture any column renaming due to 
		 * a derived column list.
		 */

		// Use visibleSize, because we don't want to propagate any order by
		// columns not selected.
		int rclSize = getResultColumns().visibleSize();

		for (int index = 0; index < rclSize; index++)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
			ResultColumn resultColumn = getResultColumns().elementAt(index);
			ValueNode		 valueNode;
			String			 columnName;

			if (resultColumn.isGenerated())
			{
				continue;
			}

			// Build a ResultColumn/ColumnReference pair for the column //
			columnName = resultColumn.getName();
			boolean isNameGenerated = resultColumn.isNameGenerated();

			/* If this node was generated for a GROUP BY, then tablename for the CR, if any,
			 * comes from the source RC.
			 */
			TableName tableName;

			tableName = exposedName;
//IC see: https://issues.apache.org/jira/browse/DERBY-2442

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            valueNode = new ColumnReference(columnName,
											tableName,
											getContextManager());
//IC see: https://issues.apache.org/jira/browse/DERBY-673
            resultColumn = new ResultColumn(columnName,
											valueNode,
											getContextManager());

			resultColumn.setNameGenerated(isNameGenerated);
			// Build the ResultColumnList to return //
			rcList.addResultColumn(resultColumn);
		}
		return rcList;
	}

	/**
	 * Search to see if a query references the specifed table name.
	 *
	 * @param name		Table name (String) to search for.
	 * @param baseTable	Whether or not name is for a base table
	 *
	 * @return	true if found, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    boolean referencesTarget(String name, boolean baseTable)
		throws StandardException
	{
		return subquery.referencesTarget(name, baseTable);
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
		return subquery.referencesSessionSchema();
	}

	/**
	 * Bind any untyped null nodes to the types in the given ResultColumnList.
	 *
	 * @param bindingRCL	The ResultColumnList with the types to bind to.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void bindUntypedNullsToResultColumns(ResultColumnList bindingRCL)
				throws StandardException
	{
		subquery.bindUntypedNullsToResultColumns(bindingRCL);
	}

	/**
	 * Decrement (query block) level (0-based) for this FromTable.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
    @Override
	void decrementLevel(int decrement)
	{
		super.decrementLevel(decrement);
		subquery.decrementLevel(decrement);
	}

	/**
	 * Associate this subquery with the original compilation schema of a view.
	 *
	 * @param sd schema descriptor of the original compilation schema of the
	 * view.
	 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setOrigCompilationSchema(SchemaDescriptor sd) {
//IC see: https://issues.apache.org/jira/browse/DERBY-3270
		origCompilationSchema = sd;
	}

    /**
     * @see QueryTreeNode#acceptChildren
     */
    @Override
    void acceptChildren(Visitor v)
        throws StandardException
    {
        super.acceptChildren(v);

        subquery.accept(v);

        if (orderByList != null) {
            orderByList.accept(v);
        }

        if (offset != null) {
            offset.accept(v);
        }

        if (fetchFirst != null) {
            fetchFirst.accept(v);
        }
    }
}
