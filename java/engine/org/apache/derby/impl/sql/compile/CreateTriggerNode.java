/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateTriggerNode

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Parser;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.depend.Dependent;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.catalog.UUID;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * A CreateTriggerNode is the root of a QueryTree 
 * that represents a CREATE TRIGGER
 * statement.
 *
 * @author jamie
 */

public class CreateTriggerNode extends CreateStatementNode
{
	private	TableName			triggerName;
	private	TableName			tableName;
	private	int					triggerEventMask;
	private ResultColumnList	triggerCols;
	private	boolean				isBefore;
	private	boolean				isRow;
	private	boolean				isEnabled;
	private	Vector				refClause;
	private	QueryTreeNode		whenClause;
	private	String				whenText;
	private	int					whenOffset;
	private	StatementNode		actionNode;
	private	String				actionText;
	private	String				originalActionText; // text w/o trim of spaces
	private	int					actionOffset;

	private SchemaDescriptor	triggerSchemaDescriptor;
	private SchemaDescriptor	compSchemaDescriptor;
	private int[]				referencedColInts;
	private TableDescriptor		triggerTableDescriptor;
	private	UUID				actionCompSchemaId;

	/*
	** Names of old and new table.  By default we have
	** OLD/old and NEW/new.  The casing is dependent on 
	** the language connection context casing as the rest
    ** of other code. Therefore we will set the value of the 
    ** String at the init() time.
    ** However, if there is a referencing clause
	** we will reset these values to be whatever the user
	** wants.
	*/
	private String oldTableName;
	private String newTableName;

	private boolean oldTableInReferencingClause;
	private boolean newTableInReferencingClause;


	/**
	 * Initializer for a CreateTriggerNode
	 *
	 * @param triggerName			name of the trigger	
	 * @param tableName				name of the table which the trigger is declared upon	
	 * @param triggerEventMask		TriggerDescriptor.TRIGGER_EVENT_XXX
	 * @param triggerCols			columns trigger is to fire upon.  Valid
	 *								for UPDATE case only.
	 * @param isBefore				is before trigger (false for after)
	 * @param isRow					true for row trigger, false for statement
	 * @param isEnabled				true if enabled
	 * @param refClause				the referencing clause
	 * @param whenClause			the WHEN clause tree
	 * @param whenText				the text of the WHEN clause
	 * @param whenOffset			offset of start of WHEN clause
	 * @param actionNode			the trigger action tree
	 * @param actionText			the text of the trigger action
	 * @param actionOffset			offset of start of action clause
	 *
	 * @return	A CreateTriggerNode 
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init 
	(
		Object		triggerName,
		Object		tableName,
		Object				triggerEventMask,
		Object triggerCols,
		Object			isBefore,
		Object			isRow,
		Object			isEnabled,
		Object			refClause,
		Object	whenClause,
		Object			whenText,
		Object				whenOffset,
		Object	actionNode,
		Object			actionText,
		Object				actionOffset
	) throws StandardException
	{
		initAndCheck(triggerName);
		this.triggerName = (TableName) triggerName;
		this.tableName = (TableName) tableName;
		this.triggerEventMask = ((Integer) triggerEventMask).intValue();
		this.triggerCols = (ResultColumnList) triggerCols;
		this.isBefore = ((Boolean) isBefore).booleanValue();
		this.isRow = ((Boolean) isRow).booleanValue();
		this.isEnabled = ((Boolean) isEnabled).booleanValue();
		this.refClause = (Vector) refClause;	
		this.whenClause = (QueryTreeNode) whenClause;
		this.whenText = (whenText == null) ? null : ((String) whenText).trim();
		this.whenOffset = ((Integer) whenOffset).intValue();
		this.actionNode = (StatementNode) actionNode;
		this.originalActionText = (String) actionText;
		this.actionText =
					(actionText == null) ? null : ((String) actionText).trim();
		this.actionOffset = ((Integer) actionOffset).intValue();

		implicitCreateSchema = true;
	}

	public String statementToString()
	{
		return "CREATE TRIGGER";
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

			if (triggerCols != null)
			{
				printLabel(depth, "triggerColumns: ");
				triggerCols.treePrint(depth + 1);
			}
			if (whenClause != null)
			{
				printLabel(depth, "whenClause: ");
				whenClause.treePrint(depth + 1);
			}
			if (actionNode != null)
			{
				printLabel(depth, "actionNode: ");
				actionNode.treePrint(depth + 1);
			}
		}
	}


	// accessors


	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Bind this CreateTriggerNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		CompilerContext compilerContext = getCompilerContext();
		DataDictionary	dd = getDataDictionary();
		/*
		** Grab the current schema.  We will use that for
		** sps compilation
		*/
		LanguageConnectionContext lcc = getLanguageConnectionContext();
		compSchemaDescriptor = lcc.getDefaultSchema();

		/*
		** Get and check the schema descriptor for this
		** trigger.  This check will throw the proper exception
		** if someone tries to create a trigger in the SYS
		** schema.
		*/
		triggerSchemaDescriptor = getSchemaDescriptor();

		/*
		** Get the trigger table.
		*/
		triggerTableDescriptor = getTableDescriptor(tableName);

		//throw an exception if user is attempting to create a trigger on a temporary table
		if (isSessionSchema(triggerTableDescriptor.getSchemaDescriptor()))
		{
				throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);
		}

		/*
		** Regenerates the actionText and actionNode if necessary.
		*/
		boolean needInternalSQL = bindReferencesClause(dd);

		lcc.pushTriggerTable(triggerTableDescriptor);
		try
		{	
			/*
			** Bind the trigger action and the trigger
			** when clause to make sure that they are
			** ok.  Note that we have already substituted 
			** in various replacements for OLD/NEW transition
			** tables/variables and reparsed if necessary.
			*/
			if (needInternalSQL)
				compilerContext.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);

			actionNode.bind();

			if (whenClause != null)
			{
				whenClause.bind();
			}
		}
		finally
		{
			lcc.popTriggerTable(triggerTableDescriptor);
		}

		/* 
		** Statement is dependent on the TableDescriptor 
		*/
		compilerContext.createDependency(triggerTableDescriptor);

		/*
		** If there is a list of columns, then no duplicate columns,
		** and all columns must be found.
		*/
		if (triggerCols != null && triggerCols.size() != 0)
		{
			referencedColInts = new int[triggerCols.size()];
			Hashtable columnNames = new Hashtable();
			int tcSize = triggerCols.size();
			for (int i = 0; i < tcSize; i++)
			{
				ResultColumn rc  = (ResultColumn) triggerCols.elementAt(i);
				if (columnNames.put(rc.getName(), rc) != null)
				{
					throw StandardException.newException(SQLState.LANG_DUPLICATE_COLUMN_IN_TRIGGER_UPDATE, 
											rc.getName(), 
											triggerName);
				}

				ColumnDescriptor cd = triggerTableDescriptor.getColumnDescriptor(rc.getName());
				if (cd == null)
				{
					throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, 
																rc.getName(),
																tableName);
				}

				referencedColInts[i] = cd.getPosition();
			}
 
			// sort the list
			java.util.Arrays.sort(referencedColInts);
		}

		//If attempting to reference a SESSION schema table (temporary or permanent) in the trigger action, throw an exception
		if (actionNode.referencesSessionSchema())
			throw StandardException.newException(SQLState.LANG_OPERATION_NOT_ALLOWED_ON_SESSION_SCHEMA_TABLES);

		return this;
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
		//If create trigger is part of create statement and the trigger is defined on or it references SESSION schema tables,
		//it will get caught in the bind phase of trigger and exception will be thrown by the trigger bind. 
		return (isSessionSchema(triggerTableDescriptor.getSchemaName()) || actionNode.referencesSessionSchema());
	}

	/*
	** BIND OLD/NEW TRANSITION TABLES/VARIABLES
	**
	** 1) validate the referencing clause (if any)
	**
	** 2) convert trigger action text.  e.g. 
	**		DELETE FROM t WHERE c = old.c
	** turns into
	**		DELETE FROM t WHERE c = org.apache.derby.iapi.db.Factory::
	**					getTriggerExecutionContext().getOldRow().getInt('C');
	** or
	**		DELETE FROM t WHERE c in (SELECT c FROM OLD)
	** turns into
	**		DELETE FROM t WHERE c in (SELECT c FROM new TriggerOldTransitionTable OLD)
	**
	** 3) check all column references against new/old transition 
	**	variables (since they are no longer 'normal' column references
	** 	that will be checked during bind)
	**
	** 4) reparse the new action text
	**
	** You might be wondering why we regenerate the text and reparse
	** instead of just reworking the tree to have the nodes we want.
	** Well, the primary reason is that if we screwed with the tree,
	** then we would have a major headache if this trigger action
	** was ever recompiled -- spses don't really know that they are
	** triggers so it would be quite arduous to figure out that an
	** sps is a trigger and munge up its query tree after figuring
	** out what its OLD/NEW tables are, etc.  Also, it is just plain
	** easier to just generate the sql and rebind.  
	*/
	private boolean bindReferencesClause(DataDictionary dd) throws StandardException
	{
		validateReferencesClause(dd);

		StringBuffer newText = new StringBuffer();
		boolean regenNode = false;
		int start = 0;
		if (isRow)
		{
			/*
			** For a row trigger, we find all column references.  If
			** they are referencing NEW or OLD we turn them into
			** getTriggerExecutionContext().getOldRow().getInt('C');
			*/
			CollectNodesVisitor visitor = new CollectNodesVisitor(ColumnReference.class);
			actionNode.accept(visitor);
			Vector refs = visitor.getList();
			/* we need to sort on position in string, beetle 4324
			 */
			QueryTreeNode[] cols = sortRefs(refs, true);

			for (int i = 0; i < cols.length; i++)
			{
				ColumnReference ref = (ColumnReference) cols[i];
				TableName tableName = ref.getTableNameNode();
				if ((tableName == null) ||
					((oldTableName == null || !oldTableName.equals(tableName.getTableName())) &&
					(newTableName == null || !newTableName.equals(tableName.getTableName()))))
				{
					continue;
				}
					
				int tokBeginOffset = tableName.getTokenBeginOffset();
				int tokEndOffset = tableName.getTokenEndOffset();
				if (tokBeginOffset == -1)
				{
					continue;
				}

				regenNode = true;
				checkInvalidTriggerReference(tableName.getTableName());
				String colName = ref.getColumnName();
				int columnLength = ref.getTokenEndOffset() - ref.getTokenBeginOffset() + 1;

				newText.append(originalActionText.substring(start, tokBeginOffset-actionOffset));
				newText.append(genColumnReferenceSQL(dd, colName, tableName.getTableName(), tableName.getTableName().equals(oldTableName)));
				start = tokEndOffset- actionOffset + columnLength + 2;
			}
		}
		else
		{
			/*
			** For a statement trigger, we find all FromBaseTable nodes.  If
			** the from table is NEW or OLD (or user designated alternates
			** REFERENCING), we turn them into a trigger table VTI.
			*/
			CollectNodesVisitor visitor = new CollectNodesVisitor(FromBaseTable.class);
			actionNode.accept(visitor);
			Vector refs = visitor.getList();
			QueryTreeNode[] tabs = sortRefs(refs, false);
			for (int i = 0; i < tabs.length; i++)
			{
				FromBaseTable fromTable = (FromBaseTable) tabs[i];
				String refTableName = fromTable.getTableName().getTableName();
				String baseTableName = fromTable.getBaseTableName();
				if ((baseTableName == null) ||
					((oldTableName == null || !oldTableName.equals(baseTableName)) &&
					(newTableName == null || !newTableName.equals(baseTableName))))
				{
					continue;
				}
				int tokBeginOffset = fromTable.getTableNameField().getTokenBeginOffset();
				int tokEndOffset = fromTable.getTableNameField().getTokenEndOffset();
				if (tokBeginOffset == -1)
				{
					continue;
				}

				checkInvalidTriggerReference(baseTableName);

				regenNode = true;
				newText.append(originalActionText.substring(start, tokBeginOffset-actionOffset));
				newText.append(baseTableName.equals(oldTableName) ?
								"new org.apache.derby.catalog.TriggerOldTransitionRows() " :
								"new org.apache.derby.catalog.TriggerNewTransitionRows() ");
				/*
				** If the user supplied a correlation, then just
				** pick it up automatically; otherwise, supply
				** the default.
				*/
				if (refTableName.equals(baseTableName))
				{
					newText.append(baseTableName).append(" ");
				}
				start=tokEndOffset-actionOffset+1;
			}
		}

		/*
		** Parse the new action text with the substitutions.
		** Also, we reset the actionText to this new value.  This
		** is what we are going to stick in the system tables.
		*/
		if (regenNode)
		{
			if (start < originalActionText.length())
			{
				newText.append(originalActionText.substring(start));
			}
			actionText = newText.toString();
			actionNode = (StatementNode)reparseTriggerText(actionText);
		}

		return regenNode;
	}

	/*
	** Sort the refs into array.
	*/
	private QueryTreeNode[] sortRefs(Vector refs, boolean isRow)
	{
		int size = refs.size();
		QueryTreeNode[] sorted = new QueryTreeNode[size];
		int i = 0;
		for (Enumeration e = refs.elements(); e.hasMoreElements(); )
		{
			if (isRow)
				sorted[i++] = (ColumnReference)e.nextElement();
			else
				sorted[i++] = (FromBaseTable)e.nextElement();
		}

		/* bubble sort
		 */
		QueryTreeNode temp;
		for (i = 0; i < size - 1; i++)
		{
			temp = null;
			for (int j = 0; j < size - i - 1; j++)
			{
				if ((isRow && 
					 ((ColumnReference) sorted[j]).getTokenBeginOffset() > 
					 ((ColumnReference) sorted[j+1]).getTokenBeginOffset()
					) ||
					(!isRow &&
					 ((FromBaseTable) sorted[j]).getTableNameField().getTokenBeginOffset() > 
					 ((FromBaseTable) sorted[j+1]).getTableNameField().getTokenBeginOffset()
					))
				{
					temp = sorted[j];
					sorted[j] = sorted[j+1];
					sorted[j+1] = temp;
				}
			}
			if (temp == null)		// sorted
				break;
		}

		return sorted;
	}

	/*
	** Parse the text and return the tree.
	*/
	private QueryTreeNode reparseTriggerText(String text) throws StandardException
	{
		/*
		** Get a new compiler context, so the parsing of the text
		** doesn't mess up anything in the current context 
		*/
		LanguageConnectionContext lcc = getLanguageConnectionContext();
		CompilerContext newCC = lcc.pushCompilerContext();
		newCC.setReliability(CompilerContext.INTERNAL_SQL_LEGAL);

		try
		{
			return QueryTreeNode.parseQueryText(newCC, text, (Object[])null, lcc);
		}

		finally
		{
			lcc.popCompilerContext(newCC);
		}
	}
	/*
	** Make sure the given column name is found in the trigger
	** target table.  Generate the appropriate SQL to get it.
	**
	** @return a string that is used to get the column using
	** getObject() on the desired result set and CAST it back
	** to the proper type in the SQL domain. 
	**
	** @exception StandardException on invalid column name
	*/
	private String genColumnReferenceSQL
	(
		DataDictionary	dd, 
		String			colName, 
		String			tabName,
		boolean			isOldTable
	) throws StandardException
	{
		ColumnDescriptor colDesc = null;
		if ((colDesc = triggerTableDescriptor.getColumnDescriptor(colName)) == null)
		{
			throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND, tabName+"."+colName);
		}

		/*
		** Generate something like this:
		**
		** 		cast (org.apache.derby.iapi.db.Factory::
		**			getTriggerExecutionContext().getNewRow().
		**				getObject('<colName>') AS DECIMAL(6,2))
		**
		** The cast back to the SQL Domain may seem redundant
		** but we need it to make the column reference appear
		** EXACTLY like a regular column reference, so we need
		** the object in the SQL Domain and we need to have the
		** type information.  Thus a user should be able to do 
		** something like
		**
		**		CREATE TRIGGER ... INSERT INTO T length(Column), ...
		*/
		StringBuffer methodCall = new StringBuffer();
		methodCall.append("cast (org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().");
		methodCall.append(isOldTable ? "getOldRow()" : "getNewRow()");
		methodCall.append(".getObject('"+colName+"') AS ");
		DataTypeDescriptor dts = colDesc.getType();
		TypeId typeId = dts.getTypeId();

		/*
		** getSQLString() returns <typeName> 
		** for user types, so call getSQLTypeName in that
		** case.
		*/
		methodCall.append((typeId.systemBuiltIn() ? 
					dts.getSQLstring() :
					typeId.getSQLTypeName()) + ") ");

		return methodCall.toString();
	}

	/*
	** Check for illegal combinations here: insert & old or
	** delete and new
	*/
	private void checkInvalidTriggerReference(String tableName) throws StandardException
	{
		if (tableName.equals(oldTableName) && 
			(triggerEventMask & TriggerDescriptor.TRIGGER_EVENT_INSERT) == TriggerDescriptor.TRIGGER_EVENT_INSERT)
		{
			throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "INSERT", "new");
		}
		else if (tableName.equals(newTableName) && 
			(triggerEventMask & TriggerDescriptor.TRIGGER_EVENT_DELETE) == TriggerDescriptor.TRIGGER_EVENT_DELETE)
		{
			throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "DELETE", "old");
		}
	}

	/*
	** Make sure that the referencing clause is legitimate.
	** While we are at it we set the new/oldTableName to
	** be whatever the user wants.
	*/
	private void validateReferencesClause(DataDictionary dd) throws StandardException
	{
		if ((refClause == null) || (refClause.size() == 0))
		{
			return;
		}

		for (Enumeration e = refClause.elements(); e.hasMoreElements(); )
		{
			TriggerReferencingStruct trn = (TriggerReferencingStruct)e.nextElement();

			/*
			** 1) Make sure that we don't try to refer
			** to a table for a row trigger or a row for
			** a table trigger.
			*/
			if (isRow && !trn.isRow)
			{
				throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "ROW", "row");
			}
			else if (!isRow && trn.isRow) 
			{
				throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "STATEMENT", "table");
			}

			/*
			** 2) Make sure we have no dups
			*/
			if (trn.isNew)
			{

				if (newTableInReferencingClause) 
				{
					throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_CLAUSE_DUPS);
				}

				/*
				** 3a) No NEW reference in delete trigger
				*/
				if ((triggerEventMask & TriggerDescriptor.TRIGGER_EVENT_DELETE) == TriggerDescriptor.TRIGGER_EVENT_DELETE)
				{
					throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "DELETE", "old");
				}
				newTableName = trn.identifier;
				newTableInReferencingClause = true;
			}
			else
			{
				if (oldTableInReferencingClause)
				{
					throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_CLAUSE_DUPS);
				}
				/*
				** 3b) No OLD reference in insert trigger
				*/
				if ((triggerEventMask & TriggerDescriptor.TRIGGER_EVENT_INSERT) == TriggerDescriptor.TRIGGER_EVENT_INSERT)
				{
					throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "INSERT", "new");
				}
				oldTableName = trn.identifier;
				oldTableInReferencingClause = true;
			}

			/*
			** 4) Additional restriction on BEFORE triggers
			*/
			if (this.isBefore && !trn.isRow) {
			// OLD_TABLE and NEW_TABLE not allowed for BEFORE triggers.
				throw StandardException.newException(SQLState.LANG_TRIGGER_BAD_REF_MISMATCH, "BEFORE", "row");
			}

		}

	}				


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction makeConstantAction() throws StandardException
	{
		String oldReferencingName = (oldTableInReferencingClause) ? oldTableName : null;
		String newReferencingName = (newTableInReferencingClause) ? newTableName : null;

		return	getGenericConstantActionFactory().getCreateTriggerConstantAction(
											triggerSchemaDescriptor.getSchemaName(),
											getRelativeName(),
											triggerEventMask,
											isBefore,
											isRow,
											isEnabled,
											triggerTableDescriptor,	
											(UUID)null,			// when SPSID
											whenText,
											(UUID)null,			// action SPSid 
											actionText,
											(actionCompSchemaId == null) ?
												compSchemaDescriptor.getUUID() :
												actionCompSchemaId,
											(Timestamp)null,	// creation time
											referencedColInts,
											originalActionText,
											oldTableInReferencingClause,
											newTableInReferencingClause,
											oldReferencingName,
											newReferencingName
											);
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
			String refString = "null";
			if (refClause != null)
			{
				StringBuffer buf = new StringBuffer();
				for (Enumeration e = refClause.elements(); e.hasMoreElements(); )
				{
					buf.append("\t");
					TriggerReferencingStruct trn = 	
							(TriggerReferencingStruct)e.nextElement();
					buf.append(trn.toString());
					buf.append("\n");
				}
				refString = buf.toString();
			}

			return super.toString() +
				"tableName: "+tableName+		
				"\ntriggerEventMask: "+triggerEventMask+		
				"\nisBefore: "+isBefore+		
				"\nisRow: "+isRow+		
				"\nisEnabled: "+isEnabled+		
				"\nwhenText: "+whenText+
				"\nrefClause: "+refString+
				"\nactionText: "+actionText+
				"\n";
		}
		else
		{
			return "";
		}
	}

}
