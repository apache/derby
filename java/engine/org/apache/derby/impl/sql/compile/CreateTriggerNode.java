/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateTriggerNode

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

import java.sql.Timestamp;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * A CreateTriggerNode is the root of a QueryTree 
 * that represents a CREATE TRIGGER
 * statement.
 *
 */

public class CreateTriggerNode extends DDLStatementNode
{
	private	TableName			triggerName;
	private	TableName			tableName;
	private	int					triggerEventMask;
	private ResultColumnList	triggerCols;
	private	boolean				isBefore;
	private	boolean				isRow;
	private	boolean				isEnabled;
	private	Vector				refClause;
	private	ValueNode		    whenClause;
	private	String				whenText;
	private	int					whenOffset;
	private	StatementNode		actionNode;
	private	String				actionText;
	private	String				originalActionText; // text w/o trim of spaces
	private	int					actionOffset;

	private SchemaDescriptor	triggerSchemaDescriptor;
	private SchemaDescriptor	compSchemaDescriptor;
	
	/*
	 * The following arrary will include columns that will cause the trigger to
	 * fire. This information will get saved in SYSTRIGGERS.
	 * 
	 * The array will be null for all kinds of insert and delete triggers but
	 * it will be non-null for a subset of update triggers.
	 *  
	 * For update triggers, the array will be null if no column list is 
	 * supplied in the CREATE TRIGGER trigger column clause as shown below.
	 * The UPDATE trigger below will fire no matter which column in table1
	 * gets updated.
	 * eg
	 * CREATE TRIGGER tr1 AFTER UPDATE ON table1 
	 *    REFERENCING OLD AS oldt NEW AS newt
	 *    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
	 * 
	 * For update triggers, this array will be non-null if specific trigger
	 * column(s) has been specified in the CREATE TRIGGER sql. The UPDATE
	 * trigger below will fire when an update happens on column c12 in table1.
	 * eg
	 * CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
	 *    REFERENCING OLD AS oldt NEW AS newt
	 *    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
	 * 
	 * Array referencedColInts along with referencedColsInTriggerAction will 
	 * be used to determine which columns from the triggering table need to 
	 * be read in when the trigger fires, thus making sure that we do not
	 * read the columns from the trigger table that are not required for
	 * trigger execution.
	 */
	private int[]				referencedColInts;
	
	/*
	 * The following array (which was added as part of DERBY-1482) will 
	 * include columns referenced in the trigger action through the 
	 * REFERENCING clause(old/new transition variables), in other trigger
	 * action columns. This information will get saved in SYSTRIGGERS
	 * (with the exception of triggers created in pre-10.7 dbs. For 
	 * pre-10.7 dbs, this information will not get saved in SYSTRIGGERS
	 * in order to maintain backward compatibility.
	 * 
	 * Unlike referencedColInts, this array can be non-null for all 3 types
	 * of triggers, namely, INSERT, UPDATE AND DELETE triggers. This array
	 * will be null if no columns in the trigger action are referencing
	 * old/new transition variables
	 * 
	 * eg of a trigger in 10.7 and higher dbs which will cause 
	 * referencedColsInTriggerAction to be null
	 * CREATE TRIGGER tr1 NO CASCADE BEFORE UPDATE of c12 ON table1
	 *    SELECT c24 FROM table2 WHERE table2.c21 = 1
	 * 
	 * eg of a trigger in 10.7 and higher dbs which will cause 
	 * referencedColsInTriggerAction to be non-null
	 * For the trigger below, old value of column c14 from trigger table is
	 * used in the trigger action through old/new transition variables. A
	 * note of this requirement to read c14 will be made in
	 * referencedColsInTriggerAction array.
	 * eg
	 * CREATE TRIGGER tr1 AFTER UPDATE ON table1 
	 *    REFERENCING OLD AS oldt NEW AS newt
	 *    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
	 * 
	 * The exception to the rules above for trigger action columns information
	 * in referencedColsInTriggerAction is a trigger that was created with
	 * pre-10.7 release. Prior to 10.7, we did not collect any information
	 * about trigger action columns. So, any of the 2 kinds of trigger shown
	 * above prior to 10.7 will not have any trigger action column info on
	 * them in SYSTRIGGERS table. In order to cover the pre-existing pre-10.7
	 * triggers and all the other kinds of triggers, we will follow following
	 * 4 rules during trigger execution.
	 *   Rule1)If trigger column information is null, then read all the
	 *   columns from trigger table into memory irrespective of whether
	 *   there is any trigger action column information. 2 egs of such
	 *   triggers
	 *      create trigger tr1 after update on t1 for each row values(1);
	 *      create trigger tr1 after update on t1 referencing old as oldt
	 *      	for each row insert into t2 values(2,oldt.j,-2);
	 *   Rule2)If trigger column information is available but no trigger
	 *   action column information is found and no REFERENCES clause is
	 *   used for the trigger, then only read the columns identified by
	 *   the trigger column. eg
	 *      create trigger tr1 after update of c1 on t1 
	 *      	for each row values(1);
	 *   Rule3)If trigger column information and trigger action column
	 *   information both are not null, then only those columns will be
	 *   read into memory. This is possible only for triggers created in
	 *   release 10.7 or higher. Because prior to that we did not collect
	 *   trigger action column informatoin. eg
	 *      create trigger tr1 after update of c1 on t1
	 *      	referencing old as oldt for each row
	 *      	insert into t2 values(2,oldt.j,-2);
	 *   Rule4)If trigger column information is available but no trigger
	 *   action column information is found but REFERENCES clause is used
	 *   for the trigger, then read all the columns from the trigger
	 *   table. This will cover soft-upgrade and hard-upgrade scenario
	 *   for triggers created pre-10.7. This rule prevents us from having
	 *   special logic for soft-upgrade. Additionally, this logic makes
	 *   invalidation of existing triggers unnecessary during
	 *   hard-upgrade. The pre-10.7 created triggers will work just fine
	 *   even though for some triggers, they would have trigger action
	 *   columns missing from SYSTRIGGERS. A user can choose to drop and
	 *   recreate such triggers to take advantage of Rule 3 which will
	 *   avoid unnecessary column reads during trigger execution.
	 *   eg trigger created prior to 10.7
	 *      create trigger tr1 after update of c1 on t1
	 *      	referencing old as oldt for each row
	 *      	insert into t2 values(2,oldt.j,-2);
	 *   To reiterate, Rule4) is there to cover triggers created with
	 *   pre-10,7 releases but now that database has been
	 *   hard/soft-upgraded to 10.7 or higher version. Prior to 10.7,
	 *   we did not collect any information about trigger action columns.
	 *   
	 *   The only place we will need special code for soft-upgrade is during
	 *   trigger creation. If we are in soft-upgrade mode, we want to make sure
	 *   that we do not save information about trigger action columns in
	 *   SYSTRIGGERS because the releases prior to 10.7 do not understand
	 *   trigger action column information.
	 *   
	 * Array referencedColInts along with referencedColsInTriggerAction will 
	 * be used to determine which columns from the triggering table needs to 
	 * be read in when the trigger fires, thus making sure that we do not
	 * read the columns from the trigger table that are not required for
	 * trigger execution.
	 */
	private int[]				referencedColsInTriggerAction;
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
		this.whenClause = (ValueNode) whenClause;
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
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindStatement() throws StandardException
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
		if (isPrivilegeCollectionRequired())
		{
			compilerContext.pushCurrentPrivType(Authorizer.TRIGGER_PRIV);
			compilerContext.addRequiredTablePriv(triggerTableDescriptor);
			compilerContext.popCurrentPrivType();			
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
			
			// For before triggers, the action statement cannot contain calls
			// to procedures that modify SQL data. If the action statement 
			// contains a procedure call, this reliability will be used during
			// bind of the call statement node. 
			if(isBefore)
				compilerContext.setReliability(CompilerContext.MODIFIES_SQL_DATA_PROCEDURE_ILLEGAL);
					
			actionNode.bindStatement();

			/* when clause is always null
			if (whenClause != null)
			{
				whenClause.bind();
			}
			*/
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
	** BIND OLD/NEW TRANSITION TABLES/VARIABLES AND collect TRIGGER ACTION
	** COLUMNS referenced through REFERECING CLAUSE in CREATE TRIGGER statement
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
	** 4) collect all column references in trigger action through new/old 
	** transition variables. Information about them will be saved in
	** SYSTRIGGERS table DERBY-1482(if we are dealing with pre-10.7 db, then we
	** will not put any information about trigger action columns in the system
	** table to ensure backward compatibility). This information along with the
	** trigger columns will decide what columns from the trigger table will be
	** fetched into memory during trigger execution.
	**
	** 5) reparse the new action text
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
	**
	** More information on step 4 above. 
	** DERBY-1482 One of the work done by this method for row level triggers
	** is to find the columns which are referenced in the trigger action 
	** through the REFERENCES clause ie thro old/new transition variables.
	** This information will be saved in SYSTRIGGERS so it can be retrieved
	** during the trigger execution time. The purpose of this is to recognize
	** what columns from the trigger table should be read in during trigger
	** execution. Before these code change, during trigger execution, Derby
	** was opting to read all the columns from the trigger table even if they
	** were not all referenced during the trigger execution. This caused Derby
	** to run into OOM at times when it could really be avoided.
	**
	** We go through the trigger action text and collect the column positions
	** of all the REFERENCEd columns through new/old transition variables. We
	** keep that information in SYSTRIGGERS. At runtime, when the trigger is
	** fired, we will look at this information along with trigger columns from
	** the trigger definition and only fetch those columns into memory rather
	** than all the columns from the trigger table.
	** This is especially useful when the table has LOB columns and those
	** columns are not referenced in the trigger action and are not recognized
	** as trigger columns. For such cases, we can avoid reading large values of
	** LOB columns into memory and thus avoiding possibly running into OOM 
	** errors.
	** 
	** If there are no trigger columns defined on the trigger, we will read all
	** the columns from the trigger table when the trigger fires because no
	** specific columns were identified as trigger column by the user. The 
	** other case where we will opt to read all the columns are when trigger
	** columns and REFERENCING clause is identified for the trigger but there
	** is no trigger action column information in SYSTRIGGERS. This can happen
	** for triggers created prior to 10.7 release and later that database got
	** hard/soft-upgraded to 10.7 or higher release.
	*/
	private boolean bindReferencesClause(DataDictionary dd) throws StandardException
	{
		validateReferencesClause(dd);

        // the actions of before triggers may not reference generated columns
        if ( isBefore ) { forbidActionsOnGenCols(); }
        
		//Total Number of columns in the trigger table
		int numberOfColsInTriggerTable = triggerTableDescriptor.getNumberOfColumns();

		StringBuffer newText = new StringBuffer();
		boolean regenNode = false;
		int start = 0;
		if (isRow)
		{
			ResultColumn rc;
			
			//The purpose of following array(triggerActionColsOnly) is to
			//identify all the columns from the trigger action which are
			//referenced though old/new transition variables(in other words,
			//accessed through the REFERENCING clause section of
			//CREATE TRIGGER sql). This array will be initialized to -1 at the
			//beginning. By the end of this method, all the columns referenced
			//by the trigger action through the REFERENCING clause will have
			//their column positions in the trigger table noted in this array.
			//eg
			//CREATE TABLE table1 (c11 int, c12 int, c13 int, c14 int, c15 int);
			//CREATE TABLE table2 (c21 int, c22 int, c23 int, c24 int, c25 int);
			//CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
			//    REFERENCING OLD AS oldt NEW AS newt
			//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
			//For the trigger above, triggerActionColsOnly will finally have 
			//[-1,-1,-1,4,-1]. We will note all the entries for this array
			//which are not -1 into SYSTRIGGERS(-1 indiciates columns with
			//those column positions from the trigger table are not being
			//referenced in the trigger action through the old/new transition
			//variables.
			int[] triggerActionColsOnly = new int[numberOfColsInTriggerTable];
			for (int i=0; i < numberOfColsInTriggerTable; i++)
				triggerActionColsOnly[i]=-1;
			
			//The purpose of following array(triggerColsAndTriggerActionCols)
			//is to identify all the trigger columns and all the columns from
			//the trigger action which are referenced though old/new 
			//transition variables(in other words, accessed through the
			//REFERENCING clause section of CREATE TRIGGER sql). This array 
			//will be initialized to -1 at the beginning. By the end of this
			//method, all the columns referenced by the trigger action 
			//through the REFERENCING clause and all the trigger columns will
			//have their column positions in the trigger table noted in this
			//array.
			//eg
			//CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
			//    REFERENCING OLD AS oldt NEW AS newt
			//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
			//For the trigger above, triggerColsAndTriggerActionCols will
			//finally have [-1,2,-1,4,-1] This list will include all the
			//columns that need to be fetched into memory during trigger
			//execution. All the columns with their entries marked -1 will
			//not be read into memory because they are not referenced in the
			//trigger action through old/new transition variables and they are
			//not recognized as trigger columns.
			int[] triggerColsAndTriggerActionCols = new int[numberOfColsInTriggerTable];
			for (int i=0; i < numberOfColsInTriggerTable; i++) 
				triggerColsAndTriggerActionCols[i]=-1;
			
			if (triggerCols == null || triggerCols.size() == 0) {
				//This means that even though the trigger is defined at row 
				//level, it is either an INSERT/DELETE trigger. Or it is an
				//UPDATE trigger with no specific column(s) identified as the
				//trigger column(s). In these cases, Derby is going to read all
				//the columns from the trigger table during trigger execution.
				//eg of an UPDATE trigger with no specific trigger column(s) 
				// CREATE TRIGGER tr1 AFTER UPDATE ON table1 
				//    REFERENCING OLD AS oldt NEW AS newt
				//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
				for (int i=0; i < numberOfColsInTriggerTable; i++) {
					triggerColsAndTriggerActionCols[i]=i+1;
				}
			} else {
				//This is the most interesting case for us. If we are here, 
				//then it means that the trigger is defined at the row level
				//and a set of trigger columns are specified in the CREATE
				//TRIGGER statement. This can only happen for an UPDATE
				//trigger.
				//eg
				//CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
				//    REFERENCING OLD AS oldt NEW AS newt
				//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
				
				for (int i=0; i < triggerCols.size(); i++){
					rc = (ResultColumn)triggerCols.elementAt(i);
					ColumnDescriptor cd = triggerTableDescriptor.getColumnDescriptor(rc.getName());
					//Following will catch the case where an invalid trigger column
					//has been specified in CREATE TRIGGER statement.
					//CREATE TRIGGER tr1 AFTER UPDATE OF c1678 ON table1 
					//    REFERENCING OLD AS oldt NEW AS newt
					//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
					if (cd == null)
					{
						throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, 
																	rc.getName(),
																	tableName);
					}
					//Make a note of this trigger column's column position in
					//triggerColsAndTriggerActionCols. This will tell us that 
					//this column needs to be read in when the trigger fires.
					//eg for the CREATE TRIGGER below, we will make a note of
					//column c12's position in triggerColsAndTriggerActionCols
					//eg
					//CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
					//    REFERENCING OLD AS oldt NEW AS newt
					//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
					triggerColsAndTriggerActionCols[cd.getPosition()-1]=cd.getPosition();				
				}
			}
			//By this time, we have collected the positions of the trigger
			//columns in array triggerColsAndTriggerActionCols. Now we need
			//to start looking at the columns in trigger action to collect
			//all the columns referenced through REFERENCES clause. These
			//columns will be noted in triggerColsAndTriggerActionCols and
			//triggerActionColsOnly arrays.

			CollectNodesVisitor visitor = new CollectNodesVisitor(ColumnReference.class);
			actionNode.accept(visitor);
			Vector refs = visitor.getList();
			/* we need to sort on position in string, beetle 4324
			 */
			QueryTreeNode[] cols = sortRefs(refs, true);

			//At the end of the for loop below, we will have both arrays
			//triggerColsAndTriggerActionCols & triggerActionColsOnly
			//filled up with the column positions of the columns which are
			//either trigger columns or triger action columns which are
			//referenced through old/new transition variables. 
			//eg
			//CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
			//    REFERENCING OLD AS oldt NEW AS newt
			//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
			//For the above trigger, before the for loop below, the contents
			//of the 2 arrays will be as follows
			//triggerActionColsOnly [-1,-1,-1,-1,-1]
			//triggerColsAndTriggerActionCols [-1,2,-1,-1,-1]
			//After the for loop below, the 2 arrays will look as follows
			//triggerActionColsOnly [-1,-1,-1,4,-1]
			//triggerColsAndTriggerActionCols [-1,2,-1,4,-1]
			//If the database is at 10.6 or earlier version(meaning we are in
			//soft-upgrade mode), then we do not want to collect any 
			//information about trigger action columns. The collection and 
			//usage of trigger action columns was introduced in 10.7 DERBY-1482
			boolean in10_7_orHigherVersion =
					getLanguageConnectionContext().getDataDictionary().checkVersion(
							DataDictionary.DD_VERSION_DERBY_10_7,null);
			for (int i = 0; i < cols.length; i++)
			{
				ColumnReference ref = (ColumnReference) cols[i];
				/*
				** Only occurrences of those OLD/NEW transition tables/variables 
				** are of interest here.  There may be intermediate nodes in the 
				** parse tree that have its own RCL which contains copy of 
				** column references(CR) from other nodes. e.g.:  
				**
				** CREATE TRIGGER tt 
				** AFTER INSERT ON x
				** REFERENCING NEW AS n 
				** FOR EACH ROW
				**    INSERT INTO y VALUES (n.i), (999), (333);
				** 
				** The above trigger action will result in InsertNode that 
				** contains a UnionNode of RowResultSetNodes.  The UnionNode
				** will have a copy of the CRs from its left child and those CRs 
				** will not have its beginOffset set which indicates they are 
				** not relevant for the conversion processing here, so we can 
				** safely skip them. 
				*/
				if (ref.getBeginOffset() == -1) 
				{
					continue;
				}

				TableName tableName = ref.getTableNameNode();
				if ((tableName == null) ||
					((oldTableName == null || !oldTableName.equals(tableName.getTableName())) &&
					(newTableName == null || !newTableName.equals(tableName.getTableName()))))
				{
					continue;
				}

				if (tableName.getBeginOffset() == -1)
				{
					continue;
				}

				checkInvalidTriggerReference(tableName.getTableName());
				String colName = ref.getColumnName();

				ColumnDescriptor triggerColDesc;
				//Following will catch the case where an invalid column is
				//used in trigger action through the REFERENCING clause. The
				//following tigger is trying to use oldt.c13 but there is no
				//column c13 in trigger table table1
				//CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
				//    REFERENCING OLD AS oldt NEW AS newt
				//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14567;
				if ((triggerColDesc = triggerTableDescriptor.getColumnDescriptor(colName)) == 
	                null) {
					throw StandardException.newException(
			                SQLState.LANG_COLUMN_NOT_FOUND, tableName+"."+colName);
					}

				if (in10_7_orHigherVersion) {
					int triggerColDescPosition = triggerColDesc.getPosition();
					triggerColsAndTriggerActionCols[triggerColDescPosition-1]=triggerColDescPosition;
					triggerActionColsOnly[triggerColDescPosition-1]=triggerColDescPosition;
				}
			}

			//Now that we know what columns we need for trigger columns and
			//trigger action columns, we can get rid of remaining -1 entries
			//for the remaining columns from trigger table.
			//eg
			//CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
			//    REFERENCING OLD AS oldt NEW AS newt
			//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
			//For the above trigger, before the justTheRequiredColumns() call,
			//the content of triggerColsAndTriggerActionCols array were as
			//follows [-1, 2, -1, 4, -1]
			//After the justTheRequiredColumns() call below, 
			//triggerColsAndTriggerActionCols will have [2,4]. What this means
			//that, at run time, during trigger execution, these are the only
			//2 column positions that will be read into memory from the
			//trigger table. The columns in other column positions are not
			//needed for trigger execution.
			triggerColsAndTriggerActionCols = justTheRequiredColumns(triggerColsAndTriggerActionCols);

			for (int i = 0; i < cols.length; i++)
			{
				ColumnReference ref = (ColumnReference) cols[i];
				
				/*
				** Only occurrences of those OLD/NEW transition tables/variables 
				** are of interest here.  There may be intermediate nodes in the 
				** parse tree that have its own RCL which contains copy of 
				** column references(CR) from other nodes. e.g.:  
				**
				** CREATE TRIGGER tt 
				** AFTER INSERT ON x
				** REFERENCING NEW AS n 
				** FOR EACH ROW
				**    INSERT INTO y VALUES (n.i), (999), (333);
				** 
				** The above trigger action will result in InsertNode that 
				** contains a UnionNode of RowResultSetNodes.  The UnionNode
				** will have a copy of the CRs from its left child and those CRs 
				** will not have its beginOffset set which indicates they are 
				** not relevant for the conversion processing here, so we can 
				** safely skip them. 
				*/
				if (ref.getBeginOffset() == -1) 
				{
					continue;
				}
				
				TableName tableName = ref.getTableNameNode();
				if ((tableName == null) ||
					((oldTableName == null || !oldTableName.equals(tableName.getTableName())) &&
					(newTableName == null || !newTableName.equals(tableName.getTableName()))))
				{
					continue;
				}
					
				int tokBeginOffset = tableName.getBeginOffset();
				int tokEndOffset = tableName.getEndOffset();
				if (tokBeginOffset == -1)
				{
					continue;
				}

				regenNode = true;
				String colName = ref.getColumnName();
				int columnLength = ref.getEndOffset() - ref.getBeginOffset() + 1;

				newText.append(originalActionText.substring(start, tokBeginOffset-actionOffset));
				int colPositionInRuntimeResultSet = -1;
				ColumnDescriptor triggerColDesc = triggerTableDescriptor.getColumnDescriptor(colName);
				int colPositionInTriggerTable = triggerColDesc.getPosition();

				//This part of code is little tricky and following will help
				//understand what mapping is happening here.
				//eg
				//CREATE TRIGGER tr1 AFTER UPDATE OF c12 ON table1 
				//    REFERENCING OLD AS oldt NEW AS newt
				//    FOR EACH ROW UPDATE table2 SET c24=oldt.c14;
				//For the above trigger, triggerColsAndTriggerActionCols will 
				//have [2,4]. What this means that, at run time, during trigger
				//execution, these are the only 2 column positions that will be
				//read into memory from the trigger table. The columns in other
				//column positions are not needed for trigger execution. But
				//even though column positions in original trigger table are 2
				//and 4, their relative column positions in the columns read at
				//execution time is really [1,2]. At run time, when the trigger
				//gets fired, column position 2 from the trigger table will be
				//read as the first column and column position 4 from the
				//trigger table will be read as the second column. And those
				//relative column positions at runtime is what should be used
				//during trigger action conversion from
				//UPDATE table2 SET c24=oldt.c14
				//to
				//UPDATE table2 SET c24=org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().getOldRow().getInt(2)
				//Note that the generated code above refers to column c14 from
				//table1 by position 2 rather than position 4. Column c14's
				//column position in table1 is 4 but in the relative columns
				//that will be fetched during trigger execution, it's position
				//is 2. That is what the following code is doing.
				if (in10_7_orHigherVersion && triggerColsAndTriggerActionCols != null){
					for (int j=0; j<triggerColsAndTriggerActionCols.length; j++){
						if (triggerColsAndTriggerActionCols[j] == colPositionInTriggerTable)
							colPositionInRuntimeResultSet=j+1;
					}
				} else
					colPositionInRuntimeResultSet=colPositionInTriggerTable;

				newText.append(genColumnReferenceSQL(dd, colName, 
						tableName.getTableName(), 
						tableName.getTableName().equals(oldTableName),
						colPositionInRuntimeResultSet));
				start = tokEndOffset- actionOffset + columnLength + 2;
			}
			//By this point, we are finished transforming the trigger action if
			//it has any references to old/new transition variables.

			//Now that we know what columns we need for trigger action columns,
			//we can get rid of -1 entries for the remaining columns from
			//trigger table.
			//The final step is to put all the column positions from the 
			//trigger table of the columns which are referenced in the trigger
			//action through old/new transition variables. This information
			//will be saved in SYSTRIGGERS and will be used at trigger 
			//execution time to decide which columns need to be read into
			//memory for trigger action
			referencedColsInTriggerAction = justTheRequiredColumns(triggerActionColsOnly);
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
				int tokBeginOffset = fromTable.getTableNameField().getBeginOffset();
				int tokEndOffset = fromTable.getTableNameField().getEndOffset();
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
				//If we are dealing with statement trigger, then we will read 
				//all the columns from the trigger table since trigger will be
				//fired for any of the columns in the trigger table.
				referencedColInts= new int[numberOfColsInTriggerTable];
				for (int j=0; j < numberOfColsInTriggerTable; j++)
					referencedColInts[j]=j+1;
			}
		}

		if (referencedColsInTriggerAction != null)
			java.util.Arrays.sort(referencedColsInTriggerAction);

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
			actionNode = parseStatement(actionText, true);
		}

		return regenNode;
	}

	/*
	 * The arrary passed will have either -1 or a column position as it's 
	 * elements. If the array only has -1 as for all it's elements, then
	 * this method will return null. Otherwise, the method will create a
	 * new arrary with all -1 entries removed from the original arrary.
	 */
	private int[] justTheRequiredColumns(int[] columnsArrary) {
		int countOfColsRefedInArray = 0;
		int numberOfColsInTriggerTable = triggerTableDescriptor.getNumberOfColumns();

		//Count number of non -1 entries
		for (int i=0; i < numberOfColsInTriggerTable; i++) {
			if (columnsArrary[i] != -1)
				countOfColsRefedInArray++;
		}

		if (countOfColsRefedInArray > 0){
			int[] tempArrayOfNeededColumns = new int[countOfColsRefedInArray];
			int j=0;
			for (int i=0; i < numberOfColsInTriggerTable; i++) {
				if (columnsArrary[i] != -1)
					tempArrayOfNeededColumns[j++] = columnsArrary[i];
			}
			return tempArrayOfNeededColumns;
		} else
			return null;
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
					 sorted[j].getBeginOffset() > 
					 sorted[j+1].getBeginOffset()
					) ||
					(!isRow &&
					 ((FromBaseTable) sorted[j]).getTableNameField().getBeginOffset() > 
					 ((FromBaseTable) sorted[j+1]).getTableNameField().getBeginOffset()
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
     * Forbid references to generated columns in the actions of BEFORE triggers.
     * This is DERBY-3948, enforcing the following section of the SQL standard:
     * part 2, section 11.39 (<trigger definition>), syntax rule 12c:
     *
     * <blockquote>
     *    12) If BEFORE is specified, then:
     * :
     * c) The <triggered action> shall not contain a <field reference> that
     * references a field in the new transition variable corresponding to a
     * generated column of T. 
     * </blockquote>
     */
    private void    forbidActionsOnGenCols()
        throws StandardException
    {
        ColumnDescriptorList    generatedColumns = triggerTableDescriptor.getGeneratedColumns();
        int                                 genColCount = generatedColumns.size();

        if ( genColCount == 0 ) { return; }

        CollectNodesVisitor     visitor = new CollectNodesVisitor( ColumnReference.class );

        actionNode.accept( visitor );

        Vector                   columnRefs = visitor.getList();
        int                             colRefCount = columnRefs.size();

        for ( int crf_idx = 0; crf_idx < colRefCount; crf_idx++ )
        {
            ColumnReference     cr = (ColumnReference) columnRefs.get( crf_idx );
            String  colRefName = cr.getColumnName();
            String  tabRefName = cr.getTableName();

            for ( int gc_idx = 0; gc_idx < genColCount; gc_idx++ )
            {
                String  genColName = generatedColumns.elementAt( gc_idx ).getColumnName();

                if ( genColName.equals( colRefName ) && equals( newTableName, tabRefName ) )
                {
                    throw StandardException.newException( SQLState.LANG_GEN_COL_BEFORE_TRIG, genColName );
                }
            }
        }
    }

    /*
     * Compare two strings.
     */
    private boolean equals( String left, String right )
    {
        if ( left == null ) { return (right == null); }
        else
        {
            return left.equals( right );
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
		boolean			isOldTable,
		int				colPositionInRuntimeResultSet
	) throws StandardException
	{
		ColumnDescriptor colDesc = null;
		if ((colDesc = triggerTableDescriptor.getColumnDescriptor(colName)) == 
                null)
		{
			throw StandardException.newException(
                SQLState.LANG_COLUMN_NOT_FOUND, tabName+"."+colName);
		}

		/*
		** Generate something like this:
		**
		** 		CAST (org.apache.derby.iapi.db.Factory::
		**			getTriggerExecutionContext().getNewRow().
		**				getObject(<colPosition>) AS DECIMAL(6,2))
        **
        ** Column position is used to avoid the wrong column being
        ** selected problem (DERBY-1258) caused by the case insensitive
        ** JDBC rules for fetching a column by name.
		**
		** The cast back to the SQL Domain may seem redundant
		** but we need it to make the column reference appear
		** EXACTLY like a regular column reference, so we need
		** the object in the SQL Domain and we need to have the
		** type information.  Thus a user should be able to do 
		** something like
		**
		**		CREATE TRIGGER ... INSERT INTO T length(Column), ...
        **
        */

		DataTypeDescriptor  dts     = colDesc.getType();
		TypeId              typeId  = dts.getTypeId();

        if (!typeId.isXMLTypeId())
        {

            StringBuffer methodCall = new StringBuffer();
            methodCall.append(
                "CAST (org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().");
            methodCall.append(isOldTable ? "getOldRow()" : "getNewRow()");
            methodCall.append(".getObject(");
            methodCall.append(colPositionInRuntimeResultSet);
            methodCall.append(") AS ");

            /*
            ** getSQLString() returns <typeName> 
            ** for user types, so call getSQLTypeName in that
            ** case.
            */
            methodCall.append(
                (typeId.userType() ? 
                     typeId.getSQLTypeName() : dts.getSQLstring()));
            
            methodCall.append(") ");

            return methodCall.toString();
        }
        else
        {
            /*  DERBY-2350
            **
            **  Triggers currently use jdbc 1.2 to access columns.  The default
            **  uses getObject() which is not supported for an XML type until
            **  jdbc 4.  In the meantime use getString() and then call 
            **  XMLPARSE() on the string to get the type.  See Derby issue and
            **  http://wiki.apache.org/db-derby/TriggerImplementation , for
            **  better long term solutions.  Long term I think changing the
            **  trigger architecture to not rely on jdbc, but instead on an
            **  internal direct access interface to execution nodes would be
            **  best future direction, but believe such a change appropriate
            **  for a major release, not a bug fix.
            **
            **  Rather than the above described code generation, use the 
            **  following for XML types to generate an XML column from the
            **  old or new row.
            ** 
            **          XMLPARSE(DOCUMENT
            **              CAST (org.apache.derby.iapi.db.Factory::
            **                  getTriggerExecutionContext().getNewRow().
            **                      getString(<colPosition>) AS CLOB)  
            **                        PRESERVE WHITESPACE)
            */

            StringBuffer methodCall = new StringBuffer();
            methodCall.append("XMLPARSE(DOCUMENT CAST( ");
            methodCall.append(
                "org.apache.derby.iapi.db.Factory::getTriggerExecutionContext().");
            methodCall.append(isOldTable ? "getOldRow()" : "getNewRow()");
            methodCall.append(".getString(");
            methodCall.append(colPositionInRuntimeResultSet);
            methodCall.append(") AS CLOB) PRESERVE WHITESPACE ) ");

            return methodCall.toString();
        }
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
			// OLD TABLE and NEW TABLE not allowed for BEFORE triggers.
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
											referencedColsInTriggerAction,
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
