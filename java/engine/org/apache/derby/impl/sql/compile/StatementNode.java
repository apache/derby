/*

   Derby - Class org.apache.derby.impl.sql.compile.StatementNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.Parser;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.loader.GeneratedClass;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.lang.reflect.Modifier;

/**
 * A StatementNode represents a single statement in the language.  It is
 * the top node for any statement.
 * <p>
 * StatementNode controls the class generation for query tree nodes.
 *
 */

/*
* History:
*	5/8/97	Rick Hilleags	Moved node-name-string to child classes.
*/

public abstract class StatementNode extends QueryTreeNode
{

	/**
	 * By default, assume StatementNodes are atomic.
	 * The rare statements that aren't atomic (e.g.
	 * CALL method()) override this.
	 *
	 * @return true if the statement is atomic
	 *
	 * @exception StandardException		Thrown on error
	 */	
	public boolean isAtomic() throws StandardException
	{
		return true;
	}
	
	/**
	 * Returns whether or not this Statement requires a set/clear savepoint
	 * around its execution.  The following statement "types" do not require them:
	 *		Cursor	- unnecessary and won't work in a read only environment
	 *		Xact	- savepoint will get blown away underneath us during commit/rollback
	 * <p>
	 * ONLY CALLABLE AFTER GENERATION
	 * <P>
	 * This implementation returns true, sub-classes can override the
	 * method to not require a savepoint.
	 *
	 * @return boolean	Whether or not this Statement requires a set/clear savepoint
	 */
	public boolean needsSavepoint()
	{
		return true;
	}
	
	/**
	 * Get the name of the SPS that is used to execute this statement. Only
	 * relevant for an ExecSPSNode -- otherwise, returns null.
	 * 
	 * @return the name of the underlying sps
	 */
	public String getSPSName() {
		return null;
	}

	/**
	 * Returns the name of statement in EXECUTE STATEMENT command. Returns null
	 * for all other commands.
	 * 
	 * @return String null unless overridden for Execute Statement command
	 */
	public String executeStatementName() {
		return null;
	}

	/**
	 * Returns name of schema in EXECUTE STATEMENT command. Returns null for all
	 * other commands.
	 * 
	 * @return String schema for EXECUTE STATEMENT null for all others
	 */
	public String executeSchemaName() {
		return null;
	}
	
	/**
	 * Only DML statements have result descriptions - for all others return
	 * null. This method is overridden in DMLStatementNode.
	 * 
	 * @return null
	 * 
	 */
	public ResultDescription makeResultDescription() {
		return null;
	}

	/**
	 * Convert this object to a String. See comments in QueryTreeNode.java for
	 * how this should be done for tree printing.
	 * 
	 * @return This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "statementType: " + statementToString() + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public abstract String statementToString();
	
	/**
	 * Perform the binding operation statement.  Binding consists of
	 * permissions checking, view resolution, datatype resolution, and
	 * creation of a dependency list (for determining whether a tree or
	 * plan is still up to date).
	 *
	 * This bindStatement() method does nothing. 
	 * Each StatementNode type that can appear
	 * at the top of a tree can override this method with its
	 * own bindStatement() method that does "something".
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException
	{
	}
	
	/**
	 * Generates an optimized statement from a bound StatementNode.  Actually,
	 * it annotates the tree in place rather than generating a new tree.
	 *
	 * For non-optimizable statements (for example, CREATE TABLE),
	 * return the bound tree without doing anything.  For optimizable
	 * statements, this method will be over-ridden in the statement's
	 * root node (DMLStatementNode in all cases we know about so far).
	 *
	 * Throws an exception if the tree is not bound, or if the binding
	 * is out of date.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void  optimizeStatement() throws StandardException
	{
		
	}

	/**
	 * create the outer shell class builder for the class we will
	 * be generating, generate the expression to stuff in it,
	 * and turn it into a class.
	 */
	static final int NEED_DDL_ACTIVATION = 5;
	static final int NEED_CURSOR_ACTIVATION = 4;
	static final int NEED_PARAM_ACTIVATION = 2;
	static final int NEED_ROW_ACTIVATION = 1;
	static final int NEED_NOTHING_ACTIVATION = 0;

	abstract int activationKind();

	/* We need to get some kind of table lock (IX here) at the beginning of
	 * compilation of DMLModStatementNode and DDLStatementNode, to prevent the
	 * interference of insert/update/delete/DDL compilation and DDL execution,
	 * see beetle 3976, 4343, and $WS/language/SolutionsToConcurrencyIssues.txt
	 */
	protected TableDescriptor lockTableForCompilation(TableDescriptor td)
		throws StandardException
	{
		DataDictionary dd = getDataDictionary();

		/* we need to lock only if the data dictionary is in DDL cache mode
		 */
		if (dd.getCacheMode() == DataDictionary.DDL_MODE)
		{
			ConglomerateController  heapCC;
			TransactionController tc =
				getLanguageConnectionContext().getTransactionCompile();

			heapCC = tc.openConglomerate(td.getHeapConglomerateId(),
                                    false,
									TransactionController.OPENMODE_FORUPDATE |
									TransactionController.OPENMODE_FOR_LOCK_ONLY,
									TransactionController.MODE_RECORD,
									TransactionController.ISOLATION_SERIALIZABLE);
			heapCC.close();
			/*
			** Need to get TableDescriptor again after getting the lock, in
			** case for example, a concurrent add column thread commits
			** while we are binding.
			*/
			String tableName = td.getName();
			td = getTableDescriptor(td.getName(), getSchemaDescriptor(td.getSchemaName()));
			if (td == null)
			{
				throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, tableName);
			}
		}
		return td;
	}


	/**
	 * Do code generation for this statement.
	 *
	 * @param byteCode	the generated byte code for this statement.
	 *			if non-null, then the byte code is saved
	 *			here.
	 *
	 * @return		A GeneratedClass for this statement
	 *
	 * @exception StandardException		Thrown on error
	 */
	public GeneratedClass generate(ByteArray byteCode) throws StandardException
	{
		// start the new activation class.
		// it starts with the Execute method
		// and the appropriate superclass (based on
		// statement type, from inspecting the queryTree).

		int nodeChoice = activationKind();

		/* RESOLVE: Activation hierarchy was way too complicated
		 * and added no value.  Simple thing to do was to simply
		 * leave calling code alone and to handle here and to
		 * eliminate unnecessary classes.
		 */
		String superClass;
		switch (nodeChoice)
		{
		case NEED_CURSOR_ACTIVATION:
			superClass = ClassName.CursorActivation;
			break;
		case NEED_DDL_ACTIVATION:
			return getClassFactory().loadGeneratedClass(
				"org.apache.derby.impl.sql.execute.ConstantActionActivation", null);

		case NEED_NOTHING_ACTIVATION :
		case NEED_ROW_ACTIVATION :
		case NEED_PARAM_ACTIVATION :
			superClass = ClassName.BaseActivation;
			break;
		default :
			throw StandardException.newException(SQLState.LANG_UNAVAILABLE_ACTIVATION_NEED,
					String.valueOf(nodeChoice));
		}

		ActivationClassBuilder generatingClass = new ActivationClassBuilder(
										superClass, 
										getCompilerContext());

        /*
         * Generate the code to execute this statement.
         * Two methods are generated here: execute() and
         * fillResultSet().
         * <BR>
         * execute is called for every execution of the
         * Activation. Nodes may add code to this using
         * ActivationClassBuilder.getExecuteMethod().
         * This code will be executed every execution.
         * <BR>
         * fillResultSet is called by execute if the BaseActivation's
         * resultSet field is null and the returned ResultSet is
         * set into the the resultSet field.
         * <P>
         * The generated code is equivalent to:
         * <code>
         * public ResultSet execute() {
         * 
         *    // these two added by ActivationClassBuilder
         *    throwIfClosed("execute");
         *    startExecution();
         *    
         *    [per-execution code added by nodes]
         *    
         *    if (resultSet == null)
         *        resultSet = fillResultSet();
         *    
         *    return resultSet;
         * }
         * </code>
         */

        MethodBuilder executeMethod = generatingClass.getExecuteMethod();

        MethodBuilder mbWorker = generatingClass.getClassBuilder().newMethodBuilder(
                Modifier.PRIVATE,
                ClassName.ResultSet,
                "fillResultSet");
        mbWorker.addThrownException(ClassName.StandardException);
        
        // Generate the complete ResultSet tree for this statement.
        // This step may add statements into the execute method
        // for per-execution actions.
        generate(generatingClass, mbWorker);
        mbWorker.methodReturn();
        mbWorker.complete();

		executeMethod.pushThis();
		executeMethod.getField(ClassName.BaseActivation, "resultSet",
                ClassName.ResultSet);
        
		executeMethod.conditionalIfNull();
        
            // Generate the result set tree and store the
            // resulting top-level result set into the resultSet
            // field, as well as returning it from the execute method.
			
			executeMethod.pushThis();
			executeMethod.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null,
									 "fillResultSet", ClassName.ResultSet, 0);
            executeMethod.pushThis();
            executeMethod.swap();
            executeMethod.putField(ClassName.BaseActivation, "resultSet", ClassName.ResultSet);
            
		executeMethod.startElseCode(); // this is here as the compiler only supports ? :
			executeMethod.pushThis();
			executeMethod.getField(ClassName.BaseActivation, "resultSet", ClassName.ResultSet);
		executeMethod.completeConditional();

   		// wrap up the activation class definition
		// generate on the tree gave us back the newExpr
		// for getting a result set on the tree.
		// we put it in a return statement and stuff
		// it in the execute method of the activation.
		// The generated statement is the expression:
		// the activation class builder takes care of constructing it
		// for us, given the resultSetExpr to use.
		//   return (this.resultSet = #resultSetExpr);
		generatingClass.finishExecuteMethod(this instanceof CursorNode);

		// wrap up the constructor by putting a return at the end of it
		generatingClass.finishConstructor();

		try {
			// cook the completed class into a real class
			// and stuff it into activationClass
			GeneratedClass activationClass = generatingClass.getGeneratedClass(byteCode);

			return activationClass;
		} catch (StandardException e) {
			
			String msgId = e.getMessageId();

			if (SQLState.GENERATED_CLASS_LIMIT_EXCEEDED.equals(msgId)
					|| SQLState.GENERATED_CLASS_LINKAGE_ERROR.equals(msgId))
			{
				throw StandardException.newException(
						SQLState.LANG_QUERY_TOO_COMPLEX, e);
			}
	
			throw e;
		}
	 }
}
