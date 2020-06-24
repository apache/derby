/*

   Derby - Class org.apache.derby.impl.sql.compile.ActivationClassBuilder

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

package org.apache.derby.impl.sql.compile;

import java.lang.reflect.Modifier;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.sql.compile.CodeGeneration;
import org.apache.derby.iapi.sql.compile.CompilerContext;

/**
 * ActivationClassBuilder
 * provides an interface to satisfy generation's
 * common tasks in building an activation class,
 * as well as a repository for the JavaFactory used
 * to generate the basic language constructs for the methods in the class.
 * Common tasks include the setting of a static field for each
 * expression function that gets added, the creation
 * of the execute method that gets expanded as the query tree
 * is walked, setting the superclass.
 * <p>
 * An activation class is defined for each statement. It has
 * the following basic layout: TBD
 * See the document
 * \\Jeeves\Unversioned Repository 1\Internal Technical Documents\Other\GenAndExec.doc
 * for details.
 * <p>
 * We could also verify methods as they are
 * added, to have 0 parameters, ...
 *
 */
class ActivationClassBuilder	extends	ExpressionClassBuilder
{
	///////////////////////////////////////////////////////////////////////
	//
	// CONSTANTS
	//
	///////////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////////
	//
	// STATE
	//
	///////////////////////////////////////////////////////////////////////

	private LocalField	targetResultSetField;
	private LocalField  cursorResultSetField;

	private MethodBuilder closeActivationMethod;


	///////////////////////////////////////////////////////////////////////
	//
	// CONSTRUCTOR
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * By the time this is done, it has constructed the following class:
	 * <pre>
	 *    final public class #className extends #superClass {
	 *		// public void reset() { return; }
	 *		protected ResultSet doExecute() throws StandardException {
	 *			// statements must be added here
	 *		}
     *      public #className() { super(); }
	 *    }
	 * </pre>
	 *
	 * @exception StandardException thrown on failure
	 */
	ActivationClassBuilder (String superClass, CompilerContext cc) throws StandardException
	{
		super( superClass, (String) null, cc );
	}

	///////////////////////////////////////////////////////////////////////
	//
	// ACCESSORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	  *	Get the package name that this generated class lives in
	  *
	  *	@return	package name
	  */
    public	String	getPackageName()
	{	return	CodeGeneration.GENERATED_PACKAGE_PREFIX; }

	/**
		The base class for activations is BaseActivation
	 */
	String getBaseClassName() {
	    return ClassName.BaseActivation;
	}


	/**
	  *	Get the number of ExecRows to allocate
	  *
	  * @exception StandardException thrown on failure
	  *	@return	package name
	  */
	public	int		getRowCount()
		 throws StandardException
	{
		return	myCompCtx.getNumResultSets();
	}

	/**
	 * Generate the assignment for numSubqueries = x
	 *
	 * @exception StandardException thrown on failure
	 */
	public	 void	setNumSubqueries()
	{
		int				numSubqueries = myCompCtx.getNumSubquerys();

		// If there are no subqueries then
		// the field is set to the correctly
		// value (0) by java.
		if (numSubqueries == 0)
			return;

		/* Generated code is:
		 *		numSubqueries = x;
		 */
		constructor.pushThis();
		constructor.push(numSubqueries);
		constructor.putField(ClassName.BaseActivation, "numSubqueries", "int");
		constructor.endStatement();
	}


	///////////////////////////////////////////////////////////////////////
	//
	// EXECUTE METHODS
	//
	///////////////////////////////////////////////////////////////////////

	MethodBuilder startResetMethod() {
		MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC,
			"void", "reset");

		mb.addThrownException(ClassName.StandardException);
		mb.pushThis();
		mb.callMethod(VMOpcode.INVOKESPECIAL, ClassName.BaseActivation, "reset", "void", 0);


		return mb;
	}

	/**
	 * An execute method always ends in a return statement, returning
	 * the result set that has been constructed.  We want to
	 * do some bookkeeping on that statement, so we generate
	 * the return given the result set.

	   Upon entry the only word on the stack is the result set expression
	 */
	void finishExecuteMethod() {
//IC see: https://issues.apache.org/jira/browse/DERBY-5947

        if (executeMethod != null) {
            executeMethod.methodReturn();
            executeMethod.complete();
        }

		if (closeActivationMethod != null) {
			closeActivationMethod.methodReturn();
			closeActivationMethod.complete();
		}
	}

	///////////////////////////////////////////////////////////////////////
	//
	// CURSOR SUPPORT
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Updatable cursors
	 * need to add a getter method for use in BaseActivation to access
	 * the result set that identifies target rows for a positioned
	 * update or delete.
	 * <p>
	 * The code that is generated is:
	 * <pre><verbatim>
	 *  public CursorResultSet getTargetResultSet() {
	 *	    return targetResultSet;
	 *  }
	 *
	 *  public CursorResultSet getCursorResultSet() {
	 *		return cursorResultSet;
	 *  }
	 * </verbatim></pre>
	 *
	 */
	void addCursorPositionCode() {

		// the getter
		// This method is an implementation of the interface method
		// CursorActivation - CursorResultSet getTargetResultSet()
		MethodBuilder getter = cb.newMethodBuilder(Modifier.PUBLIC, 
			ClassName.CursorResultSet, "getTargetResultSet");

		getter.getField(targetResultSetField);
		getter.methodReturn();
		getter.complete();

		// This method is an implementation of the interface method
		// CursorActivation - CursorResultSet getCursorResultSet()

		getter = cb.newMethodBuilder(Modifier.PUBLIC, 
			ClassName.CursorResultSet, "getCursorResultSet");

		getter.getField(cursorResultSetField);
		getter.methodReturn();
		getter.complete();
	}

	/**
	 * Updatable cursors
	 * need to add a field and its initialization
	 * for use in BaseActivation to access the result set that
	 * identifies target rows for a positioned update or delete.
	 * <p>
	 * The code that is generated is:
	 * <pre><verbatim>
	 *  private CursorResultSet targetResultSet;
	 *
	 * </verbatim></pre>
	 *
	 * The expression that is generated is:
	 * <pre><verbatim>
	 *  (ResultSet) (targetResultSet = (CursorResultSet) #expression#)
	 * </verbatim></pre>
	 *
	 */
	void rememberCursorTarget(MethodBuilder mb) {

		// the field
		targetResultSetField = cb.addField(ClassName.CursorResultSet,
					"targetResultSet",
					Modifier.PRIVATE);

		mb.cast(ClassName.CursorResultSet);
		mb.putField(targetResultSetField);
		mb.cast(ClassName.NoPutResultSet);
	}

	/**
	 * Updatable cursors
	 * need to add a field and its initialization
	 * for use in BaseActivation to access the result set that
	 * identifies cursor result rows for a positioned update or delete.
	 * <p>
	 * The code that is generated is:
	 * <pre><verbatim>
	 *  private CursorResultSet cursorResultSet;
	 *
	 * </verbatim></pre>
	 *
	 * The expression that is generated is:
	 * <pre><verbatim>
	 *  (ResultSet) (cursorResultSet = (CursorResultSet) #expression#)
	 * </verbatim></pre>

       The expression must be the top stack word when this method is called.
	 *
	 */
	void rememberCursor(MethodBuilder mb) {

		// the field
		cursorResultSetField = cb.addField(ClassName.CursorResultSet,
					"cursorResultSet",
					Modifier.PRIVATE);

		mb.cast(ClassName.CursorResultSet);
		mb.putField(cursorResultSetField);
		mb.cast(ClassName.ResultSet);
	}

	///////////////////////////////////////////////////////////////////////
	//
	// CURRENT DATE/TIME SUPPORT
	//
	///////////////////////////////////////////////////////////////////////

	/*
		The first time a current datetime is needed, create the class
		level support for it. The first half of the logic is in our parent
//IC see: https://issues.apache.org/jira/browse/DERBY-1700
		class.
	 */
    @Override
	protected LocalField getCurrentSetup()
	{
		if (cdtField != null) return cdtField;

		LocalField lf = super.getCurrentSetup();

		// 3) the execute method gets a statement (prior to the return)
		//    to tell cdt to restart:
		//	  cdt.forget();

//IC see: https://issues.apache.org/jira/browse/DERBY-5947
        MethodBuilder execute = getExecuteMethod();
        execute.getField(lf);
        execute.callMethod(
                VMOpcode.INVOKEVIRTUAL, (String) null, "forget", "void", 0);

		return lf;
	}

	MethodBuilder getCloseActivationMethod() {

		if (closeActivationMethod == null) {
			closeActivationMethod = cb.newMethodBuilder(Modifier.PUBLIC, "void", "closeActivationAction");
			closeActivationMethod.addThrownException("java.lang.Exception");
		}
		return closeActivationMethod;
	}
}

