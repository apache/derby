/*

   Derby - Class org.apache.derby.impl.sql.compile.ActivationClassBuilder

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

package org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.compiler.ClassBuilder;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.reference.ClassName;


import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.CodeGeneration;

import org.apache.derby.iapi.sql.execute.CursorResultSet;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.lang.reflect.Modifier;

import java.io.PrintWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Hashtable;

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
 * @author ames
 */
public class ActivationClassBuilder	extends	ExpressionClassBuilder
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
	 *    public class #className extends #superClass {
	 *		// public void reset() { return; }
	 *		public ResultSet execute() throws StandardException {
	 *			throwIfClosed("execute");
	 *			// statements must be added here
	 *		}
	 *		public #className() { super(); }
	 *    }
	 * </pre>
	 *
	 * @exception StandardException thrown on failure
	 */
	public ActivationClassBuilder (String superClass, CompilerContext cc) throws StandardException
	{
		super( superClass, (String) null, cc );
		executeMethod = beginExecuteMethod();
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
	public String getBaseClassName() {
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
	 * @param numSubqueries		The number of subqueries in the query.
	 *
	 * @return Nothing.
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

	/**
	 * By the time this is done, it has generated the following code
	 * <pre>
	 *		public ResultSet execute() throws StandardException {
	 *			throwIfClosed("execute");
	 *			// statements must be added here
	 *		}
	 *    }
	 * </pre>
	 *
	 * @exception StandardException thrown on failure
	 */
	public	MethodBuilder	beginExecuteMethod()
		throws StandardException
	{
		// create a reset method that does nothing.
		// REVISIT: this might better belong in the Activation
		// superclasses ?? not clear yet what it needs to do.

		// don't yet need a reset method here. when we do,
		// it will need to call super.reset() as well as
		// whatever it does.
		// mb = cb.newMethodBuilder(
		// 	Modifier.PUBLIC, "void", "reset");
		// mb.addStatement(javaFac.newStatement(
		//		javaFac.newSpecialMethodCall(
		//			thisExpression(),
		//			BaseActivation.CLASS_NAME,
		//			"reset", "void")));
		// mb.addStatement(javaFac.newReturnStatement());
		// mb.complete(); // there is nothing else.


		// This method is an implementation of the interface method
		// Activation - ResultSet execute()

		// create an empty execute method
		MethodBuilder mb = cb.newMethodBuilder(Modifier.PUBLIC,
			ClassName.ResultSet, "execute");
		mb.addThrownException(ClassName.StandardException);

		// put a 'throwIfClosed("execute");' statement into the execute method.
		mb.pushThis(); // instance
		mb.push("execute");
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "throwIfClosed", "void", 1);

		// call this.startExecution(), so the parent class can know an execution
		// has begun.

		mb.pushThis(); // instance
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "startExecution", "void", 0);

		return	mb;
	}

	public MethodBuilder startResetMethod() {
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
	public void finishExecuteMethod(boolean genMarkAsTopNode) {

		executeMethod.pushThis();
		executeMethod.getField(ClassName.BaseActivation, "resultSet", ClassName.ResultSet);

		/* We only call markAsTopResultSet() for selects.
		 * Non-select DML marks the top NoPutResultSet in the constructor.
		 * Needed for closing down resultSet on an error.
		 */
		if (genMarkAsTopNode)
		{
			// dup the result set to leave one for the return and one for this call
			executeMethod.dup();
			executeMethod.cast(ClassName.NoPutResultSet);
			executeMethod.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "markAsTopResultSet", "void", 0);
		}

		/* return resultSet */
		executeMethod.methodReturn();
		executeMethod.complete();

		getClassBuilder().newFieldWithAccessors("getExecutionCount", "setExecutionCount",
			Modifier.PROTECTED, true, "int");

		getClassBuilder().newFieldWithAccessors("getRowCountCheckVector", "setRowCountCheckVector",
			Modifier.PROTECTED, true, "java.util.Vector");

		getClassBuilder().newFieldWithAccessors("getStalePlanCheckInterval", "setStalePlanCheckInterval",
			Modifier.PROTECTED, true, "int");

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
	public void addCursorPositionCode() {

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
	public void rememberCursorTarget(MethodBuilder mb) {

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
	public void rememberCursor(MethodBuilder mb) {

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
		class. Then we add logic here to tidy up for ResultSet management.
	 */
	protected LocalField getCurrentSetup()
	{
		if (cdtField != null) return cdtField;

		LocalField lf = super.getCurrentSetup();

		// 3) the execute method gets a statement (prior to the return)
		//    to tell cdt to restart:
		//	  cdt.forget();

		executeMethod.getField(lf);
		executeMethod.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, "forget", "void", 0);

		// 4) a resultSetClosed method is set up to be passed to
		//    the outermost result set, if it is an open/close result set,
		//    so that cdt can be told to forget when a result set closes:
		//	  GeneratedMethod rscm; // the name is just a generated name, simpler.
		//    void rscm() {
		//		cdt.forget();
		//	  }
		MethodBuilder mb = newExprFun();
		mb.getField(lf); // the instance
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, "forget", "void", 0);
		mb.pushNull("java.lang.Object");
		mb.methodReturn();
		mb.complete();
		
		resultSetClosedMethod = mb;

		return lf;
	}

	//////////////////////////////////////////////////////////////////////////
	//
	//	NAMED PARAMETER METHODS
	//
	//////////////////////////////////////////////////////////////////////////

	/**
	 *	Generates a parameter reference. Only implemented for Filters right now.
	 *
	 *	@param	name		Parameter name.
	 *	@param	position	Parameter id.
	 *	@param	dataType	Parameter datatype.
	 *  @param	mb			The method to put the generated code into
	 *
	 *	@return	an expression representing the parameter reference.
	 *
	 * 	@exception StandardException thrown on failure
	 */
	public	void	getParameterReference( String				name,
											   int					position,
											   DataTypeDescriptor		dataType,
											   MethodBuilder		mb )
		 throws StandardException
	{
		if (SanityManager.DEBUG)
			SanityManager.NOTREACHED();
	}


	public MethodBuilder getCloseActivationMethod() {

		if (closeActivationMethod == null) {
			closeActivationMethod = cb.newMethodBuilder(Modifier.PUBLIC, "void", "closeActivationAction");
			closeActivationMethod.addThrownException("java.lang.Exception");
		}
		return closeActivationMethod;
	}
}

