/*

   Derby - Class org.apache.derby.impl.sql.compile.ExpressionClassBuilder

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
import org.apache.derby.iapi.services.compiler.JavaFactory;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.ExpressionClassBuilderInterface;

import org.apache.derby.iapi.sql.execute.ResultSetFactory;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.sql.Row;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.impl.sql.compile.OrderedColumnList;
import org.apache.derby.impl.sql.compile.ResultColumnList;
import org.apache.derby.impl.sql.execute.IndexColumnOrder;
import org.apache.derby.iapi.store.access.ColumnOrdering;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.util.ByteArray;

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.services.loader.GeneratedByteCode;
import org.apache.derby.iapi.services.loader.GeneratedMethod;

import java.lang.reflect.Modifier;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.io.FormatableArrayHolder;

import java.io.Serializable;

/**
 * ExpressionClassBuilder
 * provides an interface to satisfy generation's
 * common tasks in building classes that involve expressions.
 * This is the common superclass of ActivationClassBuilder and
 * FilterClassBuilder. See the documentation on ActivationClassBuilder.
 *
 * @author Rick	Extracted out of ActivationClassBuilder
 */
public abstract	class ExpressionClassBuilder implements ExpressionClassBuilderInterface
{
	///////////////////////////////////////////////////////////////////////
	//
	// CONSTANTS
	//
	///////////////////////////////////////////////////////////////////////

	static final protected String currentDatetimeFieldName = "cdt";

	///////////////////////////////////////////////////////////////////////
	//
	// STATE
	//
	///////////////////////////////////////////////////////////////////////

	protected ClassBuilder cb;
	protected GeneratedClass gc;
	protected int nextExprNum;
	protected int nextNonFastExpr;
	protected int nextFieldNum;
	protected MethodBuilder constructor;
	public	  CompilerContext myCompCtx;
	public MethodBuilder executeMethod; // to find it fast

	protected LocalField cdtField;

	//protected final JavaFactory javaFac;

	protected MethodBuilder resultSetClosedMethod;

	private String currentRowScanResultSetName;


	///////////////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * By the time this is done, it has constructed the following class:
	 * <pre>
	 *    public class #className extends #superClass {
	 *		public #className() { super(); }
	 *    }
	 * </pre>
	 *
	 * @exception StandardException thrown on failure
	 */
	public	ExpressionClassBuilder (String superClass, String className, CompilerContext cc ) 
		throws StandardException
	{
		int modifiers = Modifier.PUBLIC | Modifier.FINAL;

		myCompCtx = cc;
		JavaFactory javaFac = myCompCtx.getJavaFactory();

		if ( className == null ) { className = myCompCtx.getUniqueClassName(); }

		// start the class
		cb = javaFac.newClassBuilder(myCompCtx.getClassFactory(),
			getPackageName(), modifiers,
			className, superClass);

		beginConstructor();
	}

	///////////////////////////////////////////////////////////////////////
	//
	// ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Get the name of the package that the generated class will live in.
	 *
	 *	@return	name of package that the generated class will live in.
	 */
	public	abstract	String	getPackageName();

	/**
	 * Get the number of ExecRows that must be allocated
	 *
	 *	@return	number of ExecRows that must be allocated
	 *
	 * 	@exception StandardException thrown on failure
	 */
	public	abstract	int		getRowCount()
		 throws StandardException;

	/**
	 * Sets the number of subqueries under this expression
	 *
	 *
	 * 	@exception StandardException thrown on failure
	 */
	public	abstract	void 	setNumSubqueries()
		 throws StandardException;

	/**
	 * Generates an expression to refer to a named parameter.
	 *
	 *	@param	name		Parameter name
	 *	@param	position	Parameter number
	 *	@param	dataType	Parameter datatype
	 *  @param	mb			The method to put the generated code into
	 *
	 *	@return	an expression encoding a reference to the named parameter
	 *
	 * 	@exception StandardException thrown on failure
	 */
	public	abstract	void	getParameterReference( String				name,
														   int					position,
														   DataTypeDescriptor		dataType,
														   MethodBuilder mb )
		 throws StandardException;

	/**
	 * Build boiler plate for the Execute method
	 *
	 *
	 *	@return	a method builder containing boiler plate for the Execute method
	 *
	 * 	@exception StandardException thrown on failure
	 */
	public	abstract	MethodBuilder	beginExecuteMethod()
		throws StandardException;


	/**
	 * Finish up the Execute method.
	 *
	 *
	 * 	@exception StandardException thrown on failure
	 */
	public abstract		void 			finishExecuteMethod(boolean		genMarkAsTopNode )
		throws StandardException;


	///////////////////////////////////////////////////////////////////////
	//
	// ACCESSORS
	//
	///////////////////////////////////////////////////////////////////////

	/**
		Return the base class of the activation's hierarchy
		(the subclass of Object).

		This class is expected to hold methods used by all
		compilation code, such as datatype compilation code,
		e.g. getDataValueFactory.
	 */
	abstract public String getBaseClassName();

	public MethodBuilder getConstructor() {
		return constructor;
	}

	public ClassBuilder getClassBuilder() {
		return cb;
	}

	/**
	 * The execute method returns a result set that will evaluate the
	 * statement this activation class is the compiled form of.
	 * REVISIT: do we need to give the caller the ability to touch it
	 * directly, or could we wrap the alterations to it in this class?
	 */
	public MethodBuilder getExecuteMethod() {
		return executeMethod;
	}


	///////////////////////////////////////////////////////////////////////
	//
	// CONSTRUCTOR MANAGEMENT
	//
	///////////////////////////////////////////////////////////////////////

	private	final void	beginConstructor()
	{
		// create a constructor that just calls super.  
		MethodBuilder realConstructor =
			cb.newConstructorBuilder(Modifier.PUBLIC);
		realConstructor.callSuper();
		realConstructor.methodReturn();
		realConstructor.complete();

		constructor = cb.newMethodBuilder(Modifier.PUBLIC, "void", "postConstructor");
		constructor.addThrownException(ClassName.StandardException);
	}

	/**
	 * Finish the constructor by newing the array of Rows and putting a return 
	 * at the end of it.
	 *
	 * @exception StandardException thrown on failure
	 * @return	Nothing
	 */

	public void finishConstructor()
		 throws StandardException
	{
		int				numResultSets;

		/* Set the number of subqueries */
		setNumSubqueries();

		numResultSets = getRowCount();

		/* Generate the new of ExecRow[numResultSets] when there are ResultSets
		 * which return Rows.
		 */
		if (numResultSets >= 1)
		{
			addNewArrayOfRows(numResultSets);
		}

		/* Generated code is:
		 *		return;
		 */
		constructor.methodReturn();
		constructor.complete();
	}

	/**
	 * Generate the assignment for row = new ExecRow[numResultSets]
	 *
	 * @param numResultSets	The size of the array.
	 *
	 * @return Nothing.
	 */
	private void addNewArrayOfRows(int numResultSets)
	{
		/* Generated code is:
		 *		row = new ExecRow[numResultSets];
		 */

		constructor.pushThis();
		constructor.pushNewArray(ClassName.ExecRow, numResultSets);
		constructor.putField(ClassName.BaseActivation, "row", ClassName.ExecRow + "[]");
		constructor.endStatement();
	}

	///////////////////////////////////////////////////////////////////////
	//
	// ADD FIELDS TO GENERATED CLASS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Add a field declaration to the generated class
	 * 
	 * @param modifiers	The | of the modifier values such as public, static, etc.
	 * @param type		The type of the field in java language.
	 * @param name		The name of the field.
	 *
	 * @return None.
	 */
	public LocalField newFieldDeclaration(int modifiers, String type, String name)
	{
		return cb.addField(type, name, modifiers);
	}

	/**
	 * Add an arbitrarily named field to the generated class.
	 *
	 * This is used to generate fields where the caller doesn't care what
	 * the field is named.  It is especially useful for generating arbitrary
	 * numbers of fields, where the caller doesn't know in advance how many
	 * fields will be used.  For example, it is used for generating fields
	 * to hold intermediate values from expressions.
	 *
	 * @param modifiers	The | of the modifier values such as public, static, etc.
	 * @param type		The type of the field in java language.
	 *
	 * @return	The name of the new field
	 */

	public LocalField newFieldDeclaration(int modifiers, String type)
	{
		return cb.addField(type, newFieldName(), modifiers);
	}

	///////////////////////////////////////////////////////////////////////
	//
	// ADD FUNCTIONS TO GENERATED CLASS
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Activations might have need of internal functions
	 * that are not used by the result sets, but by other
	 * activation functions. Thus, we make it possible
	 * for functions to be generated directly as well
	 * as through the newExprFun interface.  newExprFun
	 * should be used when a static field pointing to the
	 * expression function is needed.
	 * <p>
	 * The generated function will generally have a generated name
	 * that can be viewed through the MethodBuilder interface.
	 * This name is generated to ensure uniqueness from other
	 * function names in the activation class. If you pass in a function
	 * name, think carefully about whether it will collide with other names.
	 *
	 * @param exprName	Name of function. Usually null, which causes us to
	 *					generate a unique name.
	 * @param returnType the return type of the function
	 * @param modifiers the modifiers on the function
	 *
	 * @see #newExprFun
	 */
	public MethodBuilder newGeneratedFun(String returnType, int modifiers) {

		return newGeneratedFun(returnType, modifiers,
							   (String[]) null);
	}

	public MethodBuilder newGeneratedFun(String returnType, 
										 int modifiers,
										 String[] params) {

		String exprName = "g".concat(Integer.toString(nextNonFastExpr++));
		return newGeneratedFun(exprName, returnType, modifiers,
							   params);

	}

	private MethodBuilder newGeneratedFun(String exprName, String returnType, 
										 int modifiers,
										 String[] params) {



		//
		// create a new method supplying the given modifiers and return Type
		// Java: #modifiers #returnType #exprName { }
		//
		MethodBuilder exprMethod;
		if (params == null)
		{
			exprMethod = cb.newMethodBuilder(modifiers, returnType, exprName);
		}
		else
		{
			exprMethod = cb.newMethodBuilder(modifiers, returnType, 
										     exprName, params);
		}

		//
		// declare it to throw StandardException
		// Java: #modifiers #returnType #exprName throws StandardException { }
		//
		exprMethod.addThrownException(ClassName.StandardException);

		return exprMethod;
	}

	/**
	 * "ExprFun"s are the "expression functions" that
	 * are specific to a given JSQL statement. For example,
	 * an ExprFun is generated to evaluate the where clause
	 * of a select statement and return a boolean result.
	 * <p>
	 *
	 * All methods return by this are expected to be called
	 * via the GeneratedMethod interface. Thus the methods
	 * are public and return java.lang.Object.
	 * <p>
	 * Once the exprfun has been created, the
	 * caller will need to add statements to it,
	 * minimally a return statement.
	 * <p>
	 * ExprFuns  return Object types, since they
	 * are invoked through reflection and thus their
	 * return type would get wrapped in an object anyway.
	 * For example: return java.lang.Boolean, not boolean.
	 */
	public MethodBuilder newExprFun()
	{
		// get next generated function 
		String exprName = "e".concat(Integer.toString(nextExprNum++));

		return newGeneratedFun(exprName, "java.lang.Object", Modifier.PUBLIC, (String[]) null);
	}

	/**
		Push an expression that is a GeneratedMethod reference to the
		passed in method. aka. a "function pointer".
	*/
	public void pushMethodReference(MethodBuilder mb, MethodBuilder exprMethod) {

		mb.pushThis(); // instance
		mb.push(exprMethod.getName()); // arg
		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.GeneratedByteCode,
				"getMethod",
				ClassName.GeneratedMethod,
				1
				);
	}

	/**
	 * Start a user expression.  The difference between a normal expression
	 * (returned by newExprFun)
	 * and a user expression is that a user expression catches all exceptions
	 * (because we don't want random exceptions thrown from user methods to
	 * propagate to the rest of the system.
	 *
	 * @param functionName	Name to give to the function. If null, we'll generate a
	 *						unique name.
	 * @param returnType	A String telling the return type from the expression
	 *
	 * @return	A new MethodBuilder
	 */
	public MethodBuilder newUserExprFun() {

		MethodBuilder mb = newExprFun();
		mb.addThrownException("java.lang.Exception");
		return mb;
	}

	///////////////////////////////////////////////////////////////////////
	//
	// CURRENT DATE/TIME SUPPORT
	//
	///////////////////////////////////////////////////////////////////////

	/**
		This utility method returns an expression for CURRENT_DATE.
		Get the expression this way, because the activation needs to 
		generate support information for CURRENT_DATE,
		that would otherwise be painful to create manually.
	 */
	public void getCurrentDateExpression(MethodBuilder mb) {
		// do any needed setup
		LocalField lf = getCurrentSetup();

		// generated Java:
		//	  this.cdt.getCurrentDate();
		mb.getField(lf);
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, "getCurrentDate", "java.sql.Date", 0);
	}

	/**
		This utility method returns an expression for CURRENT_TIME.
		Get the expression this way, because the activation needs to 
		generate support information for CURRENT_TIME,
		that would otherwise be painful to create manually.
	 */
	public void getCurrentTimeExpression(MethodBuilder mb) {
		// do any needed setup
		LocalField lf = getCurrentSetup();

		// generated Java:
		//	  this.cdt.getCurrentTime();
		mb.getField(lf);
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, "getCurrentTime", "java.sql.Time", 0);
	}

	/**
		This utility method generates an expression for CURRENT_TIMESTAMP.
		Get the expression this way, because the activation needs to 
		generate support information for CURRENT_TIMESTAMP,
		that would otherwise be painful to create manually.
	 */
	public void getCurrentTimestampExpression(MethodBuilder mb) {
		// do any needed setup
		LocalField lf = getCurrentSetup();

		// generated Java:
		//	  this.cdt.getCurrentTimestamp();
		mb.getField(lf);
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null,
			"getCurrentTimestamp", "java.sql.Timestamp", 0);
	}

	///////////////////////////////////////////////////////////////////////
	//
	// COLUMN ORDERING
	//
	///////////////////////////////////////////////////////////////////////

    /**
		These utility methods buffers compilation from the IndexColumnOrder
		class.

		They create an ordering based on their parameter, stuff that into
		the prepared statement, and then return the entry # for
		use in the generated code.

		We could write another utility method to generate code to
		turn an entry # back into an object, but so far no-one needs it.
	
		WARNING: this is a crafty method that ASSUMES that 
		you want every column in the list ordered, and that every
		column in the list is the entire actual result colunm.
		It is only useful for DISTINCT in select.	
	 */
	public FormatableArrayHolder getColumnOrdering(ResultColumnList rclist)
	{
		IndexColumnOrder[] ordering;
		int numCols = (rclist == null) ? 0 : rclist.size();
		//skip the columns which are not exclusively part of the insert list
    //ie columns with default and autoincrement. These columns will not
    //be part of ordering.
		int numRealCols = 0;
		for (int i=0; i<numCols; i++)
		{
			if (!(rclist.getResultColumn(i+1).isGeneratedForUnmatchedColumnInInsert()))
				numRealCols++;
		}

		ordering = new IndexColumnOrder[numRealCols];
		for (int i=0, j=0; i<numCols; i++)
		{
			if (!(rclist.getResultColumn(i+1).isGeneratedForUnmatchedColumnInInsert()))
			{
				ordering[j] = new IndexColumnOrder(i);
				j++;
			}
		}
		return new FormatableArrayHolder(ordering);
	}

	/**
	 * Add a column to the existing Ordering list.  Takes
	 * a column id and only adds it if it isn't in the list.
	 *
	 * @param columNum	the column to add
	 *
	 * @return the ColumnOrdering array
	 */
	public FormatableArrayHolder addColumnToOrdering(
						FormatableArrayHolder orderingHolder,
						int columnNum)
	{
		/*
		** We don't expect a lot of order by columns, so
		** linear search.
		*/
		ColumnOrdering[] ordering = (ColumnOrdering[])orderingHolder.
										getArray(ColumnOrdering.class);
		int length = ordering.length;
		for (int i = 0; i < length; i++)
		{
			if (ordering[i].getColumnId() == columnNum)
				return orderingHolder;
		}

		/*
		** Didn't find it.  Allocate a bigger array
		** and add it to the end
		*/
		IndexColumnOrder[] newOrdering = new IndexColumnOrder[length+1];
		System.arraycopy(ordering, 0, newOrdering, 0, length);
		newOrdering[length] = new IndexColumnOrder(columnNum);
		
		return new FormatableArrayHolder(newOrdering);
	}	


	public FormatableArrayHolder getColumnOrdering(OrderedColumnList  oclist) {
		int numCols = (oclist == null) ? 0 : oclist.size();

		if (numCols == 0)
		{
			return new FormatableArrayHolder(new IndexColumnOrder[0]);
		}

		return new FormatableArrayHolder(oclist.getColumnOrdering());
	}

	public int addItem(Object o) 
	{
		if (SanityManager.DEBUG)
		{
			if ((o != null) && !(o instanceof Serializable))
			{
				SanityManager.THROWASSERT(
					"o (" + o.getClass().getName() +
					") expected to be instanceof java.io.Serializable");
			}
		}
		return myCompCtx.addSavedObject(o);
	}

	///////////////////////////////////////////////////////////////////////
	//
	// Caching resuable Expressions
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Get/reuse the Expression for getting the DataValueFactory
	 */
	private Object getDVF;
	public void pushDataValueFactory(MethodBuilder mb)
	{
		// generates:
		//	   getDataValueFactory()
		//

		if (getDVF == null) {
			getDVF = mb.describeMethod(VMOpcode.INVOKEVIRTUAL,
										getBaseClassName(),
										"getDataValueFactory",
										ClassName.DataValueFactory);
		}

		mb.pushThis();
		mb.callMethod(getDVF);
	}

	///////////////////////////////////////////////////////////////////////
	//
	// RESULT SET SUPPORT
	//
	///////////////////////////////////////////////////////////////////////

	/**
		This is a utility method to get a common expression --
		"BaseActivation.getResultSetFactory()".
		<p>
		BaseActivation gets the factory from the context and
		caches it for faster retrieval.
	 */
	private Object getRSF;
	public void pushGetResultSetFactoryExpression(MethodBuilder mb) {
		// generated Java:
		//	this.getResultSetFactory()
		//
		if (getRSF == null) {
			getRSF = mb.describeMethod(VMOpcode.INVOKEVIRTUAL, getBaseClassName(),
					"getResultSetFactory",
					ClassName.ResultSetFactory);
		}
		mb.pushThis();
		mb.callMethod(getRSF);
	}

	/**
		This is a utility method to get a common expression --
		"BaseActivation.getExecutionFactory()".
		REVISIT: could the same expression objects be reused within
		the tree and have the correct java generated each time?
		<p>
		BaseActivation gets the factory from the context and
		caches it for faster retrieval. 
	 */
	private Object getEF;
	public void pushGetExecutionFactoryExpression(MethodBuilder mb) {
		if (getEF == null) {
			getEF = mb.describeMethod(VMOpcode.INVOKEVIRTUAL, getBaseClassName(),
					"getExecutionFactory",
					ClassName.ExecutionFactory);
		}

		// generated Java:
		//	this.getExecutionFactory()
		//
		mb.pushThis();
		mb.callMethod(getEF);
	}

	/**
		This utility method returns the resultSetClosed method reference that the
		activation wants called when a result set closes, to let it clean up.
		This will be null if none was needed.

		REMIND: because ObjectManager returns exceptions on its invoke() method
		and close() is not supposed to return exceptions, we may want to
		move this to be something done on open() instead of on close().
		Otherwise, we have to do try/catch/THROWASSERT in the close code,
		which looks unfriendly.
	 */
	public void pushResultSetClosedMethodFieldAccess(MethodBuilder mb) {
		if (resultSetClosedMethod != null)
			pushMethodReference(mb, resultSetClosedMethod);
		else
			mb.pushNull(ClassName.GeneratedMethod);
	}

	/**
	 * Generate a reference to the row array that
	 * all activations use.
	 * 
	 * @param eb the expression block
	 *
	 * @return expression
	 */
	//private void pushRowArrayReference(MethodBuilder mb)
	//{ 		
		// PUSHCOMPILE - cache
	//	mb.pushThis();
	//	mb.getField(ClassName.BaseActivation, "row", ClassName.ExecRow + "[]");
	//}

	/**
	 * Generate a reference to a colunm in a result set.
	 * 
	 * @param eb the expression block
	 * @param rsNumber the result set number
	 * @param colId the column number
	 *
	 * @return expression
	 */
	public void pushColumnReference(MethodBuilder mb, int rsNumber, int colId)
	{
		mb.pushThis();
		mb.push(rsNumber);
		mb.push(colId);
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "getColumnFromRow",
						ClassName.DataValueDescriptor, 2);

		//System.out.println("pushColumnReference ");
		//pushRowArrayReference(mb);
		//mb.getArrayElement(rsNumber); // instance for getColumn
		//mb.push(colId); // first arg
		//mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Row, "getColumn", ClassName.DataValueDescriptor, 1);
	}

	/**
	 * Generate a reference to the parameter value
	 * set that all activations use.
	 * 
	 * @param eb the expression block
	 *
	 * @return expression
	 */
	public void pushPVSReference(MethodBuilder mb)
	{
		// PUSHCOMPILER-WASCACHED
		mb.pushThis();
		mb.getField(ClassName.BaseActivation, "pvs", ClassName.ParameterValueSet);
	}

	///////////////////////////////////////////////////////////////////////
	//
	// CLASS IMPLEMENTATION
	//
	///////////////////////////////////////////////////////////////////////

	/*
		The first time a current datetime is needed, create the class
		level support for it.
	 */
	protected LocalField getCurrentSetup() {
		if (cdtField != null)
			return cdtField;

		// generated Java:
		// 1) the field "cdt" is created:
		//    private CurrentDatetime cdt;
		cdtField = newFieldDeclaration(
			Modifier.PRIVATE,
			ClassName.CurrentDatetime,
			currentDatetimeFieldName);

		// 2) the constructor gets a statement to init CurrentDatetime:
		//	  cdt = new CurrentDatetime();

		constructor.pushNewStart(ClassName.CurrentDatetime);
		constructor.pushNewComplete(0);
		constructor.putField(cdtField);
		constructor.endStatement();

		return cdtField;
	}

	/**
	 * generated the next field name available.
	 * these are of the form 'f#', where # is
	 * incremented each time.
	 */
	private String newFieldName()
	{
		return "f".concat(Integer.toString(nextFieldNum++));
	}


	///////////////////////////////////////////////////////////////////////
	//
	// DEBUG
	//
	///////////////////////////////////////////////////////////////////////


	///////////////////////////////////////////////////////////////////////
	//
	// DATATYPES
	//
	///////////////////////////////////////////////////////////////////////
	/**
	 * Get the TypeCompiler associated with the given TypeId
	 *
	 * @param typeId	The TypeId to get a TypeCompiler for
	 *
	 * @return	The corresponding TypeCompiler
	 *
	 */
	protected TypeCompiler getTypeCompiler(TypeId typeId)
	{
		return myCompCtx.getTypeCompilerFactory().getTypeCompiler(typeId);
	}

	///////////////////////////////////////////////////////////////////////
	//
	// GENERATE BYTE CODE
	//
	///////////////////////////////////////////////////////////////////////

	/**
	 * Take the generated class, and turn it into an
	 * actual class.
	 * <p> This method assumes, does not check, that
	 * the class and its parts are all complete.
 	 *
	 * @param savedBytes place to save generated bytes.
	 *	if null, it is ignored
	 * @exception StandardException thrown when exception occurs
	 */
	public GeneratedClass getGeneratedClass(ByteArray savedBytes) throws StandardException {
		if (gc != null) return gc;

		if (savedBytes != null)
		{
			ByteArray classBytecode = cb.getClassBytecode();

			// note: be sure to set the length since
			// the class builder allocates the byte array
			// in big chunks
			savedBytes.setBytes(classBytecode.getArray());
			savedBytes.setLength(classBytecode.getLength());
		}

	    gc =  cb.getGeneratedClass();

		return gc; // !! yippee !! here it is...
	}

	/**
	 * Get a "this" expression declared as an Activation.
	 * This is the commonly used type of the this expression.
	 *
	 */
	public void pushThisAsActivation(MethodBuilder mb) {
		// PUSHCOMPILER - WASCACHED
		mb.pushThis();
		mb.upCast(ClassName.Activation);
	}

	/**
		Generate a Null data value.
		Nothing is required on the stack, a SQL null data value
		is pushed.
	*/
	public void generateNull(MethodBuilder mb, TypeCompiler tc) {
		pushDataValueFactory(mb);
		mb.pushNull(tc.interfaceName());
		tc.generateNull(mb);
	}

	/**
		Generate a Null data value.
		The express value is required on the stack and will be popped, a SQL null data value
		is pushed.
	*/
	public void generateNullWithExpress(MethodBuilder mb, TypeCompiler tc) {
		pushDataValueFactory(mb);
		mb.swap(); // need the dvf as the instance
		mb.cast(tc.interfaceName());
		tc.generateNull(mb);
	}

	/**
		Generate a data value.
		The value is to be set in the SQL data value is required
		on the stack and will be popped, a SQL data value
		is pushed.
	*/
	public void generateDataValue(MethodBuilder mb, TypeCompiler tc, LocalField field) {
		pushDataValueFactory(mb);
		mb.swap(); // need the dvf as the instance
		tc.generateDataValue(mb, field);
	}

	
	/**
	 *generates a variable name for the rowscanresultset.
	 *This can not be a fixed name because in cases like
	 *cascade delete same activation class will be dealing 
	 * more than one RowScanResultSets for dependent tables.
	*/

	public String newRowLocationScanResultSetName()
	{
		currentRowScanResultSetName = newFieldName();
		return currentRowScanResultSetName;
	}

	// return the Name of ResultSet with the RowLocations to be modified (deleted or updated).
	public String getRowLocationScanResultSetName()
	{
		return currentRowScanResultSetName;
	}

	
}









