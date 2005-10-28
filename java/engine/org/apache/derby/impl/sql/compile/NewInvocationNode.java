/*

   Derby - Class org.apache.derby.impl.sql.compile.NewInvocationNode

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

import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;


import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.util.JBitSet;

import org.apache.derby.catalog.AliasInfo;

import java.lang.reflect.Member;
import java.lang.reflect.Modifier;

import java.util.Vector;
import java.util.Enumeration;

/**
 * A NewInvocationNode represents a new object() invocation.
 *
 * @author Jerry Brenner
 */
public class NewInvocationNode extends MethodCallNode
{
	// Whether or not to do a single instantiation
	private boolean singleInstantiation = false;

	private boolean delimitedIdentifier;

	/**
	 * Initializer for a NewInvocationNode
	 *
	 * @param javaClassName		The full package.class name of the class
	 * @param parameterList		The parameter list for the constructor
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
					Object javaClassName,
					Object params,
					Object delimitedIdentifier)
		throws StandardException
	{
		super.init("<init>");
		addParms((Vector) params);

		this.javaClassName = (String) javaClassName;
		this.delimitedIdentifier =
				 ((Boolean) delimitedIdentifier).booleanValue();
	}

	/**
	 * Mark this node as only needing to
	 * to a single instantiation.  (We can
	 * reuse the object after newing it.)
	 *
	 * @return Nothing.
	 */
	void setSingleInstantiation()
	{
		singleInstantiation = true;
	}

	/**
	  *	Get the resolved Classes of our parameters
	  *
	  *	@return	the Classes of our parameters
	  */
	public	Class[]	getMethodParameterClasses() 
	{ 
		ClassInspector ci = getClassFactory().getClassInspector();

		Class[]	parmTypeClasses = new Class[methodParms.length];
		for (int i = 0; i < methodParms.length; i++)
		{
			String className = methodParameterTypes[i];
			try
			{
				parmTypeClasses[i] = ci.getClass(className);
			}
			catch (ClassNotFoundException cnfe)
			{
				/* We should never get this exception since we verified 
				 * that the classes existed at bind time.  Just return null.
				 */
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected exception - " + cnfe);
				}
				return null;
			}
		}

		return parmTypeClasses;
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public JavaValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector aggregateVector) 
			throws StandardException
	{
		bindParameters(fromList, subqueryList, aggregateVector);

		javaClassName = verifyClassExist(javaClassName, !delimitedIdentifier);
		/*
		** Get the parameter type names out of the parameters and put them
		** in an array.
		*/
		String[]	parmTypeNames = getObjectSignature();
		boolean[]	isParam = getIsParam();
		ClassInspector classInspector = getClassFactory().getClassInspector();

		/*
		** Find the matching constructor.
		*/
		try
		{
			/* First try with built-in types and mappings */
			method = classInspector.findPublicConstructor(javaClassName,
											parmTypeNames, null, isParam);

			/* If no match, then retry to match any possible combinations of
			 * object and primitive types.
			 */
			if (method == null)
			{
				String[] primParmTypeNames = getPrimitiveSignature(false);
				method = classInspector.findPublicConstructor(javaClassName,
								parmTypeNames, primParmTypeNames, isParam);
			}
		}
		catch (ClassNotFoundException e)
		{
			/*
			** If one of the classes couldn't be found, just act like the
			** method couldn't be found.  The error lists all the class names,
			** which should give the user enough info to diagnose the problem.
			*/
			method = null;
		}

		if (method == null)
		{
			/* Put the parameter type names into a single string */
			String	parmTypes = "";
			for (int i = 0; i < parmTypeNames.length; i++)
			{
				if (i != 0)
					parmTypes += ", ";
				parmTypes += (parmTypeNames[i].length() != 0 ?
								parmTypeNames[i] :
								MessageService.getTextMessage(
									SQLState.LANG_UNTYPED)
									);
			}

			throw StandardException.newException(SQLState.LANG_NO_CONSTRUCTOR_FOUND, 
													javaClassName,
												 	parmTypes);
		}

		methodParameterTypes = classInspector.getParameterTypes(method);

		for (int i = 0; i < methodParameterTypes.length; i++)
		{
			if (classInspector.primitiveType(methodParameterTypes[i]))
				methodParms[i].castToPrimitive(true);
		}

		/* Set type info for any null parameters */
		if ( someParametersAreNull() )
		{
			setNullParameterInfo(methodParameterTypes);
		}

		/* Constructor always returns an object of type javaClassName */
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(javaClassName.equals(classInspector.getType(method)),
				"Constructor is wrong type, expected " + javaClassName + 
				" actual is " + classInspector.getType(method));
		}
	 	setJavaTypeName( javaClassName );

		return this;
	}

	/**
	 * Categorize this predicate.  Initially, this means
	 * building a bit map of the referenced tables for each predicate.
	 * If the source of this ColumnReference (at the next underlying level) 
	 * is not a ColumnReference or a VirtualColumnNode then this predicate
	 * will not be pushed down.
	 *
	 * For example, in:
	 *		select * from (select 1 from s) a (x) where x = 1
	 * we will not push down x = 1.
	 * NOTE: It would be easy to handle the case of a constant, but if the
	 * inner SELECT returns an arbitrary expression, then we would have to copy
	 * that tree into the pushed predicate, and that tree could contain
	 * subqueries and method calls.
	 * RESOLVE - revisit this issue once we have views.
	 *
	 * @param referencedTabs	JBitSet with bit map of referenced FromTables
	 * @param simplePredsOnly	Whether or not to consider method
	 *							calls, field references and conditional nodes
	 *							when building bit map
	 *
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode.
	 * @exception StandardException		Thrown on error
	 */
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		/* We stop here when only considering simple predicates
		 *  as we don't consider new opeators when looking
		 * for null invariant predicates.
		 */
		if (simplePredsOnly)
		{
			return false;
		}

		boolean pushable = true;

		pushable = pushable && super.categorize(referencedTabs, simplePredsOnly);

		return pushable;
	}

	/**
	 * Build a JBitSet of all of the tables that we are
	 * correlated with.
	 *
	 * @param correlationMap	The JBitSet of the tables that we are correlated with.
	 *
	 * @return Nothing.
	 */
	void getCorrelationTables(JBitSet correlationMap)
		throws StandardException
	{
		CollectNodesVisitor getCRs = new CollectNodesVisitor(ColumnReference.class);
		super.accept(getCRs);
		Vector colRefs = getCRs.getList();
		for (Enumeration e = colRefs.elements(); e.hasMoreElements(); )
		{
			ColumnReference ref = (ColumnReference)e.nextElement();
			if (ref.getCorrelated())
			{
				correlationMap.set(ref.getTableNumber());
			}
		}
	}

	/**
	 * Is this class assignable to the specified class?
	 * This is useful for the VTI interface where we want to see
	 * if the class implements java.sql.ResultSet.
	 *
	 * @param toClassName	The java class name we want to assign to
	 *
	 * @return boolean		Whether or not this class is assignable to
	 *						the specified class
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected boolean assignableTo(String toClassName) throws StandardException
	{
		ClassInspector classInspector = getClassFactory().getClassInspector();
		return classInspector.assignableTo(javaClassName, toClassName);
	}


	/**
	 * Is this class have a public method with the specified signiture
	 * This is useful for the VTI interface where we want to see
	 * if the class has the option static method for returning the
	 * ResultSetMetaData.
	 *
	 * @param methodName	The method name we are looking for
	 * @param staticMethod	Whether or not the method we are looking for is static
	 *
	 * @return Member		The Member representing the method (or null
	 *						if the method doesn't exist).
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected Member findPublicMethod(String methodName, boolean staticMethod)
		throws StandardException
	{
		Member publicMethod;

		/*
		** Get the parameter type names out of the parameters and put them
		** in an array.
		*/
		String[]	parmTypeNames = getObjectSignature();
		boolean[]	isParam = getIsParam();

		ClassInspector classInspector = getClassFactory().getClassInspector();

		try
		{
			publicMethod = classInspector.findPublicMethod(javaClassName, methodName,
											   parmTypeNames, null, isParam, staticMethod, false);

			/* If no match, then retry to match any possible combinations of
			 * object and primitive types.
			 */
			if (publicMethod == null)
			{
				String[] primParmTypeNames = getPrimitiveSignature(false);
				publicMethod = classInspector.findPublicMethod(javaClassName, 
										methodName, parmTypeNames,
										primParmTypeNames, isParam, staticMethod, false);
			}
		}
		catch (ClassNotFoundException e)
		{
			/* We should always be able to find the class at this point
			 * since the protocol is to check to see if it exists
			 * before checking for a method off of it.  Anyway, just return
			 * null if the class doesn't exist, since the method doesn't
			 * exist in that case.
			 */
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT(
					"Unexpected ClassNotFoundException for javaClassName");
			}
			return null;
		}

		return	publicMethod;
	}

	/**
	 * Do code generation for this method call
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		/* If this node is for an ungrouped aggregator, 
		 * then we generate a conditional
		 * wrapper so that we only new the aggregator once.
		 *		(fx == null) ? fx = new ... : fx
		 */
		LocalField objectFieldLF = null;
		if (singleInstantiation)
		{
			/* Declare the field */
			objectFieldLF = acb.newFieldDeclaration(Modifier.PRIVATE, javaClassName);

			// now we fill in the body of the conditional

			mb.getField(objectFieldLF);
			mb.conditionalIfNull();
		}

		mb.pushNewStart(javaClassName);
		int nargs = generateParameters(acb, mb);
		mb.pushNewComplete(nargs);

		if (singleInstantiation) {

			  mb.putField(objectFieldLF);
			mb.startElseCode();
			  mb.getField(objectFieldLF);
			mb.completeConditional();
		}
	}
}
