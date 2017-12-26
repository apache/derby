/*

   Derby - Class org.apache.derby.iapi.services.loader.ClassInspector

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

package org.apache.derby.iapi.services.loader;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.shared.common.reference.SQLState;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
	Methods to find out relationships between classes and methods within a class.
	All class names within this interface are treated as java language class names,
	e.g. int, COM.foo.Myclass, int[], java.lang.Object[]. That is java internal
	class names as defined in the class file format are not understood.
*/
public class ClassInspector
{
	private static final String[] primTypeNames =
		{"boolean", "byte", "char", "short", "int", "long", "float", "double"};

	// collect these as static, instead of each time allocates these new
	// Strings for every method resolution

	private static final String[] nonPrimTypeNames =
		{"java.lang.Boolean", "java.lang.Byte", "java.lang.Character",
		 "java.lang.Short", "java.lang.Integer", "java.lang.Long",
		 "java.lang.Float", "java.lang.Double"};

    private static final String OBJECT_TYPE_NAME = "java.lang.Object";
    private static final String STRING_TYPE_NAME = "java.lang.String";
    private static final String BIGDECIMAL_TYPE_NAME = "java.math.BigDecimal";

	private final ClassFactory cf;

	/**
		DO NOT USE! use the method in ClassFactory.
	*/
	public ClassInspector(ClassFactory cf) {
		this.cf = cf;
	}
	
	/**
	 * Is the given object an instance of the named class?
	 *
	 * @param className	The name of the class
	 * @param obj		The object to test to see if it's an instance
	 *			of the named class
	 *
	 * @return	true if obj is an instanceof className, false if not
	 */
	public boolean instanceOf(String className, Object obj)
		throws ClassNotFoundException
	{
		Class<?> clazz = getClass(className);
		// is className an untyped null
		if (clazz == null)
			return false;

		return clazz.isInstance(obj);
	}

	/**
	 * Is one named class assignable to another named class or interface?
	 *
	 * @param fromClassName	The name of the class to be assigned
	 * @param toClassName	The name of the class to be assigned to
	 *
	 * @return	true if an object of type fromClass can be assigned to an
	 *			object of type toClass, false if not.
	 */
	public boolean assignableTo(String fromClassName, String toClassName)
	{
		try
		{
			Class<?> toClass = getClass(toClassName);
			// is toClass an untyped null
			if (toClass == null) {
				return false;
			}

			Class<?> fromClass = getClass(fromClassName);

			// is fromClass an untyped null
			if (fromClass == null)
				return !toClass.isPrimitive() || (toClass == Void.TYPE);


			return toClass.isAssignableFrom(fromClass);
		}
		catch (ClassNotFoundException cnfe)
		{
			/* If either class can't be found, they can't be assigned */
			return false;
		}
	}

	/**
	 * Does the named class exist, and is it accessible?
	 *
	 * @param className	The name of the class to test for existence
	 *
	 * @return	true if the class exists and is accessible, false if not
	 */
	public boolean accessible(String className)
		throws ClassNotFoundException
	{
		Class<?> theClass = getClass(className);
		if (theClass == null)
			return false;

		/* Classes must be public to be accessible */
		if (! Modifier.isPublic(theClass.getModifiers()))
			return false;

		return true;
	}


	/**
	 * Get the Java name of the return type from a Member representing
	 * a method or the type of a Member representing a field.
	 *
	 * @param member		A Member representing the method for
	 *						which we want the return type.
	 *
	 * @return	A Java-language-style string describing the return type of
	 *			the method (for example, it returns "int" instead of "I".
	 */
	public String getType(Member member)
	{
		Class type;

		if (member instanceof Method)
			type = ((Method) member).getReturnType();
		else if (member instanceof Field)
			type = ((Field) member).getType();
		else if (member instanceof Constructor)
			type = ((Constructor) member).getDeclaringClass();
		else
			type = Void.TYPE;

		return ClassInspector.readableClassName(type);
	}


	/**
	 * Find a public method that implements a given signature.
	 * The signature is given using the full Java class names of the types.
	 <BR>
	 * A untyped null parameter is indicated by passing in an empty string ("")
	 * as its class name.
	 <BR>
	 If receiverType represents an interface then the methods of java.lang.Object
	 are included in the candidate list.
	 <BR>
	 If the caller is simply checking to see that a public method with the
	 specified name exists, regardless of the signature, exists, then the
	 caller should pass in a null for parmTypes.  (This is useful for checking
	 the validity of a method alias when creating one.)
	 <BR>
	 We use a two-pass algorithm to resolve methods.  In the first pass, we
	 use all "object" types to try to match a method.  If this fails, in the
	 second pass, an array of "primitive" types (if the parameter has one,
	 otherwise the same object type is used) is passed in, as well as the
	 "object" type array.  For each parameter of a method, we try to match it
	 against either the "object" type, or the "primitive" type.  Of all the
	 qualified candidate methods found, we choose the closest one to the input
	 parameter types.  This involves comparing methods whose parameters are
	 mixed "object" and "primitive" types in the second pass.  This is
	 eventually handled in classConvertableFromTo.
	 *
	 * @param receiverType	The class name of the receiver
	 * @param methodName	The name of the method
	 * @param parmTypes		An array of class names representing the
	 *						parameter types.  Pass a zero-element array if
	 *						there are no parameters.  Pass a null if it is
	 *						okay to match any signature.
	 * @param primParmTypes This is used in the second pass of the two-pass
	 *						method resolution algorithm.  Use primitive type
	 *						if it has one, otherwise use same object type
	 * @param isParam		Array of booleans telling whether parameter is a ?.
	 * @param staticMethod	Find a static method.
	   @param repeatLastParameter If true the last parameter may be repeated any number of times (total count must be greater than one).
	   If false the last parameter is matched as usual. This also requires an exact match on the last parameter type.
	 *
	 * @return	A Member representing the matching method.  Returns null
	 *			if no such method.
	 *
	 * @exception ClassNotFoundException	One or more of the classes does
	 *										not exist.
	 * @exception StandardException			Thrown on ambiguous method invocation.
	 *
	 * @see	Member
	 * @see Modifier
	 */
	public Member findPublicMethod
        (
         String receiverType,
         String methodName,
         String[] parmTypes,
         String[] primParmTypes,
         boolean[] isParam,
         boolean staticMethod,
         boolean repeatLastParameter,
         boolean hasVarargs
         )
        throws ClassNotFoundException, StandardException
	{
		Class<?> receiverClass = getClass(receiverType);
		if (receiverClass == null)
			return null;

		// primitives don't have methods
		// note that arrays do since they are objects they have
		// all the methods of java.lang.Object
		if (receiverClass.isPrimitive()) {
			return null;
		}

		// if parmTypes is null, then the caller is simply 
		// looking to see if any public method with the
		// specified name exists, regardless of its signature
		if (parmTypes == null) {
			Method[] methods = receiverClass.getMethods();
			
			for (int index = 0; index < methods.length; index++) {
				if (staticMethod) {
					if (!Modifier.isStatic(methods[index].getModifiers())) {
						continue;
					}
				}

                // If the routine was declared to be varargs, then we eliminate
                // all non-varargs methods from consideration
                if ( hasVarargs && !isVarArgsMethod( methods[index] ) ) { continue; }

				if (methodName.equals(methods[index].getName())) {
					// We found a match
					return methods[index];
				}
			}
			// No match
			return null;
		}

		// convert the parameter types to classes
		Class[] paramClasses = new Class[parmTypes.length];
		Class[] primParamClasses = null;
		if (primParmTypes != null)
			primParamClasses = new Class[primParmTypes.length];
		for (int i = 0; i < paramClasses.length; i++)
		{
			paramClasses[i] = getClass(parmTypes[i]);
			if (primParmTypes == null)
				continue;
			if (primParmTypes[i].equals(parmTypes[i]))  // no separate primitive
				primParamClasses[i] = null;
			else
				primParamClasses[i] = getClass(primParmTypes[i]);
		}

		// no overloading possible if there are no arguments, so perform
		// an exact match lookup.
		if (paramClasses.length == 0) {

			try {
				Method method = receiverClass.getMethod(methodName, paramClasses);

				if (staticMethod) {
					if (!Modifier.isStatic(method.getModifiers()))
						return null;
				}

				return method;

				} catch (NoSuchMethodException nsme2) {


					// if we are an interface then the method could be defined on Object
					if (!receiverClass.isInterface())
						return null;
				}
		}

		// now the tricky method resolution
		Member[] methodList = receiverClass.getMethods();
		// if we have an interface we need to add the methods of Object into the mix
		if (receiverClass.isInterface()) {

			Member[] objectMethods = java.lang.Object.class.getMethods();
			if (methodList.length == 0) {
				methodList = objectMethods;
			} else {
				Member[] set = new Member[methodList.length + objectMethods.length];
				System.arraycopy(methodList, 0, set, 0, methodList.length);
				System.arraycopy(objectMethods, 0, set, methodList.length, objectMethods.length);
				methodList = set;
			}
		}

		return resolveMethod
            (
             receiverClass, methodName, paramClasses,
             primParamClasses, isParam, staticMethod, repeatLastParameter, methodList,
             hasVarargs
             );
	}


	/**
	 * Find a public field  for a class.
	   This follows the semantics of the java compiler for locating a field.
	   This means if a field fieldName exists in the class with package, private or
	   protected then an error is raised. Even if the field hides a field fieldName
	   in a super-class/super--interface. See the JVM spec on fields.
	 *
	 * @param receiverType	The class name of the receiver
	 * @param fieldName		The name of the field
	 * @param staticField	Find a static field
	 *
	 * @return	A Member representing the matching field.  
	 * @exception StandardException	Class or field does not exist or is not public or a security exception.
	 *
	 * @see	Member
	 * @see Modifier
	 */
	public Member findPublicField(String receiverType,
								String fieldName,
								boolean staticField)
					throws StandardException
	{

		Exception e = null;
		try {

			Class<?> receiverClass = getClass(receiverType);
			if (receiverClass == null)
				return null;
			if (receiverClass.isArray() || receiverClass.isPrimitive()) {
				// arrays don't have fields (the fake field 'length' is not returned here)
				return null;
			}
  
			int modifier = staticField ? (Modifier.PUBLIC | Modifier.STATIC) : Modifier.PUBLIC;

			// Look for a public field first
			Field publicField = receiverClass.getField(fieldName);

			if ((publicField.getModifiers() & modifier) == modifier)
			{
				/*
					If the class is an interface then we avoid looking for a declared field
					that can hide a super-class's public field and not be accessable. This is because
					a interface's fields are always public. This avoids a security check.
				*/
				if (receiverClass.isInterface() || (publicField.getDeclaringClass().equals(receiverClass)))
					return publicField;

				/*
					Now check to see if there is a declared field that hides the public field.
				*/

				try {

					Field declaredField = receiverClass.getDeclaredField(fieldName);

					if (SanityManager.DEBUG) {

						if ((declaredField.getModifiers() & Modifier.PUBLIC) == Modifier.PUBLIC)
							SanityManager.THROWASSERT("declared field not expected to be public here " + declaredField);
					}

				} catch (NoSuchFieldException nsfe) {

					// no field hides the public field in the super class
					return publicField;
				}
			}

		} catch (ClassNotFoundException cnfe) {
			e = cnfe;
		} catch (NoSuchFieldException nsfep) {
			e = nsfep;
		} catch (SecurityException se) {
			e = se;
		}

		throw StandardException.newException(
			staticField ? SQLState.LANG_NO_STATIC_FIELD_FOUND : SQLState.LANG_NO_FIELD_FOUND, 
								e, fieldName, receiverType);
	}

	/**
	 * Find a public constructor that implements a given signature.
	 * The signature is given using the full Java class names of the types.
	 <BR>
	 * A untyped null parameter is indicated by passing in an empty string ("")
	 * as its class name. 
	 *
	 * @param receiverType	The class name of the receiver
	 * @param parmTypes		An array of class names representing the
	 *						parameter types.  Pass a zero-element array if
	 *						there are no parameters.
	 * @param primParmTypes This is used in the second pass of the two-pass
	 *						method resolution algorithm.  Use primitive type
	 *						if it has one, otherwise use same object type
	 * @param isParam		Array of booleans telling whether parameter is a ?.
	 *
	 * @return	A Member representing the matching constructor.  Returns null
	 *			if no such constructor.
	 *
	 * @exception ClassNotFoundException	One or more of the classes does
	 *										not exist.
	 * @exception StandardException			Thrown on ambiguous constructor invocation.
	 *
	 * @see	Member
	 * @see Modifier
	 */
	public Member findPublicConstructor(String receiverType,
									String[] parmTypes,
									String[] primParmTypes,
									boolean[] isParam)
						throws ClassNotFoundException, StandardException 
	{
		Class<?> receiverClass = getClass(receiverType);
		if (receiverClass == null)
			return null;

		// arrays, primitives, and interfaces do not have constructors
		if (receiverClass.isArray() || receiverClass.isPrimitive() || receiverClass.isInterface()) {
			return null;
		}

		// convert the parameter types to classes
		Class[] paramClasses = new Class[parmTypes.length];
		Class[] primParamClasses = null;
		if (primParmTypes != null)
			primParamClasses = new Class[primParmTypes.length];
		boolean unknownParameters = false;
		for (int i = 0; i < paramClasses.length; i++) {
			paramClasses[i] = getClass(parmTypes[i]);
			if (paramClasses[i] == null)
				unknownParameters = true;
			if (primParmTypes == null)
				continue;
			if (primParmTypes[i].equals(parmTypes[i]))  // no separate primitive
				primParamClasses[i] = null;
			else
				primParamClasses[i] = getClass(primParmTypes[i]);
		}

		try {

			if (!unknownParameters && (primParmTypes == null)) {
				// look for an exact match for first pass
				Member method = receiverClass.getConstructor(paramClasses);

				return method;
			}

		} catch (NoSuchMethodException nsme) {

			// no overloading possible if there are no arguments
			if (paramClasses.length == 0)
				return null;

			// now the tricky method resolution
		}

		// name is only used for debugging
		return resolveMethod
            (
             receiverClass, "<init>", paramClasses, 
             primParamClasses, isParam, false, false,
             receiverClass.getConstructors(),
             false
             );
	}

	/**
	 * Given an implementation of a parameterized interface, return
     * the bounds on the type variables. May return null if type resolution
     * fails.
	 */
	public Class[][] getTypeBounds( Class parameterizedInterface, Class implementation )
        throws StandardException
	{
        if ( implementation == null ) { return null; }

        Type[]  genericInterfaces = implementation.getGenericInterfaces();
        for ( Type genericInterface : genericInterfaces )
        {
            //
            // Look for the generic interface whose raw type is the
            // parameterized interface we're interested in.
            //
            if ( genericInterface instanceof ParameterizedType )
            {
                ParameterizedType   pt = (ParameterizedType) genericInterface;
                Type    rawType = pt.getRawType();

                // found it!
                if ( parameterizedInterface == rawType )
                {
                    return findTypeBounds( pt );
                }
            }
        }

        // couldn't find the interface we're looking for. check our superclass.
        return getTypeBounds( parameterizedInterface, implementation.getSuperclass() );
    }

	/**
	 * Return true if the method or constructor supports varargs.
	 */
	public boolean  isVarArgsMethod( Member member )
	{
        if (member instanceof Method) {
            return ((Method) member).isVarArgs();
        } else if (member instanceof Constructor) {
            return ((Constructor) member).isVarArgs();
        } else {
            return false;
        }
    }

	/**
	 * Given an implementation of a parameterized interface, return
     * the actual types of the interface type variables.
     * May return null or an array of nulls if type resolution fails.
	 */
    public Class<?>[] getGenericParameterTypes(
            Class parameterizedType, Class implementation )
        throws StandardException
	{
        // construct the inheritance chain stretching from the parameterized
        // type down to the concrete implemention
        ArrayList<Class<?>> chain =
                getTypeChain(parameterizedType, implementation);

        // walk the chain, filling in a map of generic types to their
        // resolved types
        HashMap<Type, Type> resolvedTypes = getResolvedTypes(chain);

        // compose the resolved types together in order to compute the actual
        // classes which are plugged into the variables of the parameterized
        // type
        ArrayList<Class<?>> parameterTypes =
                getParameterTypes(parameterizedType, resolvedTypes);

        // turn the list into an array
        if (parameterTypes == null) {
            return null;
        }

        return parameterTypes.toArray(new Class<?>[parameterTypes.size()]);
    }

	/**
	 * Get the parameter types for a method described by a Member as a String[].
	 *
	 * @param method	A Member describing a method
	 *
	 * @return	A String[] describing the parameters of the method
	 */
	public String[] getParameterTypes(Member method)
	{

		Class[] parameterClasses;
		if (method instanceof Method) {
			parameterClasses = ((Method) method).getParameterTypes();
		} else {
			parameterClasses = ((Constructor) method).getParameterTypes();
		}

		String[] parameterTypes = new String[parameterClasses.length];

		for (int i = 0; i < parameterTypes.length; i++) {
			parameterTypes[i] = ClassInspector.readableClassName(parameterClasses[i]);
		}

		return parameterTypes;
	}

	/**
	 * Determine whether a type is a Java primitive, like int or boolean
	 *
	 * @param typeName	The name of the Java type
	 *
	 * @return	true if it's a primitive type
	 */
	public static boolean primitiveType(String typeName)
	{
		for (int i = 0; i < primTypeNames.length; i++)
		{
			if (typeName.equals(primTypeNames[i]))
				return true;
		}

		return false;
	}


	/**
	 *  Tricky function to resolve a method.  If primParamClasses is null
	 *  we know it's first pass.  First pass try to match as all "object"
	 *  types, second pass try to match any combination of "object" and
	 *  "primitive" types.  Find the closest match among all the qualified
	 *  candidates.  If there's a tie, it's ambiguous.
     *
     *  The preceding paragraph is a bit misleading. As of release 10.4, the
     *  second pass did not consider arbitrary combinations of primitive and
     *  wrapper types. This is because the first pass removed from consideration
     *  candidates which would be allowed under ANSI rules. As a fix for bug
     *  DERBY-3652, we now allow primitive and wrapper type matches during
     *  the first pass. The ANSI rules are documented in DERBY-3652.
     *
	 *  @param receiverClass 	the class who holds the methods
	 *  @param methodName		the name of method
	 *	@param paramClasses		object type classes of input parameters
	 *  @param primParamClasses	primitive type classes or null
	 *  @param isParam			isParam (for ?) array
	 *  @param staticMethod		static method or not
	 *  @param methods			method stack
	 *  @return	the matched method
	 *
	 **/
	private Member resolveMethod
        (
         Class receiverClass,
         String methodName,
         Class[] paramClasses,
         Class[] primParamClasses,
         boolean[] isParam,
         boolean staticMethod,
         boolean repeatLastParameter,
         Member[] methods,
         boolean hasVarargs
         )
        throws StandardException
	{

		if (SanityManager.DEBUG) {
		  if (SanityManager.DEBUG_ON("MethodResolutionInfo"))
		  {
			SanityManager.DEBUG("MethodResolutionInfo",
				"MRI - Begin method resolution trace for " + methodName + 
				"() with " + paramClasses.length + (repeatLastParameter ? "+" : "") + " parameters");

			for (int parmCtr = 0; parmCtr < paramClasses.length; parmCtr++)
			{
				SanityManager.DEBUG("MethodResolutionInfo",
					"MRI - Parameter #" + parmCtr +
					" is of type " + (paramClasses[parmCtr] == null ? "null" : paramClasses[parmCtr].getName()));
			}
		  }
		}
			
		/* Step through all the methods available in this class */
		int candidateIndex = -1;

		boolean firstTimeAround = true;
		boolean	ambiguous;
		boolean somethingChanged;
		do {

			ambiguous = false;
			somethingChanged = false;

nextMethod:	for (int i = 0; i < methods.length; i++) {

				Member currentMethod = methods[i];

				// on second and later times around there will be null entries
				// also, don't compare ourself to ourself
				if ((currentMethod == null) ||
					(i == candidateIndex))
				{
					continue;
				}

				// must have the same number of parameters
				Class[] currentMethodParameters = currentMethod instanceof Method ?
					((Method) currentMethod).getParameterTypes():
					((Constructor) currentMethod).getParameterTypes();

				// only check the basic stuff once
				if (firstTimeAround) {

					if (repeatLastParameter) {
						// match any number of parameters greater or equal to
						// the passed in number, but repeating the last type.
						if (currentMethodParameters.length < paramClasses.length) {
							methods[i] = null; // remove non-applicable methods
							continue;
						}


					} else {

						// regular match on parameter count
						if (currentMethodParameters.length != paramClasses.length) {
							methods[i] = null; // remove non-applicable methods
							continue;
						}
					}

					/* Look only at methods that match the modifiers */
					if (staticMethod && !Modifier.isStatic(currentMethod.getModifiers())) {
						methods[i] = null; // remove non-applicable methods
						continue;
					}

                    // If the routine was declared to be varargs, then we eliminate
                    // all non-varargs methods from consideration
					if ( hasVarargs && !isVarArgsMethod( currentMethod )) {
						methods[i] = null; // remove non-applicable methods
						continue;
					}

					/* Look only at methods with the right name */
					if (!methodName.startsWith("<")) {
						if ( ! methodName.equals(currentMethod.getName())) {
							methods[i] = null; // remove non-applicable methods
							continue;
						}
					}	


					if (repeatLastParameter) {
						// With N parameters requested check all parameters from N-1 to end are equal
						// to the requested parameter.
						for (int pr = paramClasses.length - 1; pr < currentMethodParameters.length; pr++) {
							if (!currentMethodParameters[pr].equals(paramClasses[paramClasses.length - 1])) {
								methods[i] = null; // remove non-applicable methods
								continue nextMethod;
							}
						}
					}
				}

				if (SanityManager.DEBUG) {
				  if (SanityManager.DEBUG_ON("MethodResolutionInfo")) {
					SanityManager.DEBUG("MethodResolutionInfo", 
						"MRI - Considering :" + currentMethod.toString());
				  }
				}

                //
                // According to the ANSI rules, primitives and their
                // corresponding wrapper types are equally good for resolving
                // numeric arguments of user-coded functions and procedures. See
                // DERBY-3652 for a description of the ANSI rules.
                //
				// can the required signature be converted to those of this method?
				if (!signatureConvertableFromTo(paramClasses, primParamClasses,
							currentMethodParameters, isParam, true)) {

					if (SanityManager.DEBUG) {
					  if (SanityManager.DEBUG_ON("MethodResolutionInfo")) {
						SanityManager.DEBUG("MethodResolutionInfo", 
							"MRI - Skipping :" + currentMethod.toString());
					  }
					}

					methods[i] = null; // remove non-applicable methods
					continue;
				}

			if (SanityManager.DEBUG) {
				  if (SanityManager.DEBUG_ON("MethodResolutionInfo")) {
					SanityManager.DEBUG("MethodResolutionInfo",	"MRI - Match found ");
				  }
				}

				/* Is this the first match? */
				if (candidateIndex == -1)
				{
					candidateIndex = i;
					if (SanityManager.DEBUG) {
					  if (SanityManager.DEBUG_ON("MethodResolutionInfo")) {
						SanityManager.DEBUG("MethodResolutionInfo",
						"MRI - Current method is now candidate");
					  }
					}
					continue;
				}

                //
                // Before the fixes to DERBY-3652, we used to weed out
                // ambiguities by applying the rules from section 15.11
                // of the Java Language Specification. These are not the
                // ANSI resolution rules however. The code to weed out
                // ambiguities has been removed.
                //

				/* We have seen an ambiguous situation; one of the cases may
				 * tie on each parameter.
				 */
				ambiguous = true;

				if (SanityManager.DEBUG) {
				  if (SanityManager.DEBUG_ON("MethodResolutionInfo")) {
					SanityManager.DEBUG("MethodResolutionInfo", "MRI - Ambiguous match");
				  }
				}
			}
			firstTimeAround = false;
		} while (ambiguous && somethingChanged);  

		if (SanityManager.DEBUG) {
		  if (SanityManager.DEBUG_ON("MethodResolutionInfo")) {
			SanityManager.DEBUG("MethodResolutionInfo",
				"MRI - End method resolution trace for " + methodName + "()" +
				"\nMRI - ");
		  }
		}

		/* Throw an exception here if the method invocation ambiguous */
		if (ambiguous)
		{
			/* Put the parameter type names into a single string */
			StringBuffer parmTypesString = new StringBuffer();
			for (int i = 0; i < paramClasses.length; i++)
			{
				if (i != 0)
					parmTypesString.append(", ");
				parmTypesString.append(paramClasses[i] == null ? "null" : paramClasses[i].getName());
				if (primParamClasses != null && primParamClasses[i] != null)
					parmTypesString.append("(").append(primParamClasses[i].getName()).append(")");
			}

			throw StandardException.newException(SQLState.LANG_AMBIGUOUS_METHOD_INVOCATION, 
												receiverClass.getName(), 
												methodName,
												parmTypesString.toString());
		}

		if (candidateIndex == -1)
			return null;

		if (SanityManager.DEBUG) {
			if (methods[candidateIndex] == null)
				SanityManager.THROWASSERT("methods is null at index " + candidateIndex);
		}
		return methods[candidateIndex];
	}

	/**
		Get (load) the class for the given class name.
		This method converts any java language class name
		into a Class object. This includes cases like String[]
		and primitive types.
		This will attempt to load the class from the application set.

		@exception ClassNotFoundException Class cannot be found, or
		a SecurityException or LinkageException was thrown loading the class.
	*/
	public Class<?> getClass(String className) throws ClassNotFoundException {

		if ((className == null) || 
			(className.length() == 0))
		{
			return null;
		}

		int arrayDepth = 0;
		int classNameLength = className.length();

		int position = classNameLength - 2;

		while ((position >= 0) && className.substring(position, position + 2).equals("[]")) {
			arrayDepth++;
			position -= 2;
			classNameLength -= 2;
		}

		if (classNameLength <= 0) {
			// a bogus class name, let Class.forName deal with the error.
			return Class.forName(className);
		}

		if (arrayDepth != 0)
			className = className.substring(0, classNameLength);

		Class baseClass = null;

		if (classNameLength >=3 && classNameLength <=7) {
			if ("int".equals(className)) 
				baseClass = Integer.TYPE;
			else if ("short".equals(className)) 
				baseClass = Short.TYPE;
			else if ("boolean".equals(className)) 
				baseClass = Boolean.TYPE;
			else if ("byte".equals(className)) 
				baseClass = Byte.TYPE;
			else if ("float".equals(className)) 
				baseClass = Float.TYPE;
			else if ("double".equals(className)) 
				baseClass = Double.TYPE;
			else if ("long".equals(className)) 
				baseClass = Long.TYPE;
			else if ("char".equals(className)) 
				baseClass = Character.TYPE;
			else if ("void".equals(className)) 
				baseClass = Void.TYPE;
		}
		
		if (baseClass == null) {
			baseClass = cf.loadApplicationClass(className);
		}

		if (arrayDepth == 0)
			return baseClass;

		// need to create an actual instance of the array type
		// and get its class from that. There is no other documented
		// way to do this. While a getName() on an array class
		// returns [[[Lclassname; format it's not consistent
		// with primitive types, e.g.
		//
		// Integer.TYPE.getName()   returns "int"
		// Class.forName(new int[0] returns "[I"
		// 

		if (arrayDepth == 1)
			return Array.newInstance(baseClass, 0).getClass();

		return Array.newInstance(baseClass, new int[arrayDepth]).getClass();
	}

	/**
	 *  Can we convert a signature from fromTypes(primFromTypes) to toTypes.
	 *  "mixTypes" is a flag to show if object/primitive type conversion is
	 *  possible; this is used for comparing two candidate methods in the
	 *  second pass of the two pass method resolution.
	 *
	 *  @param fromTypes	from types' classes
	 *	@param primFromTypes primitive from types or null
	 *	@param toTypes		to types' classes
	 *	@param isParam		is parameter (?) or not
	 *	@param mixTypes		mixing object/primitive types for comparison
	 **/
	private boolean signatureConvertableFromTo(Class[] fromTypes, Class[] primFromTypes,
												 Class[] toTypes, boolean[] isParam,
												 boolean mixTypes) {

		// In the case repeatLastParameter was true, then the two methods may have
		// different numbers of parameters. We need to compare only the non-repeated
		// parameters, which is the number of input parameters.

		int checkCount = fromTypes.length;
		if (toTypes.length < checkCount)
			checkCount = toTypes.length;

		for (int i = 0; i < checkCount; i++) {

			Class fromClass = fromTypes[i];
			Class toClass = toTypes[i];

			// this means an untyped null was passed in. Can only ever be in the
			// from side as the null can only be in the signature passed in by
			// the caller of findPublicMethod. Any signatures of existing methods
			// are always typed.
			if (fromClass == null) {

				// primitive types are only considered on
				// the 2nd pass
				if (toClass.isPrimitive())
				{
					if ((primFromTypes == null)		// first pass
						|| (isParam != null && ! isParam[i]))
					{
						return false;
					}
				}
				continue;
			}


			if ((!classConvertableFromTo(fromClass, toClass, mixTypes)) &&
				// primitive type, if any, also doesn't work
				((primFromTypes == null) || (primFromTypes[i] == null) ||
				 (!classConvertableFromTo(primFromTypes[i], toClass, mixTypes))
				))
				return false;
		}

		return true;
	}

	/**
	 *  Can we convert a fromClass to toClass.
	 *  "mixTypes" is a flag to show if object/primitive type conversion is
	 *  possible; this is used for comparing two candidate methods in the
	 *  second pass of the two pass method resolution.
	 *
	 *  @param fromClass	from class
	 *	@param toClass		to class
	 *	@param mixTypes		mixing object/primitive types for comparison
	 **/
	protected boolean classConvertableFromTo(Class fromClass, Class toClass, boolean mixTypes) {

        //
        // ANSI rules do not allow widening
        //
        if ( fromClass.getName().equals( toClass.getName() ) ) { return true; }

        //
        // OUT and INOUT args are arrays. Compare the cell types rather than the array types.
        //
        if ( fromClass.isArray() && toClass.isArray() )
        {
            return classConvertableFromTo( fromClass.getComponentType(), toClass.getComponentType(), mixTypes );
        }

		// When comparing two candidate methods to see which one is closer,
		// we want to mix object type and primitive type, because they could
		// both be chosen in the second pass.  But when deciding if a method
		// is qualified (to be a candidate), we do not want to mix types at
		// any time, the reason is that we can NOT do more than one step
		// conversion: for example, input parameter is BigDecimal, we convert
		// it to double for method resolution, we can NOT convert it again to
		// Double to match a method. "(paramTypes, primParamTypes)" already
		// includes all the one-step conversions.  But at any time we do want
		// to see if two primitives are convertable.
		if ((!(toClass.isPrimitive() && fromClass.isPrimitive())) && (!mixTypes))
			return false;

		// There are nine predefined Class objects to represent the eight 
		// primitive Java types and void.  We also handle prim vs. non-prim
		// conversion of the same type.  boolean and double are only convertable
		// to themseleves.  void should never be seen here.  In the second pass
		// we treat primitive type and the corrsponding non-primitive type
		// uniformly

		String fromName = fromClass.getName(), toName = toClass.getName();
		if ((fromClass == Boolean.TYPE) || fromName.equals(nonPrimTypeNames[0]))
		{
			if ((toClass == Boolean.TYPE) || toName.equals(nonPrimTypeNames[0]))
				return true;
		} else if ((fromClass == Byte.TYPE) || fromName.equals(nonPrimTypeNames[1]))
		{
			if ((toClass == Byte.TYPE) || toName.equals(nonPrimTypeNames[1]) ||
				// we never need to see if toClass is of wider "object" type,
				// because a wider "object" type and a narrower "primitive"
				// type can never both be candidate, eg, "int" and "Long" can
				// never both accomodate the same parameter; while "long" and
				// "Integer" can.
				(toClass == Short.TYPE) ||
				(toClass == Integer.TYPE) ||
				(toClass == Long.TYPE) ||
				(toClass == Float.TYPE) ||
				(toClass == Double.TYPE) )
				return true;
		} else if ((fromClass == Character.TYPE) || fromName.equals(nonPrimTypeNames[2]))
		{
			if ((toClass == Character.TYPE) || toName.equals(nonPrimTypeNames[2]) ||
				(toClass == Integer.TYPE) ||
				(toClass == Long.TYPE) ||
				(toClass == Float.TYPE) ||
				(toClass == Double.TYPE) )
				return true;
		} else if ((fromClass == Short.TYPE) || fromName.equals(nonPrimTypeNames[3]))
			{
			if ((toClass == Short.TYPE) || toName.equals(nonPrimTypeNames[4]) )
				return true;
		} else if ((fromClass == Integer.TYPE) || fromName.equals(nonPrimTypeNames[4]))
		{
			if ((toClass == Integer.TYPE) || toName.equals(nonPrimTypeNames[4]) )
				return true;
		} else if ((fromClass == Long.TYPE) || fromName.equals(nonPrimTypeNames[5]))
		{
			if ((toClass == Long.TYPE) || toName.equals(nonPrimTypeNames[5]) )
				return true;
		} else if ((fromClass == Float.TYPE) || fromName.equals(nonPrimTypeNames[6]))
		{
			if ((toClass == Float.TYPE) || toName.equals(nonPrimTypeNames[6]) )
				return true;
		} else if ((fromClass == Double.TYPE) || fromName.equals(nonPrimTypeNames[7]))
		{
			if ((toClass == Double.TYPE) || toName.equals(nonPrimTypeNames[7]))
				return true;
		}

		return false;
	}

	/**
	 * Translate a JVM-style type descriptor to a Java-language-style type
	 * name.
	 *
	 * @param clazz		The String that contains the JVM type name
	 *
	 * @return	The Java-language-style type name
	 */
	public static String readableClassName(Class clazz)
	{
		if (!clazz.isArray())
			return clazz.getName();

		int arrayDepth = 0;
		do {
			arrayDepth++;
			clazz = clazz.getComponentType();
		} while (clazz.isArray());

		StringBuffer sb = new StringBuffer(clazz.getName());

		for (int i = 0; i < arrayDepth; i++) {
			sb.append("[]");
		}

		return sb.toString();
	}

	/**
	 * Get the declaring class for a method.
	 *
	 * @param method	A Member describing a method
	 *
	 * @return	A String with the declaring class
	 *
	 * @see Member#getDeclaringClass
	 */
	public String getDeclaringClass(Member method)
	{
		return method.getDeclaringClass().getName();
	}		

    /////////////////////////////////////////////////////////////////////////
    //
    // MINIONS FOR getTypeBounds()
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * Get the type bounds for all of the type variables of the given
     * parameterized type.
     */
    private Class[][] findTypeBounds(ParameterizedType pt) {
        Type[] actualTypeArguments = pt.getActualTypeArguments();
        int argCount = actualTypeArguments.length;
        Class[][] retval = new Class[argCount][];

        for (int i = 0; i < argCount; i++) {
            retval[ i] = boundType(actualTypeArguments[ i]);
        }

        return retval;
    }

    /**
     * Get the bounds for a single type variable.
     */
    private Class[] boundType(Type type) {
        if (type instanceof Class) {
            return new Class[]{(Class) type};
        } else if (type instanceof TypeVariable) {
            Type[] bounds = ((TypeVariable) type).getBounds();
            int count = bounds.length;
            Class[] retval = new Class[count];

            for (int i = 0; i < count; i++) {
                retval[ i] = getRawType(bounds[ i]);
            }

            return retval;
        } else {
            return null;
        }
    }

    /**
     * Get the raw type of a type bound.
     */
    private Class getRawType(Type bound) {
        if (bound instanceof Class) {
            return (Class) bound;
        } else if (bound instanceof ParameterizedType) {
            return getRawType(((ParameterizedType) bound).getRawType());
        } else {
            return null;
        }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    // MINIONS FOR getGenericParameterTypes()
    //
    /////////////////////////////////////////////////////////////////////////

    /**
     * Construct an inheritance chain of types stretching from a supertype down
     * to a concrete implementation.
     */
    private ArrayList<Class<?>> getTypeChain(Class<?> chainEnd, Class<?> start) {
        ArrayList<Class<?>> result = null;

        if (start == null) {
            return null;
        }

        if (!chainEnd.isAssignableFrom(start)) {
            return null;
        }

        if (start == chainEnd) {
            result = new ArrayList<Class<?>>();
        }

        if (result == null) {
            result = getTypeChain(chainEnd, start.getSuperclass());

            if (result == null) {
                for (Class<?> iface : start.getInterfaces()) {
                    result = getTypeChain(chainEnd, iface);
                    if (result != null) {
                        break;
                    }
                }
            }
        }

        if (result != null) {
            result.add(start);
        }

        return result;
    }

    /**
     * Given an inheritance chain of types, stretching from a superclass down
     * to a terminal concrete class, construct a map of generic types to their
     * resolved types.
     */
    private HashMap<Type, Type> getResolvedTypes(ArrayList<Class<?>> chain) {
        if (chain == null) {
            return null;
        }

        HashMap<Type, Type> resolvedTypes = new HashMap<Type, Type>();

        for (Class<?> klass : chain) {
            addResolvedTypes(resolvedTypes, klass.getGenericSuperclass());

            for (Type iface : klass.getGenericInterfaces()) {
                addResolvedTypes(resolvedTypes, iface);
            }
        }

        return resolvedTypes;
    }

    /**
     * Given a generic type, add its parameter types to an evolving
     * map of resolved types. Some of the resolved types may be
     * generic type variables which will need further resolution from
     * other generic types.
     */
    private void addResolvedTypes(HashMap<Type, Type> resolvedTypes,
                                  Type genericType) {
        if (genericType == null) {
            return;
        }

        if (genericType instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType) genericType;
            Class rawType = (Class) pt.getRawType();

            Type[] actualTypeArguments = pt.getActualTypeArguments();
            TypeVariable[] typeParameters = rawType.getTypeParameters();
            for (int i = 0; i < actualTypeArguments.length; i++) {
                resolvedTypes.put(typeParameters[i], actualTypeArguments[i]);
            }
        }
    }

    /**
     * Given a map of resolved types, compose them together in order
     * to resolve the actual concrete types that are plugged into the
     * parameterized type.
     */
    private ArrayList<Class<?>> getParameterTypes(
            Class<?> parameterizedType, HashMap<Type, Type> resolvedTypes) {
        if (resolvedTypes == null) {
            return null;
        }

        Type[] actualTypeArguments = parameterizedType.getTypeParameters();

        ArrayList<Class<?>> result = new ArrayList<Class<?>>();

        // resolve types by composing type variables.
        for (Type baseType : actualTypeArguments) {
            while (resolvedTypes.containsKey(baseType)) {
                baseType = resolvedTypes.get(baseType);
            }

            result.add(getRawType(baseType));
        }

        return result;
    }
}

