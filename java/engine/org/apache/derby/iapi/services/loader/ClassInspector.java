/*

   Derby - Class org.apache.derby.iapi.services.loader.ClassInspector

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.loader;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import java.lang.reflect.*;

/**
	Methods to find out relationships between classes and methods within a class.
	All class names within this interface are treated as java language class names,
	e.g. int, COM.foo.Myclass, int[], java.lang.Object[]. That is java internal
	class names as defined in the class file format are not understood.
*/
public final class ClassInspector
{
	private static final String[] primTypeNames =
		{"boolean", "byte", "char", "short", "int", "long", "float", "double"};

	// collect these as static, instead of each time allocates these new
	// Strings for every method resolution

	private static final String[] nonPrimTypeNames =
		{"java.lang.Boolean", "java.lang.Byte", "java.lang.Character",
		 "java.lang.Short", "java.lang.Integer", "java.lang.Long",
		 "java.lang.Float", "java.lang.Double"};

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
		Class clazz = getClass(className);
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
			Class toClass = getClass(toClassName);
			// is toClass an untyped null
			if (toClass == null) {
				return false;
			}

			Class fromClass = getClass(fromClassName);

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
		Class theClass = getClass(className);
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
	 * @param method		A Member representing the method for
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
	 * A untyped null paramter is indicated by passing in an empty string ("")
	 * as its class name.
	 <BR>
	 If receiverType respresents an interface then the methods of java.lang.Object
	 arer included in the candidate list.
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
	 * @param receiverTypes	The class name of the receiver
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
	   If false the laste parameter is matched as usual. This also requires an exact match on the last parameter type.
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
	public Member findPublicMethod(String receiverType,
								String methodName,
								String[] parmTypes,
								String[] primParmTypes,
								boolean[] isParam,
								boolean staticMethod,
								boolean repeatLastParameter)
					throws ClassNotFoundException, StandardException
	{
		Class receiverClass = getClass(receiverType);
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

		return resolveMethod(receiverClass, methodName, paramClasses, 
						primParamClasses, isParam, staticMethod, repeatLastParameter, methodList);
	}

	/**
	 * Find a public field  for a class.
	   This follows the sematics of the java compiler for locating a field.
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

			Class receiverClass = getClass(receiverType);
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
	 * A untyped null paramter is indicated by passing in an empty string ("")
	 * as its class name. 
	 *
	 * @param receiverTypes	The class name of the receiver
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
		Class receiverClass = getClass(receiverType);
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
		return resolveMethod(receiverClass, "<init>", paramClasses, 
							 primParamClasses, isParam, false, false,
							 receiverClass.getConstructors());
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
	 *  @param receiverClass 	the class who holds the methods
	 *  @param methodName		the name of method
	 *	@param paramClasses		object type classes of input parameters
	 *  @param primParamClasses	primitive type classes or null
	 *  @param isParam			isParam (for ?) array
	 *  @param staticMethod		static method or not
	 *  @param Member[] methods	method stack
	 *  @return	the matched method
	 *
	 **/
	private Member resolveMethod(
				Class receiverClass,
				String methodName,
				Class[] paramClasses,
				Class[] primParamClasses,
				boolean[] isParam,
				boolean staticMethod,
				boolean repeatLastParameter,
				Member[] methods)
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


				// can the required signature be converted to those of this method
				if (!signatureConvertableFromTo(paramClasses, primParamClasses,
							currentMethodParameters, isParam, false)) {

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

				/* Not the first match, so find out which one, if either one,
				 * has the best match on the parameters.  (No narrowing
				 * conversions.)  15.11 of Java Language Specification.
				 */

				Member candidateMethod = methods[candidateIndex];

				// If the candidate method is more specific than the current
				// method then the candidate method is still the maximally specific method
				// Note at this point we could still have a ambiguous situation.

				boolean candidateMoreOrEqual = isMethodMoreSpecificOrEqual(
							candidateMethod, currentMethod, isParam);
				boolean currentMoreOrEqual = isMethodMoreSpecificOrEqual(
							currentMethod, candidateMethod, isParam);
				if (candidateMoreOrEqual && ! currentMoreOrEqual) {
					if (SanityManager.DEBUG) {
					  if (SanityManager.DEBUG_ON("MethodResolutionInfo")) {
						SanityManager.DEBUG("MethodResolutionInfo",
						"MRI - Candidate is still maximally specific");
					  }
					}
					methods[i] = null; // remove non-applicable methods
					continue;
				}

				// if the current method is more specific than the candidiate
				// method then it becomes the new maximally specific method
				// Note at this point we could still have a ambiguous situation.

				if (currentMoreOrEqual && ! candidateMoreOrEqual) {
					if (SanityManager.DEBUG) {
					  if (SanityManager.DEBUG_ON("MethodResolutionInfo")) {
						SanityManager.DEBUG("MethodResolutionInfo",
						"MRI - Current method is now candidate, replaced previous candidate");
					  }
					}
					methods[candidateIndex] = null; // remove non-applicable methods
					candidateIndex = i;
					somethingChanged = true;
					continue;
				}

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
			String	parmTypesString = "";
			for (int i = 0; i < paramClasses.length; i++)
			{
				if (i != 0)
					parmTypesString += ", ";
				parmTypesString += (paramClasses[i] == null ? "null" : paramClasses[i].getName());
				if (primParamClasses != null && primParamClasses[i] != null)
					parmTypesString += "(" + primParamClasses[i].getName() + ")";
			}

			throw StandardException.newException(SQLState.LANG_AMBIGUOUS_METHOD_INVOCATION, 
												receiverClass.getName(), 
												methodName,
												parmTypesString);
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

		@exception ClassNotFoundException Class cannot be found.
	*/
	public Class getClass(String className) throws ClassNotFoundException {

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
		Is method/constructor T more or equally specific than method U.

		See the Java Language Specification section 15.11.2.2.
	*/
	private boolean isMethodMoreSpecificOrEqual(Member T, Member U, boolean[] isParam) {

		Class[] TC;
		Class[] UC;

		if (T instanceof Method) {
			if (!classConvertableFromTo(T.getDeclaringClass(), U.getDeclaringClass(), true))
				return false;

			TC = ((Method) T).getParameterTypes();
			UC = ((Method) U).getParameterTypes();
		} else {
			TC = ((Constructor) T).getParameterTypes();
			UC = ((Constructor) U).getParameterTypes();
		}

		return signatureConvertableFromTo(TC, null, UC, isParam, true);
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

		if (toClass.isAssignableFrom(fromClass)) {
			return true;
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
			if ((toClass == Short.TYPE) || toName.equals(nonPrimTypeNames[3]) ||
				(toClass == Integer.TYPE) ||
				(toClass == Long.TYPE) ||
				(toClass == Float.TYPE) ||
				(toClass == Double.TYPE) )
				return true;
		} else if ((fromClass == Integer.TYPE) || fromName.equals(nonPrimTypeNames[4]))
		{
			if ((toClass == Integer.TYPE) || toName.equals(nonPrimTypeNames[4]) ||
				(toClass == Long.TYPE) ||
				(toClass == Float.TYPE) ||
				(toClass == Double.TYPE) )
				return true;
		} else if ((fromClass == Long.TYPE) || fromName.equals(nonPrimTypeNames[5]))
		{
			if ((toClass == Long.TYPE) || toName.equals(nonPrimTypeNames[5]) ||
				(toClass == Float.TYPE) ||
				(toClass == Double.TYPE) )
				return true;
		} else if ((fromClass == Float.TYPE) || fromName.equals(nonPrimTypeNames[6]))
		{
			if ((toClass == Float.TYPE) || toName.equals(nonPrimTypeNames[6]) ||
				(toClass == Double.TYPE) )
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
	 * @param vmTypeName		The String that contains the JVM type name
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

}

