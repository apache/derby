/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.compiler
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.compiler;

import org.apache.derby.iapi.services.loader.GeneratedClass;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.util.ByteArray;

/**
 * ClassBuilder is used to construct a java class's byte array
 * representation.
 *
 * Limitations:
 *   No checking for language use violations such as invalid modifiers
 *	or duplicate field names.
 *   All classes must have a superclass; java.lang.Object must be
 *      supplied if there is no superclass.
 *
 * <p>
 * When a class is first created, it has:
 * <ul>
 * <li> a superclass
 * <li> modifiers
 * <li> a name
 * <li> a package
 * <li> no superinterfaces, methods, fields, or constructors
 * <li> an empty static initializer
 * </ul>
 * <p>
 * MethodBuilder implementations are required to get code out of the
 * constructs within their bodies in some manner. 
 * Most typically, they may have a stream to which the statement and 
 * expression constructs write the code that they represent,
 * and they walk over the statements and expressions in the appropriate order.
 *
 * @author ames
 */
public interface ClassBuilder { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
	 * add a field to this class. Fields cannot
	 * be initialized here, they must be initialized
	 * in the static initializer code (static fields)
	 * or in the constructors.
	 * <p>
	 * Methods are added when they are created with the JavaFactory.
	 * @param type	The type of the field in java language.
	 * @param name	The name of the field.
	 * @param modifiers	The | of the modifier values such as
	 *					public, static, etc.
	 * @see ClassBuilder#newMethodBuilder
	 * @see #newConstructorBuilder
	 */
	LocalField addField(String type, String name, int modifiers);

	/**
		Fully create the bytecode and load the
		class using the ClassBuilder's ClassFactory.

		@exception StandardException Standard Cloudscape policy
	*/
	GeneratedClass getGeneratedClass() throws StandardException;

	/**
	 * At the time the class is completed and bytecode
	 * generated, if there are no constructors then
	 * the default no-arg constructor will be defined.
	 */
	ByteArray getClassBytecode();

	/**
	 * the class's unqualified name
	 */
	String getName();

	/**
	 * the class's qualified name
	 */
	String getFullName();

	/**
	 * a method. Once it is created, parameters, thrown
	 * exceptions, statements, and local variable declarations
	 * must be added to it. It is put into its defining class
	 * when it is created.
	 * <verbatim>
	   Java: #modifiers #returnType #methodName() {}
	  		// modifiers is the | of the JVM constants for
	  		// the modifiers such as static, public, etc.
       </verbatim>
	   <p>
	 * This is used to start a constructor as well; pass in
	 * null for the returnType when used in that manner.
	 *
	 * @param modifiers the | of the Modifier
	 *	constants representing the visibility and control of this
	 *	method.
	 * @param returnType the return type of the method as its
	 *	Java language type name.
	 * @param methodName the name of the method.
	 *
	 * @return the method builder.
	 * @see java.lang.reflect.Modifier
	 */
	MethodBuilder newMethodBuilder(int modifiers, String returnType,
		String methodName);
	
	/**
	 * a method with parameters. Once it is created, thrown
	 * exceptions, statements, and local variable declarations
	 * must be added to it. It is put into its defining class
	 * when it is created.
	 * <verbatim>
	   Java: #modifiers #returnType #methodName() {}
	  		// modifiers is the | of the JVM constants for
	  		// the modifiers such as static, public, etc.
       </verbatim>
	   <p>
	 * This is used to start a constructor as well; pass in
	 * null for the returnType when used in that manner.
	 *
	 * @param modifiers the | of the Modifier
	 *	constants representing the visibility and control of this
	 *	method.
	 * @param returnType the return type of the method as its
	 *	Java language type name.
	 * @param methodName the name of the method.
	 * @param parms	an array of String representing the
	 *				method's parameter types
	 *
	 * @return the method builder.
	 * @see java.lang.reflect.Modifier
	 */
	MethodBuilder newMethodBuilder(int modifiers, String returnType,
		String methodName, String[] parms);

	/**
	 * a constructor. Once it is created, parameters, thrown
	 * exceptions, statements, and local variable declarations
	 * must be added to it. It is put into its defining class
	 * when it is created.
	 * <verbatim>
	   Java: #modifiers #className() {}
	  		// modifiers is the | of the JVM constants for
	  		// the modifiers such as static, public, etc.
	  		// className is taken from definingClass.name()
       </verbatim>
	 * <p>
	 * This is used to start a constructor as well; pass in
	 * null for the returnType when used in that manner.
	 *
	 * @param modifiers the | of the Modifier
	 *	constants representing the visibility and control of this
	 *	method.
	 *
	 * @return the method builder for the constructor.
	 * @see java.lang.reflect.Modifier
	 */
	MethodBuilder newConstructorBuilder(int modifiers);

	/**
		Create a new private field and its getter and setter methods.

		@param name basename for the methods, methods will have 'set' or 'get' prepended.
		@param methodModifier modifier for method
		@param boolean staticField true if the field is static
		@param type type of the field, return type of the get method and
		parameter type of the set method.

	*/
	void newFieldWithAccessors(String getter, String setter, int methodModifer,
		boolean staticField, String type);
}
