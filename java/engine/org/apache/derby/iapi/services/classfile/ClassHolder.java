/*

   Derby - Class org.apache.derby.iapi.services.classfile.ClassHolder

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.classfile;

import org.apache.derby.iapi.services.sanity.SanityManager;


import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;

import java.io.IOException;
import java.util.Vector;

import org.apache.derby.iapi.util.ByteArray;
import org.apache.derby.iapi.services.classfile.VMDescriptor;
import org.apache.derby.iapi.services.classfile.VMDescriptor;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Enumeration;


/** Based upon "THE class FILE FORMAT" chapter of "The Java Virtual Machine Specification"
    corresponding to version 1.0.2 of the Java Virtual Machine and 1.0.2 of the
	Java Language Specification.

    ISBN  0-201-63452-X, September 1996.
	*/

public class ClassHolder {


	/*
	** Constants.
	*/

	/*
	** Fields
	*/

	protected int access_flags;
	protected int this_class;
	protected int super_class;

	// protected InterfacesArray interfaces; // can be null
	protected int[] interfaces; //can be null

	protected MemberTable field_info; // can be null
	protected MemberTable method_info;	// can be null
	protected Attributes attribute_info; // can be null

	/*
	** Fields for Constant Pool Table
	*/
	protected Hashtable cptHashTable;
	protected Vector cptEntries;
	private int cptEstimatedSize;

	/**
		Used to search for index entries to avoid object allocation
		in the case a referecne already exists.
	*/
	private final CONSTANT_Index_info	searchIndex = new CONSTANT_Index_info(0, 0, 0);

	/*
	**	Constructors.
	*/

	protected ClassHolder(int estimatedConstantPoolCount) {
		// Constant Pool Information
		// 100 is the estimate of the number of entries that will be generated
		cptEntries = new Vector(estimatedConstantPoolCount);
		cptHashTable = new Hashtable(estimatedConstantPoolCount, (float)0.75);

		// reserve the 0'th constant pool entry
		cptEntries.setSize(1);
	}


	/**
		This will not define a constructor -- it is up
		to the caller to add at least one.
	*/

	public ClassHolder(String fullyQualifiedName, String superClassName,
		int modifiers) {

		this(100);

		access_flags = modifiers | /* Modifier.SUPER */ 0x0020;

		this_class = addClassReference(fullyQualifiedName);
		super_class = addClassReference(superClassName);
		method_info = new MemberTable(0);
	}

	public void put(ClassFormatOutput out) throws IOException {

		/* Write out the header */
		out.putU4(VMDescriptor.JAVA_CLASS_FORMAT_MAGIC);
		out.putU2(VMDescriptor.JAVA_CLASS_FORMAT_MINOR_VERSION);
		out.putU2(VMDescriptor.JAVA_CLASS_FORMAT_MAJOR_VERSION);

		out.putU2(cptEntries.size());
		cptPut(out);

		out.putU2(access_flags);
		out.putU2(this_class);
		out.putU2(super_class);

		if (interfaces != null) {
			int ilen = interfaces.length;
			out.putU2(ilen);
			for (int i = 0; i < ilen; i++) {
				out.putU2(interfaces[i]);
			}
		} else {
			out.putU2(0);
		}

		if (field_info != null) {
			out.putU2(field_info.size());
			field_info.put(out);
		} else {
			out.putU2(0);
		}

		if (method_info != null) {
			out.putU2(method_info.size());
			method_info.put(out);
		} else {
			out.putU2(0);
		}

		if (attribute_info != null) {
			out.putU2(attribute_info.size());
			attribute_info.put(out);
		} else {
			out.putU2(0);
		}

	}


	/*
	**	Public methods from ClassHolder.
	*/


	public ByteArray getFileFormat() {

		int classFileSize = 4 + (10 * 2);
		classFileSize += cptEstimatedSize;

		if (interfaces != null)
			classFileSize += (interfaces.length * 2);

		if (field_info != null)
			classFileSize += field_info.classFileSize();

		if (method_info != null)
			classFileSize += method_info.classFileSize();

		if (attribute_info != null)
			classFileSize += attribute_info.classFileSize();

		try {
			ClassFormatOutput cfo = new ClassFormatOutput(classFileSize + 200);

			put(cfo);

			return new ByteArray(cfo.getData(), 0, cfo.size());

		} catch (IOException e) {
			return null;
		}

	}

	/*
	** Public methods from ClassMember
	*/

	/** @see ClassMember
	*/
	public int getModifier() { return access_flags; }

	/** @see ClassMember
	*/
	public String getName() {
		return className(this_class).replace('/', '.');
	}
	/*
	**	Public methods from ClassHolder
	*/

	/** @see ClassHolder#addMember */
 	public ClassMember addMember(String simpleName, String descriptor, int modifier)
	{
		if (SanityManager.DEBUG)
		{
			if (descriptor.startsWith("(")) {
				if (method_info != null) {
					if (method_info.find(simpleName, descriptor) != null) {
						SanityManager.THROWASSERT("Method already exists " + simpleName + " " + descriptor);
					}
				}

			} else {
				if (field_info != null) {
					if (field_info.find(simpleName, descriptor) != null) {
						SanityManager.THROWASSERT("Field already exists " + simpleName + " " + descriptor);
					}
				}
			}
		}

		CONSTANT_Utf8_info utf = addUtf8Entry(simpleName);

		int nameIndex = utf.getIndex();
		int descriptorIndex = addUtf8Entry(descriptor).getIndex();

		ClassMember item = new ClassMember(this, modifier, nameIndex, descriptorIndex);
		MemberTable mt;
		if (descriptor.startsWith("(")) {
			mt = method_info;
			if (mt == null)
				mt = method_info = new MemberTable(0);

		}
		else {
			mt = field_info;
			if (mt == null)
				mt = field_info = new MemberTable(0);
		}

		mt.addEntry(item);
		return item;
	}

	/** @see ClassHolder#addFieldReference */
	public int addFieldReference(String className, String simpleName, String descriptor) {
		return addReference(VMDescriptor.CONSTANT_Fieldref, className, simpleName, descriptor);
	}

	public int addFieldReference(ClassMember field) {
		return addReference(VMDescriptor.CONSTANT_Fieldref, (ClassMember) field);
	}

	/** @see ClassHolder#addMethodReference */
	public int addMethodReference(String className, String simpleName, String descriptor, boolean isInterface) {

		int tag = isInterface ?	VMDescriptor.CONSTANT_InterfaceMethodref :
								VMDescriptor.CONSTANT_Methodref;

		return addReference(tag, className, simpleName, descriptor); 
	}

	private int addReference(int tag, String className, String simpleName, String descriptor) {

		int classIndex = addClassReference(className);
		int nameTypeIndex = addNameAndType(simpleName, descriptor);

		return addIndexReference(tag, classIndex, nameTypeIndex);
	}

	private int addReference(int tag, ClassMember member) {

		int nameTypeIndex = addIndexReference(VMDescriptor.CONSTANT_NameAndType,
							member.name_index, member.descriptor_index);

		return addIndexReference(tag, this_class, nameTypeIndex);
	}

	/** @see ClassHolder#addConstant */
	public int addConstant(String value) {

		return addString(value);
	}

	/** @see ClassHolder#addUtf8 */
	public int addUtf8(String value) {

		return addUtf8Entry(value).getIndex();
	}


	/** @see ClassHolder#addInteger */
	public int addConstant(int value) {
		return addDirectEntry(new CONSTANT_Integer_info(value));
	}

	/** @see ClassHolder#addFloat */
	public int addConstant(float value) {
		return addDirectEntry(new CONSTANT_Float_info(value));
	}

	/** @see ClassHolder#addLong */
	public int addConstant(long value) {
		return addDirectEntry(new CONSTANT_Long_info(value));
	}

	/** @see ClassHolder#addDouble */
	public int addConstant(double value) {
		return addDirectEntry(new CONSTANT_Double_info(value));
	}


	/** @see ClassMember
	*/
	public int getConstantPoolIndex() { return this_class; }

	public void addAttribute(String attributeName, ClassFormatOutput info) {

		if (attribute_info == null)
			attribute_info = new Attributes(1);


		CONSTANT_Utf8_info autf = addUtf8Entry(attributeName);

		int index = autf.getIndex();

		attribute_info.addEntry(new AttributeEntry(index, info));
	}


	public String getSuperClassName() {
		if (super_class == 0)
			return null;
		else
			return className(super_class).replace('/', '.');
	}


/*
    public ClassMember getMemberReference(String fullyQualifiedClassName, String simpleName, String descriptor) {

		int classIndex;

		if (fullyQualifiedClassName == null)
			 classIndex = this_class;
		else
			classIndex = constantPool.findClass(fullyQualifiedClassName);

		if (classIndex < 0)
			return null;

		int nameAndTypeIndex = constantPool.findNameAndType(simpleName, descriptor);
		if (nameAndTypeIndex < 0)
			return null;

        return constantPool.findReference(classIndex, nameAndTypeIndex);
	}
*/
	/*
	** Public methods from ClassRead
	*/



	/*
	** Implementation specific methods.
	*/

	/*
	** Methods related to Constant Pool Table
	*/
	/**
		Generic add entry to constant pool. Includes the logic
		for an entry to occupy more than one slot (e.g. long).

		@return The number of slots occupied by the entry.
.
	*/
	protected int addEntry(Object key, ConstantPoolEntry item) {

		item.setIndex(cptEntries.size());
		if (key != null)
			cptHashTable.put(key, item);
		cptEntries.addElement(item);

		cptEstimatedSize += item.classFileSize();

		if (item.doubleSlot()) {
			cptEntries.addElement(null);
			return 2;
		} else {
			return 1;
		}
	}
	
	/**
		Add an entry, but only if it doesn't exist.

		@return the constant pool index of the added
		or existing item.
	*/
	private int addDirectEntry(ConstantPoolEntry item) {
		ConstantPoolEntry existingItem = findMatchingEntry(item);
		if (existingItem != null) {
			item = existingItem;
			//foundCount++;
		}
		else {
			addEntry(item.getKey(), item);
		}
		return item.getIndex();
	}

	/**
		Add an index reference.
	*/
	private int addIndexReference(int tag, int i1, int i2) {

		// search for the item using the pre-allocated object 
		searchIndex.set(tag, i1, i2);

		ConstantPoolEntry item = findMatchingEntry(searchIndex);

		if (item == null) {
			item = new CONSTANT_Index_info(tag, i1, i2);
			addEntry(item.getKey(), item);
		}

		return item.getIndex();
	}

	/**
		Add a class entry to the pool.
	*/
	public int addClassReference(String fullyQualifiedName) {
		if (ClassHolder.isExternalClassName(fullyQualifiedName)) {
			fullyQualifiedName = ClassHolder.convertToInternalClassName(fullyQualifiedName);
			// System.out.println("addClassReference " + fullyQualifiedName);
		}

		int name_index = addUtf8Entry(fullyQualifiedName).getIndex();

		return addIndexReference(VMDescriptor.CONSTANT_Class, name_index, 0);
	}

	/**
		Add a name and type entry
	*/
	private int addNameAndType(String name, String descriptor) {
		int nameIndex = addUtf8Entry(name).getIndex();

		int descriptorIndex = addUtf8Entry(descriptor).getIndex();

		return addIndexReference(VMDescriptor.CONSTANT_NameAndType, nameIndex, descriptorIndex);
	}

	/**
		Add a UTF8 into the pool and return the index to it.
	*/
	private CONSTANT_Utf8_info addUtf8Entry(String value) {

		CONSTANT_Utf8_info item = (CONSTANT_Utf8_info) findMatchingEntry(value);

		if (item == null) {

			item = new CONSTANT_Utf8_info(value);
			addEntry(value, item);
		}
		return item;
	}
	/**
		Add an extra UTF8 into the pool 
	*/
	private CONSTANT_Utf8_info addExtraUtf8(String value) {

		CONSTANT_Utf8_info item = new CONSTANT_Utf8_info(value);
		addEntry(null, item);

		return item;
	}

	/**
		Add a string entry
	*/
	private int addString(String value) {
		CONSTANT_Utf8_info sutf = addUtf8Entry(value);
		int valueIndex = sutf.setAsString();
		if (valueIndex == 0) {
			// string is already being used as code
			valueIndex = addExtraUtf8(value).getIndex();
			sutf.setAlternative(valueIndex);
		}

		return addIndexReference(VMDescriptor.CONSTANT_String, valueIndex, 0);
	}

	/**
		Add a string entry
	*/
	private int addCodeUtf8(String value) {
		CONSTANT_Utf8_info sutf = addUtf8Entry(value);
		int index = sutf.setAsCode();
		if (index == 0) {
			// code string is already being used as string
			CONSTANT_Utf8_info eutf = addExtraUtf8(value);
			eutf.setAsCode(); // ensure the replace will happen
			index = eutf.getIndex();
			sutf.setAlternative(index);
		}

		return index;
	}
 	protected void cptPut(ClassFormatOutput out) throws IOException {

		for (Enumeration e = cptEntries.elements(); e.hasMoreElements(); ) {
			ConstantPoolEntry item = (ConstantPoolEntry) e.nextElement();
			if (item == null) {
				continue;
			}

			item.put(out);
		}
	}

	/*
	** Methods to convert indexes to constant pool entries and vice-versa.
	*/

	ConstantPoolEntry getEntry(int index) {
		return (ConstantPoolEntry) cptEntries.elementAt(index);
	}

	/**
		Return the class name for an index to a CONSTANT_Class_info.
	*/

	protected String className(int classIndex) {
		CONSTANT_Index_info ci = (CONSTANT_Index_info) getEntry(classIndex);

		return nameIndexToString(ci.getI1()).replace('/', '.');

	}

	/*
	** Methods to find specific types of constant pool entries.
	   In these methods we try to avoid using the ConstantPoolEntry.matchValue()
	   as that requires creating a new object for the search. The matchValue()
	   call is really intended for when objects are being added to the constant pool.
	*/

	/**
		Return the index of a UTF entry or -1 if it doesn't exist.
	*/
	int findUtf8(String value) {

		ConstantPoolEntry item = findMatchingEntry(value);
		if (item == null)
			return -1;

		return item.getIndex();
	}

	/**
		Find a class descriptor (section 4.4.1) and return its
		index, returns -1 if not found.
	*/
	public int findClass(String fullyQualifiedName) {
		String internalName = ClassHolder.convertToInternalClassName(fullyQualifiedName);
		int utf_index = findUtf8(internalName);
		if (utf_index < 0)
			return -1;

		return findIndexIndex(VMDescriptor.CONSTANT_Class,
			utf_index, 0);
	}


	/**
		Find a name and type descriptor (section 4.4.6) and
		return ita index. returns -1 if not found.
	*/
	public int findNameAndType(String name, String descriptor) {

		int name_index = findUtf8(name);
		if (name_index < 0)
			return -1;
		int descriptor_index = findUtf8(descriptor);
		if (descriptor_index < 0)
			return -1;

		return findIndexIndex(VMDescriptor.CONSTANT_NameAndType,
			name_index, descriptor_index);
	}
/*
	public ClassMember findReference(int classIndex, int nameAndTypeIndex) {

		CONSTANT_Index_info item = findIndexEntry(VMDescriptor.CONSTANT_Methodref,
				classIndex, nameAndTypeIndex);

		if (item == null) {

			item = findIndexEntry(VMDescriptor.CONSTANT_InterfaceMethodref,
				classIndex, nameAndTypeIndex);

			if (item == null) {
				item = findIndexEntry(VMDescriptor.CONSTANT_Fieldref,
					classIndex, nameAndTypeIndex);

				if (item == null)
					return null;

			}
		}

		return new ReferenceMember(this, item);
	}
*/
	protected CONSTANT_Index_info findIndexEntry(int tag, int i1, int i2) {
		// search for the item using the pre-allocated object 
		searchIndex.set(tag, i1, i2);

		return (CONSTANT_Index_info) findMatchingEntry(searchIndex);
	}

	protected int findIndexIndex(int tag, int i1, int i2) {
		CONSTANT_Index_info item = findIndexEntry(tag, i1, i2);
		if (item == null)
			return -1;

		return item.getIndex();
	}

	protected ConstantPoolEntry findMatchingEntry(Object key) {
		return (ConstantPoolEntry) cptHashTable.get(key);
	}

	/** get a string (UTF) given a name_index into the constant pool
	   */
	String nameIndexToString(int index) {

		return getEntry(index).toString();
	}

	/** get the class name of a Class given the index of its CONSTANT_Class_info
	    entry in the Constant Pool.
		*/

	protected String getClassName(int index) {

		if (index == 0)
			return ""; // must be the super class of java.lang.Object, ie. nothing.

		return 	nameIndexToString(getEntry(index).getI1());
	}

	/*
	 * Determine whether the class descriptor string is 
	 * in external format or not.  Assumes that to be in external
	 * format means it must have a '.' or end in an ']'.
	 * 
	 * @param className	the name of the class to check
	 *
	 * @return true/false
	 */
	public static boolean isExternalClassName(String className)
	{
		int len;
		if (className.indexOf('.') != -1)
		{
			return true;
		}
		else if ((len = className.length()) == 0)
		{ 
			return false;
		}
		return (className.charAt(len - 1) == ']');
	}

	/*
	 * Convert a class name to the internal VM class name format.
	   See sections 4.3.2, 4.4.1 of the vm spec.
	 * The normal leading 'L' and trailing ';' are left
	 * off of objects.  This is intended primarily for
	 * the class manager.
	 * <p>
	 * An example of a conversion would be java.lang.Double[]
	 * to "[Ljava/lang/Double;".
	 <BR>
	   java.lang.Double would be converted to "java/lang/Double"

	<BR>
	Note that for array types the result of convertToInternalClassName()
	and convertToInternalDescriptor() are identical.

	 *
	 * @param the external name (cannot be null)
	 *
	 * @return the internal string
	 */
	public static String convertToInternalClassName(String externalName)
	{
		return convertToInternal(externalName, false);
	}

	/*
	 * Convert a class name to internal JVM descriptor format.
	   See sections 4.3.2 of the vm spec.
	 * <p>
	 * An example of a conversion would be "java.lang.Double[]"
	 * to "[Ljava/lang/Double;".
	 *
	 <BR>
	   java.lang.Double would be converted to "Ljava/lang/Double;"

	<BR>
	Note that for array types the result of convertToInternalClassName()
	and convertToInternalDescriptor() are identical.

	 * @param the external name (cannot be null)
	 *
	 * @return the internal string
	 */
	public static String convertToInternalDescriptor(String externalName)
	{
		return convertToInternal(externalName, true);
	}

	/*
	** Workhorse method.  Convert to internal format.

		@param descriptor True if converting to descriptor format, false if
		converting to class name format.
	**
	** Lifted from BCClass.java. 
	**
	** Returns the result string.
	*/
	private static String convertToInternal(String externalName, boolean descriptor)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(externalName != null, "unexpected null");
		}

		int len = externalName.length();

		String internalName;	
		String retVal = null;	
		int origLen = len;
		int arity = 0;

		// first walk through all array-ness
		if (externalName.charAt(len-1) == ']')
		{
			while (len > 0
				&& externalName.charAt(len-1) == ']'
				&& externalName.charAt(len-2) == '[') 
			{
				len -= 2;
				arity++;
			}
		}
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(len > 0);
		}

		internalName = (origLen == len)? 
						  externalName 
						: externalName.substring(0,len);

	    // then check for primitive types ... 
		// in length by expected frequency order

		switch (len) {
			case 7 :
		        if ("boolean".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_BOOLEAN, arity);
				}
				break;
			case 4 :
		        if ("void".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_VOID, arity);
				}
		        else if ("long".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_LONG, arity);
				}
		        else if ("byte".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_BYTE, arity);
				}
		        else if ("char".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_CHAR, arity);
				}
				break;
			case 3 :
		        if ("int".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_INT, arity);
				}
				break;
			case 6 :
		        if ("double".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_DOUBLE, arity);
				}
				break;
			case 5 :
		        if ("short".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_SHORT, arity);
				}
		        else if ("float".equals(internalName)) {
					retVal = makeDesc(VMDescriptor.C_FLOAT, arity);
				}
				break;
		}

		// then it must be a Java class
		if (retVal == null)
			retVal = makeDesc(internalName, arity, descriptor);

		return retVal;
	}

	/**
		A helper to build a type description based on a built-in type
		and an array arity.
	 */
	static private String makeDesc (char builtin, int arity) {
		if (arity == 0)
			switch (builtin) {
				case VMDescriptor.C_BYTE : return VMDescriptor.BYTE;
				case VMDescriptor.C_CHAR : return VMDescriptor.CHAR;
				case VMDescriptor.C_DOUBLE : return VMDescriptor.DOUBLE;
				case VMDescriptor.C_FLOAT : return VMDescriptor.FLOAT;
				case VMDescriptor.C_INT : return VMDescriptor.INT;
				case VMDescriptor.C_LONG : return VMDescriptor.LONG;
				case VMDescriptor.C_SHORT : return VMDescriptor.SHORT;
				case VMDescriptor.C_BOOLEAN : return VMDescriptor.BOOLEAN;
				case VMDescriptor.C_VOID : return VMDescriptor.VOID;
				default: 
					if (SanityManager.DEBUG)
						SanityManager.THROWASSERT("No type match");
					return null;
			}
		else {
			StringBuffer desc = new StringBuffer(arity+3);

			for (int i=0;i<arity;i++)
				desc.append(VMDescriptor.C_ARRAY);

			desc.append(ClassHolder.makeDesc(builtin, 0));

			return desc.toString();
		}
	}

	/**
		A helper to build a type description based on a Java class
		and an array arity.

		If descriptor is true create a descriptor according to
		section 4.3.2 of the vm spec. If false create a class name
		according to sections 4.3.2 and 4.4.1 of the vm spec.
	
	 */
	static private String makeDesc (String className, int arity, boolean descriptor) {

		if (!descriptor && (arity == 0)) {
			return className.replace('.','/');
		}

		StringBuffer desc = new StringBuffer(arity+2+className.length());

		for (int i=0;i<arity;i++)
			desc.append(VMDescriptor.C_ARRAY);

		desc.append(VMDescriptor.C_CLASS);

		desc.append(className.replace('.','/'));

		desc.append(VMDescriptor.C_ENDCLASS);

		return desc.toString();
	}


}
