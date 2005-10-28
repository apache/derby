/*

   Derby - Class org.apache.derby.iapi.services.classfile.ClassInvestigator

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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


import java.io.InputStream;
import java.util.Enumeration;

import java.io.IOException;
import java.util.Vector;

import org.apache.derby.iapi.services.classfile.VMDescriptor;
import org.apache.derby.iapi.services.classfile.VMDescriptor;
import java.util.HashSet;

import java.util.Hashtable;
import java.util.Vector;
import java.util.Enumeration;
import java.util.Collections;


/** 
*/

public class ClassInvestigator extends ClassHolder {

	public static ClassInvestigator load(InputStream is)
		throws IOException {

		ClassInput classInput = new ClassInput(is);

		// Check the header
		checkHeader(classInput);

		//	Read in the Constant Pool
		int constantPoolCount = classInput.getU2();

		ClassInvestigator ci = new ClassInvestigator(constantPoolCount);
		// Yes, index starts at 1, The '0'th constant pool entry
		// is reserved for the JVM and is not present in the class file.
		for (int i = 1; i < constantPoolCount; ) {
			ConstantPoolEntry item = ClassInvestigator.getConstant(classInput);
			i += ci.addEntry(item.getKey(), item);
		}

		// Read in access_flags and class indexes
		ci.access_flags = classInput.getU2();
		ci.this_class = classInput.getU2();
		ci.super_class = classInput.getU2();

		// interfaces is a simple int array
		int interfaceCount = classInput.getU2();
		if (interfaceCount != 0) {
			ci.interfaces = new int[interfaceCount];
			for (int i = 0; i < interfaceCount; i++)
				ci.interfaces[i] = classInput.getU2();
		}

		int fieldCount = classInput.getU2();
		if (fieldCount != 0) {
			ci.field_info = new MemberTable(fieldCount);
			for (int i = 0; i < fieldCount; i++)
			{
				ci.field_info.addEntry(readClassMember(ci, classInput));
			}
		}

		int methodCount = classInput.getU2();
		if (methodCount != 0) {
 			ci.method_info = new MemberTable(methodCount);
			for (int i = 0; i < methodCount; i++)
			{
				ci.method_info.addEntry(readClassMember(ci, classInput));
			}
		}

		int attributeCount = classInput.getU2();
		if (attributeCount != 0) {
			ci.attribute_info = new Attributes(attributeCount);

			for (int i = 0; i < attributeCount; i++)
				ci.attribute_info.addEntry(new AttributeEntry(classInput));
		}
		return ci;

	}

	private static ClassMember readClassMember(ClassInvestigator ci, ClassInput in)
		throws IOException {

		ClassMember member = new ClassMember(ci, in.getU2(),  in.getU2(), in.getU2());

		int attributeCount = in.getU2();
		if (attributeCount != 0) {
			member.attribute_info = new Attributes(attributeCount);
			for (int i = 0; i < attributeCount; i++)
				member.attribute_info.addEntry(new AttributeEntry(in));
		}
			
		return member;
	}

	/*
	**	Constructors.
	*/

	private ClassInvestigator(int constantPoolCount) {
		super(constantPoolCount);
	}

	/*
	** Methods to investigate this class
	*/


	public Enumeration implementedInterfaces()
	{
		int interfaceCount = interfaces == null ? 0 : interfaces.length;
		Vector implemented = new Vector(interfaceCount);

        for (int i = 0; i < interfaceCount; i++)
        {
            implemented.addElement(className(interfaces[i]));
        }
        return implemented.elements();
	}
    public Enumeration getFields() {
		if (field_info == null)
			return Collections.enumeration(Collections.EMPTY_LIST);

		return field_info.entries.elements();
	}

    public Enumeration getMethods() {
		if (method_info == null)
			return Collections.enumeration(Collections.EMPTY_LIST);
		return method_info.entries.elements();
	}

    public Enumeration referencedClasses() {
        return getClasses(getMethods(), getFields() );
    }

	/**
		Return an Enumeration of all referenced classes
	*/

	private Enumeration getClasses(Enumeration methods, Enumeration fields)
	{
		return new ClassEnumeration(this, cptEntries.elements(), methods, fields);
	}

	public Enumeration getStrings() {
		HashSet strings = new HashSet(30, 0.8f);
		
		int size = cptEntries.size();
		for (int i = 1; i < size; i++) {
			ConstantPoolEntry cpe = getEntry(i);

			if ((cpe == null) || (cpe.getTag() != VMDescriptor.CONSTANT_String))
				continue;

			CONSTANT_Index_info cii = (CONSTANT_Index_info) cpe;

			strings.add(nameIndexToString(cii.getI1()));
		}

		return java.util.Collections.enumeration(strings);
	}

    public ClassMember getMember(String simpleName, String descriptor) {

		if (descriptor.startsWith("(")) {
			if (method_info == null)
				return null;
			return method_info.find(simpleName, descriptor);
		}
		else {
			if (field_info == null)
				return null;
			return  field_info.find(simpleName, descriptor);
		}
	}

	/**
		Return an Enumeration of all Member References
	*/
/*
	Enumeration getMemberReferences() {
		return new ReferenceEnumeration(this, elements());
	}
*/

	/*
	** Methods to modify the class.
	*/
	// remove all atttributes that are not essential
	public void removeAttributes() throws IOException {

		// Class level attributes
		if (attribute_info != null) {
			for (int i = attribute_info.size() - 1; i >= 0 ; i--) {

				AttributeEntry ae = (AttributeEntry) attribute_info.elementAt(i);
				String name = nameIndexToString(ae.getNameIndex());
				if (name.equals("SourceFile"))
					attribute_info.removeElementAt(i);
				else if (name.equals("InnerClasses"))
					; // leave in
				else
					System.err.println("WARNING - Unknown Class File attribute " + name);
			}

			if (attribute_info.size() == 0)
				attribute_info = null;
		}
		attribute_info = null;

		// fields
		for (Enumeration e = getFields(); e.hasMoreElements(); ) {
			ClassMember member = (ClassMember) e.nextElement();

			Attributes attrs = member.attribute_info;

			if (attrs != null) {

				for (int i = attrs.size() - 1; i >= 0 ; i--) {

					AttributeEntry ae = (AttributeEntry) attrs.elementAt(i);
					String name = nameIndexToString(ae.getNameIndex());
					if (name.equals("ConstantValue"))
						; // leave in
					else if (name.equals("Synthetic"))
						; // leave in
					else
						System.err.println("WARNING - Unknown Field attribute " + name);
				}

				if (attrs.size() == 0)
					member.attribute_info = null;
			}

		}

		// methods
		for (Enumeration e = getMethods(); e.hasMoreElements(); ) {
			ClassMember member = (ClassMember) e.nextElement();

			Attributes attrs = member.attribute_info;

			if (attrs != null) {

				for (int i = attrs.size() - 1; i >= 0 ; i--) {

					AttributeEntry ae = (AttributeEntry) attrs.elementAt(i);
					String name = nameIndexToString(ae.getNameIndex());
					if (name.equals("Code"))
						processCodeAttribute(member, ae);
					else if (name.equals("Exceptions"))
						; // leave in
					else if (name.equals("Deprecated"))
						; // leave in
					else if (name.equals("Synthetic"))
						; // leave in
					else
						System.err.println("WARNING - Unknown method attribute " + name);
				}

				if (attrs.size() == 0)
					member.attribute_info = null;
			}

		}
	}

	private void processCodeAttribute(ClassMember member, AttributeEntry ae) throws IOException {

		ClassInput ci = new ClassInput(new java.io.ByteArrayInputStream(ae.infoIn));


		ci.skipBytes(4); // puts us at code_length
		int len = ci.getU4();
		ci.skipBytes(len); // puts us at exception_table_length
		int count = ci.getU2();
		if (count != 0)
			ci.skipBytes(8 * count);

		int nonAttrLength = 4 + 4 + len + 2 + (8 * count);

		// now at attributes

		count = ci.getU2();
		if (count == 0)
			return;

		int newCount = count;
		for (int i = 0; i < count; i++) {

			int nameIndex = ci.getU2();
			String name = nameIndexToString(nameIndex);
			if (name.equals("LineNumberTable") || name.equals("LocalVariableTable"))
				newCount--;
			else
				System.err.println("ERROR - Unknown code attribute " + name);

			len = ci.getU4();
			ci.skipBytes(len);
		}

		if (newCount != 0) {
			System.err.println("ERROR - expecting all code attributes to be removed");
			System.exit(1);
		}

		// this is only coded for all attributes within a Code attribute being removed.

		byte[] newInfo = new byte[nonAttrLength + 2];
		System.arraycopy(ae.infoIn, 0, newInfo, 0, nonAttrLength);
		// last two bytes are left at 0 which means 0 attributes
		ae.infoIn = newInfo;
	}

	public void renameClassElements(Hashtable classNameMap, Hashtable memberNameMap) {

		// this & super class
		renameString(classNameMap, (CONSTANT_Index_info) getEntry(this_class));
		renameString(classNameMap, (CONSTANT_Index_info) getEntry(super_class));

		// implemented interfaces
		// handled by Class entries below

		// classes & Strings
		// descriptors
		int size = cptEntries.size();
		for (int i = 1; i < size; i++) {
			ConstantPoolEntry cpe = getEntry(i);

			if (cpe == null)
				continue;

			switch (cpe.getTag()) {
			case VMDescriptor.CONSTANT_String:
			case VMDescriptor.CONSTANT_Class:
				{
				CONSTANT_Index_info cii = (CONSTANT_Index_info) cpe;
				renameString(classNameMap, cii);
				break;
				}
			case VMDescriptor.CONSTANT_NameAndType:
				{
				CONSTANT_Index_info cii = (CONSTANT_Index_info) cpe;
				String newDescriptor = newDescriptor(classNameMap, nameIndexToString(cii.getI2()));
				if (newDescriptor != null) {
					doRenameString(cii.getI2(), newDescriptor);
				}
				break;
				}

			default:
				continue;
			}

		}

		//System.out.println("Starting Fields");

		// now the methods & fields, only descriptors at this time
		renameMembers(getFields(), classNameMap, memberNameMap);

		renameMembers(getMethods(), classNameMap, memberNameMap);
	}

	private void renameMembers(Enumeration e, Hashtable classNameMap, Hashtable memberNameMap) {

		for (; e.hasMoreElements(); ) {
			ClassMember member = (ClassMember) e.nextElement();

			String oldMemberName = nameIndexToString(member.name_index);
			String newMemberName = (String) memberNameMap.get(oldMemberName);
			if (newMemberName != null)
				doRenameString(member.name_index, newMemberName);

			String newDescriptor = newDescriptor(classNameMap, nameIndexToString(member.descriptor_index));
			if (newDescriptor != null) {
				doRenameString(member.descriptor_index, newDescriptor);
			}
		}

	}

	private void renameString(Hashtable classNameMap, CONSTANT_Index_info cii) {

		int index = cii.getI1();

		String name = nameIndexToString(index);
		String newName = (String) classNameMap.get(name);
		if (newName != null) {

			doRenameString(index, newName);

			return;
		}

		// have to look for arrays
		if (cii.getTag() == VMDescriptor.CONSTANT_Class) {

			if (name.charAt(0) == '[') {
				int classOffset = name.indexOf('L') + 1;

				String baseClassName = name.substring(classOffset, name.length() - 1);


				newName = (String) classNameMap.get(baseClassName);

				if (newName != null) {

					String newArrayClassName = name.substring(0, classOffset) + newName + ";";

					doRenameString(index, newArrayClassName);

				}

			}
		}
	}

	private void doRenameString(int index, String newName) {
		ConstantPoolEntry cpe = getEntry(index);
		if (cpe.getTag() != VMDescriptor.CONSTANT_Utf8)
			throw new RuntimeException("unexpected type " + cpe);

		CONSTANT_Utf8_info newCpe = new CONSTANT_Utf8_info(newName);

		cptHashTable.remove(cpe.getKey());
		cptHashTable.put(cpe.getKey(), cpe);

		newCpe.index = index;

		cptEntries.setElementAt(newCpe, index);
	}

	/*
	**
	*/
	static private void checkHeader(ClassInput in) throws IOException {
		int magic = in.getU4();
		int minor_version = in.getU2();
		int major_version = in.getU2();


		if (magic != VMDescriptor.JAVA_CLASS_FORMAT_MAGIC)
			   throw new ClassFormatError();
   	}
	private static ConstantPoolEntry getConstant(ClassInput in)
		throws IOException {

		ConstantPoolEntry item;
		int tag;		
		tag = in.readUnsignedByte();

		switch (tag) {
		case VMDescriptor.CONSTANT_Class:
		case VMDescriptor.CONSTANT_String:
			item = new CONSTANT_Index_info(tag, in.getU2(), 0);
			break;

		case VMDescriptor.CONSTANT_NameAndType:
		case VMDescriptor.CONSTANT_Fieldref:
		case VMDescriptor.CONSTANT_Methodref:
		case VMDescriptor.CONSTANT_InterfaceMethodref:
			item = new CONSTANT_Index_info(tag, in.getU2(), in.getU2());
			break;

		case VMDescriptor.CONSTANT_Integer:
			item = new CONSTANT_Integer_info(in.getU4());
			break;

		case VMDescriptor.CONSTANT_Float:
			item = new CONSTANT_Float_info(in.readFloat());
			break;

		case VMDescriptor.CONSTANT_Long:
			item = new CONSTANT_Long_info(in.readLong());
			break;

		case VMDescriptor.CONSTANT_Double:
			item = new CONSTANT_Double_info(in.readDouble());
			break;

		case VMDescriptor.CONSTANT_Utf8:
			item = new CONSTANT_Utf8_info(in.readUTF());
			break;
		default:
			throw new ClassFormatError();
		}

		return item;
	}
	public static String newDescriptor(Hashtable classNameMap, String descriptor) {

		String newDescriptor = null;

		int dlen = descriptor.length();
		for (int offset = 0; offset < dlen; ) {
			char c = descriptor.charAt(offset);
			switch (c) {
			case VMDescriptor.C_VOID :
			case VMDescriptor.C_BOOLEAN:
			case VMDescriptor.C_BYTE:
			case VMDescriptor.C_CHAR:
			case VMDescriptor.C_SHORT:
			case VMDescriptor.C_INT:
			case VMDescriptor.C_LONG:
			case VMDescriptor.C_FLOAT:
			case VMDescriptor.C_DOUBLE:
			case VMDescriptor.C_ARRAY:
			case VMDescriptor.C_METHOD:
			case VMDescriptor.C_ENDMETHOD:
			default:
				offset++;
				continue;

			case VMDescriptor.C_CLASS:
				{
				int startOffset = offset;
				while (descriptor.charAt(offset++) != VMDescriptor.C_ENDCLASS)
					;
				int endOffset = offset;

				// name includes L and ;
				String name = descriptor.substring(startOffset, endOffset);
				String newName = (String) classNameMap.get(name);
				if (newName != null) {
					if (newDescriptor == null)
						newDescriptor = descriptor;

					// we just replace the first occurance of it,
					// the loop will hit any next occurance.
					int startPos = newDescriptor.indexOf(name);

					String tmp;
					if (startPos == 0)
						tmp = newName;
					else
						tmp = newDescriptor.substring(0, startPos) +
								newName;

					int endPos = startPos + name.length();

					if (endPos < newDescriptor.length()) {

						tmp += newDescriptor.substring(endPos , newDescriptor.length());
					}


					newDescriptor = tmp;
				}
				}
			}


		}
		//if (newDescriptor != null) {
		//	System.out.println("O - " + descriptor);
		//	System.out.println("N - " + newDescriptor);
		//}
		return newDescriptor;
	}
}
