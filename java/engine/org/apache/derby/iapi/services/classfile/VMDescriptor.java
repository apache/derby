/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.classfile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.classfile;

public interface VMDescriptor {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
    char C_VOID = 'V';
    String VOID = "V";
    char C_BOOLEAN = 'Z';
    String BOOLEAN = "Z";
    char C_BYTE = 'B';
    String BYTE = "B";
    char C_CHAR = 'C';
    String CHAR = "C";
    char C_SHORT = 'S';
    String SHORT = "S";
    char C_INT = 'I';
    String INT = "I";
    char C_LONG = 'J';
    String LONG = "J";
    char C_FLOAT = 'F';
    String FLOAT = "F";
    char C_DOUBLE = 'D';
    String DOUBLE = "D";
    char C_ARRAY = '[';
    String ARRAY = "[";
    char C_CLASS = 'L';
    String CLASS = "L";
    char C_METHOD = '(';
    String METHOD = "(";
    char C_ENDCLASS = ';';
    String ENDCLASS = ";";
    char C_ENDMETHOD = ')';
    String ENDMETHOD = ")";
    char C_PACKAGE = '/';
    String PACKAGE = "/";

	/*
	** Constants for the constant pool tags.
	*/
		
	int CONSTANT_Class = 7;
	int CONSTANT_Fieldref = 9;
	int CONSTANT_Methodref = 10;
	int CONSTANT_InterfaceMethodref = 11;
	int CONSTANT_String = 8;
	int CONSTANT_Integer = 3;
	int CONSTANT_Float = 4;
	int CONSTANT_Long = 5;
	int CONSTANT_Double = 6;
	int CONSTANT_NameAndType = 12;
	int CONSTANT_Utf8 = 1;


	/** Magic number for class file format - page 84 */
	int JAVA_CLASS_FORMAT_MAGIC = 0xCAFEBABE;

	/** Major and minor versions numbers - 1.0.2 release - page 85 */
	int JAVA_CLASS_FORMAT_MAJOR_VERSION = 45;
	int JAVA_CLASS_FORMAT_MINOR_VERSION = 3;
}
