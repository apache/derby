/*

   Derby - Class org.apache.derby.iapi.services.classfile.VMDescriptor

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

package org.apache.derby.iapi.services.classfile;

public interface VMDescriptor {
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
