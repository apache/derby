/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.bytecode
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.bytecode;

import org.apache.derby.iapi.services.classfile.VMDescriptor;
import org.apache.derby.iapi.services.classfile.ClassHolder;

final class Type {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	static final Type LONG = new Type("long", VMDescriptor.LONG);
	static final Type INT = new Type("int", VMDescriptor.INT);
	static final Type SHORT = new Type("short", VMDescriptor.SHORT);
	static final Type BYTE = new Type("byte", VMDescriptor.BYTE);
	static final Type BOOLEAN = new Type("boolean", VMDescriptor.BOOLEAN);
	static final Type FLOAT = new Type("float", VMDescriptor.FLOAT);
	static final Type DOUBLE = new Type("double", VMDescriptor.DOUBLE);
	static final Type STRING = new Type("java.lang.String", "Ljava/lang/String;");

	private final String javaName; // e.g. java.lang.Object
	private final short vmType; // e.g. BCExpr.vm_reference
	private final String vmName; // e.g. Ljava/lang/Object;
	final String vmNameSimple; // e.g. java/lang/Object

	Type(String javaName, String vmName) {
		this.vmName = vmName;
		this.javaName = javaName;
		vmType = BCJava.vmTypeId(vmName);
		vmNameSimple = ClassHolder.convertToInternalClassName(javaName);
	}

	/*
	** Class specific methods.
	*/
	
	String javaName() {
		return javaName;
	}

	/**
	 * Get the VM Type name (java/lang/Object)
	 */
	String vmName() {
		return vmName;
	}
	/**
		Get the VM type (eg. VMDescriptor.INT)
	*/
	short vmType() {
		return vmType;
	}

	int width() {
		return Type.width(vmType);
	}

	static int width(short type) {
		switch (type) {
		case BCExpr.vm_void:
			return 0;
		case BCExpr.vm_long:
		case BCExpr.vm_double:
			return 2;
		default:
			return 1;
		}
	}
}
