/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.bytecode
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.bytecode;

import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.services.classfile.VMOpcode;

class BCLocalField implements LocalField {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	final int	   cpi; // of the Field Reference
	final Type     type;

	BCLocalField(Type type, int cpi) {
		this.cpi = cpi;
		this.type = type;
	}
}
