/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.bytecode
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.bytecode;

/**
 */
class BCMethodCaller extends BCLocalField {

	final short opcode;

	BCMethodCaller(short opcode, Type type, int cpi) {
		super(type, cpi);
		this.opcode = opcode;
	}
}

