/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.bytecode
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.bytecode;

/**
 *
 * To be able to identify the expressions as belonging to this
 * implementation, and to be able to generate code off of
 * it if so.
 *
 */
interface BCExpr { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	// maybe these should go into Declarations, instead?
	// note there is no vm_boolean; boolean is an int
	// except in arrays, where it is a byte.
	short vm_void = -1; // not used in array mappings.
	short vm_byte = 0;
	short vm_short = 1;
	short vm_int = 2;
	short vm_long = 3;
	short vm_float = 4;
	short vm_double = 5;
	short vm_char = 6;
	short vm_reference = 7;

}
