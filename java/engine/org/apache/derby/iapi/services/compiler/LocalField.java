/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.compiler
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.compiler;

/**
	A field within the generated class.
 */
public interface LocalField {

	/**
		Return an expression that's the value of this field
	 */
	//Expression getField();

	/**
		Return an expression that assigns the passed
		in value to the field and returns the value set.
	 */
	//Expression putField(Expression value);
}
