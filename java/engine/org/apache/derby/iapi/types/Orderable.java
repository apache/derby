/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;

/** 

  The Orderable interface represents a value that can
  be linearly ordered.
  <P>
  Currently only supports linear (<, =, <=) operations.
  Eventually we may want to do other types of orderings,
  in which case there would probably be a number of interfaces
  for each "class" of ordering.
  <P>
  The implementation must handle the comparison of null
  values.  This may require some changes to the interface,
  since (at least in some contexts) comparing a value with
  null should return unknown instead of true or false.

**/

public interface Orderable
{
	/**
		IBM Copyright &copy notice.
	*/
 
    public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	/**	 Ordering operation constant representing '<' **/
	static final int ORDER_OP_LESSTHAN = 1;
	/**	 Ordering operation constant representing '=' **/
	static final int ORDER_OP_EQUALS = 2;
	/**	 Ordering operation constant representing '<=' **/
	static final int ORDER_OP_LESSOREQUALS = 3;

	/** 
	 * These 2 ordering operations are used by the language layer
	 * when flipping the operation due to type precedence rules.
	 * (For example, 1 < 1.1 -> 1.1 > 1)
	 */
	/**	 Ordering operation constant representing '>' **/
	static final int ORDER_OP_GREATERTHAN = 4;
	/**	 Ordering operation constant representing '>=' **/
	static final int ORDER_OP_GREATEROREQUALS = 5;


}
