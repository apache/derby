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
 * The VariableSizeDataValue interface corresponds to 
 * Datatypes that have adjustable width. 
 *
 * The following methods are defined herein:
 *		setWidth()
 *
 * @author	jamie
 */
public interface VariableSizeDataValue 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	public static int IGNORE_PRECISION = -1;

	/*
	 * Set the width and scale (if relevant).  Sort of a poor
	 * man's normalize.  Used when we need to normalize a datatype
	 * but we don't want to use a NormalizeResultSet (e.g.
	 * for an operator that can change the width/scale of a
	 * datatype, namely CastNode).
	 *
	 * @param desiredWidth width
	 * @param desiredScale scale, if relevant (ignored for strings)
	 * @param errorOnTrunc	throw an error on truncation of value
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor setWidth(int desiredWidth,
									int desiredScale,
									boolean errorOnTrunc)
							throws StandardException;
}
