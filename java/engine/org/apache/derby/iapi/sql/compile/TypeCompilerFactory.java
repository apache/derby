/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.compile;

import org.apache.derby.iapi.types.TypeId;

/**
 * Factory interface for the compilation part of datatypes.
 *
 * @author Jeff Lichtman
 */

public interface TypeCompilerFactory
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	public static final String MODULE = "org.apache.derby.iapi.sql.compile.TypeCompilerFactory";

	/**
	 * Get a TypeCompiler corresponding to the given TypeId.
	 *
	 * @return	A TypeCompiler
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public TypeCompiler getTypeCompiler(TypeId typeId);
}

