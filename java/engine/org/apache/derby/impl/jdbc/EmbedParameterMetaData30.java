/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.jdbc
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.sql.ParameterValueSet;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import java.sql.ParameterMetaData;

/**
 * This class implements the ParameterMetaData interface from JDBC3.0
 * It provides the parameter meta data for callable & prepared statements
 * But note that the bulk of it resides in its parent class.  The reason is
 * we want to provide the functionality to the JDKs before JDBC3.0.
 *
 * @see java.sql.ParameterMetaData
 *
 */
class EmbedParameterMetaData30 extends org.apache.derby.impl.jdbc.EmbedParameterSetMetaData
    implements ParameterMetaData {
	/**
		IBM Copyright &copy notice.
	*/

    public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
    EmbedParameterMetaData30(ParameterValueSet pvs, DataTypeDescriptor[] types)  {
		super(pvs, types);
    }

}

