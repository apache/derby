/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog.types
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog.types;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.Types;

public class DecimalTypeIdImpl extends BaseTypeIdImpl
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	/* this class is needed because writeexternal for this class stores
	   extra information; when the object is sent over the wire the niladic
	   constructor is first called and then we call the readExternal method. 
	   the readExternal needs to know the formatId atleast for decimal types
	   to read the extra information.
	*/
	public DecimalTypeIdImpl() 
	{
		super(StoredFormatIds.DECIMAL_TYPE_ID_IMPL);
	}
	
	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		boolean isNumeric = in.readBoolean();

		super.readExternal(in);

		if (isNumeric)
		{
			setNumericType();
		}

	}

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		out.writeBoolean(JDBCTypeId == Types.NUMERIC);

		super.writeExternal(out);
	}

	public void setNumericType()
	{
		SQLTypeName = "NUMERIC";
		JDBCTypeId = Types.NUMERIC;
	}
}
