/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.compile;

/**
 * Rudimentary structure for containing information about
 * a REFERENCING clause for a trigger.
 *
 * @author jamie
 */
public class TriggerReferencingStruct 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	public String identifier;
	public boolean isRow;
	public boolean isNew;

	public TriggerReferencingStruct
	(
		boolean	isRow, 
		boolean	isNew,
		String	identifier
	)
	{
		this.isRow = isRow;
		this.isNew = isNew;
		this.identifier = identifier;
	}

	public String toString()
	{
		return (isRow ? "ROW " : "TABLE ")+(isNew ? "new: " : "old: ") + identifier;
	}
}
