/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.error.StandardException;

/**
 *	This interface describes the columns in a referenced constraint. Added
 *	to be the protocol version of ConstraintInfo.
 *
 * @version 0.1
 * @author Rick Hillegas
 */

public interface ConsInfo	extends	Formatable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	/**
	  *	This ConsInfo describes columns in a referenced table. What is
	  *	the schema that the referenced table lives in?
	  *
	  *	@param	dd	data dictionary to search for schema
	  *
	  *	@return	schema that referenced table lives in
	  *	@exception	StandardException thrown on oops
	  */
	public SchemaDescriptor getReferencedTableSchemaDescriptor(DataDictionary dd)
		throws StandardException;

	/**
	  *	This ConsInfo describes columns in a referenced table. What is
	  *	that table?
	  *
	  *	@param	dd	data dictionary to search for table
	  *
	  *	@return	referenced table
	  *	@exception	StandardException thrown on oops
	  */
	public TableDescriptor getReferencedTableDescriptor(DataDictionary dd)
		throws StandardException;

	/**
	  *	This ConsInfo describes columns in a referenced table. What are
	  *	their names?
	  *
	  *	@return	array of referenced column names
	  */
	public String[] getReferencedColumnNames();

	/**
	  *	Get the name of the table that these column live in.
	  *
	  *	@return	referenced table name
	  */
	public String getReferencedTableName();


	/**
	  *	Get the referential Action for an Update.
	  *
	  *	@return	referential Action for update
	  */

	public int getReferentialActionUpdateRule();
	
	/**
	  *	Get the referential Action for a Delete.
	  *
	  *	@return	referential Action Delete rule
	  */
	public int getReferentialActionDeleteRule();

}



