/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.catalog.UUID;

/**
 *	Constant action to drop an external jar file from a database. 
 *
 */
class DropJarConstantAction extends DDLConstantAction
{

	private final UUID id;
	private final String schemaName;
	private final String sqlName;

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////

	/**
	 *	Make the ConstantAction to drop a jar file to database.
	 *
	 *	@param	id					The id for the jar file
	 *	@param	schemaName			The SchemaName for the jar file.
	 *	@param	sqlName			    The sqlName for the jar file.
	 */
	DropJarConstantAction(UUID id,
								  String schemaName,
								  String sqlName)
	{
		this.id = id;
		this.schemaName = schemaName;
		this.sqlName = sqlName;
	}


	//////////////////////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	//////////////////////////////////////////////////////////////

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP JAR FILE " + schemaName + "." + sqlName;
	}

	//////////////////////////////////////////////////////////////
	//
	// CONSTANT ACTION METHODS
	//
	//////////////////////////////////////////////////////////////


	/**
	 * @see ConstantAction#executeConstantAction
	 * @exception StandardException Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		JarUtil.drop(null,schemaName,sqlName,
					 purgeOnCommit());
	}

	//
	// Replication can over-ride this to defer purging dropped jar
	// files that remain in the stage.
	protected boolean purgeOnCommit() { return true; }

}
