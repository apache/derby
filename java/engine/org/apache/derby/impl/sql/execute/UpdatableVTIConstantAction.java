/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;


import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;

import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

import org.apache.derby.catalog.UUID;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.Serializable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.Properties;

/**
 *	This class  describes compiled constants that are passed into
 *	Updatable VTIResultSets.
 *
 *	@author Jerry Brenner
 */

public class UpdatableVTIConstantAction extends WriteCursorConstantAction
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	/********************************************************
	**
	**	This class implements Formatable. But it is NOT used
 	**	across either major or minor releases.  It is only
	** 	written persistently in stored prepared statements, 
	**	not in the replication stage.  SO, IT IS OK TO CHANGE
	**	ITS read/writeExternal.
	**
	********************************************************/

	public int[]	changedColumnIds;

    public int statementType;
    
	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	UpdatableVTIConstantAction() { super(); }

	/**
	 *	Make the ConstantAction for an updatable VTI statement.
	 *
	 * @param deferred					Whether or not to do operation in deferred mode
     * @param changedColumnIds Array of ids of changed columns
	 *
	 */
	public	UpdatableVTIConstantAction( int statementType,
                                        boolean deferred,
                                        int[] changedColumnIds)
	{
		super(0, 
			  null,
			  null, 
			  null, 
			  null,
			  null, 
			  deferred	, 
			  null,
			  null,
			  0,
			  null,	
			  null,
			  (ExecRow)null, // never need to pass in a heap row
			  null,
			  null,
			  null,
			  // singleRowSource, irrelevant
			  false
			  );
        this.statementType = statementType;
        this.changedColumnIds = changedColumnIds;
	}

	// INTERFACE METHODS

	/**
	 * Does this constant action modify the passed in table
	 * uuid?  By modify we mean add or drop things tied to
	 * this table (e.g. index, trigger, constraint).  Things
	 * like views or spses that reference this table don't
	 * count.
	 *
	 * @param tableId on error
	 *
	 */
	public boolean modifiesTableId(UUID tableId)
	{
		return false;
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.UPDATABLE_VTI_CONSTANT_ACTION_V01_ID; }


	/**
	 *	NOP routine. The work is done in subclass.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
		throws StandardException { }

	// CLASS METHODS

}
