/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.store.access.ColumnOrdering;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Formatable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
/**
	Basic implementation of ColumnOrdering.
	Not sure what to tell callers about 0-based versus 1-based numbering.
	Assume 0-based for now.

	@author ames
 */
public class IndexColumnOrder implements ColumnOrdering, Formatable
{
	/********************************************************
	**
	**	This class implements Formatable. That means that it
	**	can write itself to and from a formatted stream. If
	**	you add more fields to this class, make sure that you
	**	also write/read them with the writeExternal()/readExternal()
	**	methods.
	**
	**	If, inbetween releases, you add more fields to this class,
	**	then you should bump the version number emitted by the getTypeFormatId()
	**	method.
	**
	********************************************************/

	int colNum;
	boolean ascending;

	/*
	 * class interface
	 */

	/**
	 * Niladic constructor for formatable
	 */
	public IndexColumnOrder() 
	{
	}

	public IndexColumnOrder(int colNum) {
		 this.colNum = colNum;
		 this.ascending = true;
	}

	public IndexColumnOrder(int colNum, boolean ascending) {
		 this.colNum = colNum;
		 this.ascending = ascending;
	}

	/*
	 * ColumnOrdering interface
 	 */
	public int getColumnId() {
		return colNum;
	}

	public boolean getIsAscending() {
		return ascending;
	}

	//////////////////////////////////////////////
	//
	// FORMATABLE
	//
	//////////////////////////////////////////////
	/**
	 * Write this object out
	 *
	 * @param out write bytes here
	 *
 	 * @exception IOException thrown on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(colNum);
		out.writeBoolean(ascending);
	}

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
		colNum = in.readInt();
		ascending = in.readBoolean();
	}
	
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.INDEX_COLUMN_ORDER_V01_ID; }
}
