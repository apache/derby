/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.catalog
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 *	Class for implementation of DependableFinder in the core DataDictionary 
 *	for referenced columns in a table.
 *
 *
 * @author Tingjian Ge
 */

public class DDColumnDependableFinder extends DDdependableFinder
{
	////////////////////////////////////////////////////////////////////////
	//
	//  STATE
	//
	////////////////////////////////////////////////////////////////////////

	// write least amount of data to disk, just the byte array, not even
	// a FormatableBitSet
	private byte[] columnBitMap;

    ////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTORS
    //
    ////////////////////////////////////////////////////////////////////////

	/**
	 * Constructor same as in parent.
	 */
	public  DDColumnDependableFinder(int formatId)
	{
		super(formatId);
	}

	/**
	 * Constructor given referenced column bit map byte array as in FormatableBitSet
	 */
	public  DDColumnDependableFinder(int formatId, byte[] columnBitMap)
	{
		super(formatId);
		this.columnBitMap = columnBitMap;
	}

    ////////////////////////////////////////////////////////////////////////
    //
    //  DDColumnDependable METHODS
    //
    ////////////////////////////////////////////////////////////////////////

	/**
	 * Get the byte array encoding the bitmap of referenced columns in
	 * a table.
	 *
	 * @param		none
	 * @return		byte array as in a FormatableBitSet encoding column bit map
	 */
	public 	byte[]	getColumnBitMap()
	{
		return columnBitMap;
	}

	/**
	 * Set the byte array encoding the bitmap of referenced columns in
	 * a table.
	 *
	 * @param		byte array as in a FormatableBitSet encoding column bit map
	 * @return		none
	 */
	public	void	setColumnBitMap(byte[] columnBitMap)
	{
		this.columnBitMap = columnBitMap;
	}

	/**
	 * Get a dependable object, which is essentially a table descriptor with
	 * referencedColumnMap field set.
	 *
	 * @param	data dictionary
	 * @param	dependable object ID (table UUID)
	 * @return	a dependable, a table descriptor with referencedColumnMap
	 *			field set
	 */
	protected Dependable getDependable(DataDictionary dd, UUID dependableObjectID)
		throws StandardException
	{
		TableDescriptor td = dd.getTableDescriptor(dependableObjectID);
		if (td != null)  // see beetle 4444
			td.setReferencedColumnMap(new FormatableBitSet(columnBitMap));
		return td;
	}

    //////////////////////////////////////////////////////////////////
    //
    //  FORMATABLE METHODS
    //
    //////////////////////////////////////////////////////////////////

	/**
	 * Read this object from a stream of stored objects.  Just read the
	 * byte array, besides what the parent does.
	 *
	 * @param in read this.
	 */
	public void readExternal( ObjectInput in )
			throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		FormatableHashtable fh = (FormatableHashtable)in.readObject();
		columnBitMap = (byte[])fh.get("columnBitMap");
	}

	/**
	 * Write this object to a stream of stored objects.  Just write the
	 * byte array, besides what the parent does.
	 *
	 * @param out write bytes here.
	 */
	public void writeExternal( ObjectOutput out )
			throws IOException
	{
		super.writeExternal(out);
		FormatableHashtable fh = new FormatableHashtable();
		fh.put("columnBitMap", columnBitMap);
		out.writeObject(fh);
	}
}
