/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * This represents a list of column descriptors. 
 */

public class ColumnDescriptorList extends ArrayList
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	 * Add the column.  Currently, the table id is ignored.
	 *
	 * @param tableId the table id (ignored)
	 * @param column the column to add
	 */	
	public void add(UUID tableID, ColumnDescriptor column)
	{
		/*
		** RESOLVE: The interface includes tableID because presumably
		** the primary key for the columns table will be tableID +
		** columnID (or possibly tableID + column name - both column
		** name and ID must be unique within a table).  However, the
		** ColumnDescriptor contains a reference to a tableID, so it
		** seems like we don't need the parameter here.  I am going
		** to leave it here just in case we decide we need it later.
		*/
		add(column);
	}

	/**
	 * Get the column descriptor
	 *
	 * @param tableId the table id (ignored)
	 * @param columnName the column get
	 *
	 * @return the column descriptor if found
	 */	
	public ColumnDescriptor getColumnDescriptor(UUID tableID,
							String columnName)
	{
		ColumnDescriptor	returnValue = null;

		for (Iterator iterator = iterator(); iterator.hasNext(); )
		{
			ColumnDescriptor columnDescriptor = (ColumnDescriptor) iterator.next();

			if ( columnName.equals( columnDescriptor.getColumnName() ) &&
			    tableID.equals( columnDescriptor.getReferencingUUID() ) )
			{
				returnValue = columnDescriptor;
				break;
			}
		}

		return returnValue;
	}

	/**
	 * Get the column descriptor
	 *
	 * @param tableId the table id (ignored)
	 * @param columnId the column id
	 *
	 * @return the column descriptor if found
	 */	
	public ColumnDescriptor getColumnDescriptor(UUID tableID, int columnID)
	{
		ColumnDescriptor	returnValue = null;

		for (Iterator iterator = iterator(); iterator.hasNext(); )
		{
			ColumnDescriptor columnDescriptor = (ColumnDescriptor) iterator.next();
			if ( ( columnID == columnDescriptor.getPosition() ) &&
				tableID.equals( columnDescriptor.getReferencingUUID() ) )
			{
				returnValue = columnDescriptor;
				break;
			}
		}

		return returnValue;
	}

	/**
	 * Return the nth (0-based) element in the list.
	 *
	 * @param n	Which element to return.
	 *
	 * @return The nth element in the list.
	 */
	public ColumnDescriptor elementAt(int n)
	{
		return (ColumnDescriptor) get(n);
	}

	/**
	 * Get an array of strings for all the columns
	 * in this CDL.
	 *
	 * @return the array of strings
	 */
	public String[] getColumnNames()
	{
		String strings[] = new String[size()];

		int size = size();

		for (int index = 0; index < size; index++)
		{
			ColumnDescriptor columnDescriptor = elementAt(index);
			strings[index] = columnDescriptor.getColumnName();
		}
		return strings;
	}
}
