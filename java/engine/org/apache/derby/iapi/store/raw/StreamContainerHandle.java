/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Properties;

/**
	A Stream Container handle
*/

public interface StreamContainerHandle {
	/**
		IBM Copyright &copy notice.
	*/
 

    public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	public static final int TEMPORARY_SEGMENT = -1;

	/**
		Return my identifier.
	*/
	public ContainerKey getId();

    /**
     * Request the system properties associated with a container. 
     * <p>
     * Request the value of properties that are associated with a stream table.
	 * The following properties can be requested:
     *     derby.storage.streamFileBufferSize 
     * <p>
     * To get the value of a particular property add it to the property list,
     * and on return the value of the property will be set to it's current 
     * value.  For example:
     *
     * get_prop(ConglomerateController cc)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.streamFileBufferSize", "");
     *     cc.getTableProperties(prop);
     *
     *     System.out.println(
     *         "table's buffer size = " + 
     *         prop.getProperty("derby.storage.streamFileBufferSize");
     * }
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    void getContainerProperties(Properties prop)
		throws StandardException;

	/**
		Fetch the next record.
		Fills in the Storable columns within the passed in row if
		row is not null, otherwise the record is not fetched.
		If the row.length is less than the number of fields in the row,
		then, will fill the row, and ignore the rest of the row.
		<BR>
		When no more row is found, then false is returned.

		<P>
		<B>Locking Policy</B>
		<BR>
		No locks.

		@param row Row to be filled in with information from the record.

		@exception StandardException	Standard Cloudscape error policy
	*/
	boolean fetchNext(DataValueDescriptor[] row) throws StandardException;

	/**
		Close me. After using this method the caller must throw away the
		reference to the Container object, e.g.
		<PRE>
			ref.close();
			ref = null;
		</PRE>
		<BR>
		The container will be closed automatically at the commit or abort
		of the transaction if this method is not called explictly.
	*/
	public void close();

	/**
		remove the stream container

		@exception StandardException Standard Cloudscape error policy		
	 */
	public void removeContainer() throws StandardException;
}
