/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.xact
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.util.Matchable;

public final class EscalateContainerKey implements Matchable
{
    /**
       IBM Copyright &copy notice.
    */
    public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    private ContainerKey container_key;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public EscalateContainerKey(ContainerKey key)
    {
        container_key = key;
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
	public boolean match(Object key) 
    {
		if (key instanceof RecordHandle) 
        {
			return(container_key.equals(((RecordHandle) key).getContainerId()));
		}

		return false;
	}
}
