/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.log
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.catalog.UUID;

/**
  TruncationPoint is used to store truncationLWM.  
  This class is MT-unsafe, caller of this class must provide synchronization
*/
public class TruncationPoint 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	UUID name;
	LogInstant instant;

	public TruncationPoint(UUID name, LogInstant instant)
	{
		this.name = name;
		this.instant = instant;
	}

	public boolean isEqual(UUID name)
	{
		return this.name.equals(name);
	}

	public void setLogInstant(LogInstant instant)
	{
		this.instant = instant;
	}

	public LogInstant getLogInstant()
	{
		return instant;
	}

	public UUID getName()
	{
		return name;
	}

}
