/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

/**
 * Static final trigger events.  One for
 * each known trigger event.  Use these rather
 * than constructing a new TriggerEvent.
 *
 * @author jamie
 */
public class TriggerEvents
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	public static final TriggerEvent BEFORE_INSERT = new TriggerEvent(TriggerEvent.BEFORE_INSERT);
	public static final TriggerEvent BEFORE_DELETE = new TriggerEvent(TriggerEvent.BEFORE_DELETE);
	public static final TriggerEvent BEFORE_UPDATE = new TriggerEvent(TriggerEvent.BEFORE_UPDATE);
	public static final TriggerEvent AFTER_INSERT = new TriggerEvent(TriggerEvent.AFTER_INSERT);
	public static final TriggerEvent AFTER_DELETE = new TriggerEvent(TriggerEvent.AFTER_DELETE);
	public static final TriggerEvent AFTER_UPDATE = new TriggerEvent(TriggerEvent.AFTER_UPDATE);
}
