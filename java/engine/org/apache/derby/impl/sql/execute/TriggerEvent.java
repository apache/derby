/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

/**
 * This is a simple class that we use to track
 * trigger events.  This is not expected to
 * be used directly, instead there is a static
 * TriggerEvent in TriggerEvents for each event 
 * found in this file.
 * 
 * @author jamie
 */
public class TriggerEvent
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	static final int BEFORE_INSERT = 0;	
	static final int BEFORE_DELETE = 1;	
	static final int BEFORE_UPDATE = 2;	
	static final int LAST_BEFORE_EVENT = BEFORE_UPDATE;	
	static final int AFTER_INSERT = 3;	
	static final int AFTER_DELETE = 4;	
	static final int AFTER_UPDATE = 5;	
	static final int MAX_EVENTS = 6;

	private static final String Names[] = {	"BEFORE INSERT",
											"BEFORE DELETE", 
											"BEFORE UPDATE", 
											"AFTER INSERT", 
											"AFTER DELETE", 
											"AFTER UPDATE"
										};

	private boolean before;
	private int type;

	/**
	 * Create a trigger event of the given type
	 *
 	 * @param type the type
	 */
	TriggerEvent(int type)
	{
		this.type = type;
		switch(type)
		{
			case BEFORE_INSERT:		
			case BEFORE_DELETE:		
			case BEFORE_UPDATE:		
				before = true;
				break;

			case AFTER_INSERT:		
			case AFTER_DELETE:		
			case AFTER_UPDATE:		
				before = false;
				break;
		}
	}

	/**
	 * Get the type number of this trigger
 	 *
 	 * @return the type number
	 */
	int getNumber()
	{
		return type;
	}

	/**
	 * Get the type number of this trigger
 	 *
 	 * @return the type number
	 */
	String getName()
	{
		return Names[type];
	}

	/**
	 * Is this a before trigger
 	 *
 	 * @return true if before
	 */
	boolean isBefore()
	{
		return before;
	}

	/**
	 * Is this an after trigger
 	 *
 	 * @return true if after
	 */
	boolean isAfter()
	{
		return !before;
	}
}	
