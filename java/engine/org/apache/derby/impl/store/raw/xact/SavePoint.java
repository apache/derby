/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.xact
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.services.sanity.SanityManager;

class SavePoint
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/*
	** Fields
	*/

	private LogInstant savePoint;
	private final String name;
	//kindOfSavepoint can have 3 possible values.
	//A NULL value means it is an internal savepoint (ie not a user defined savepoint)
	//Non NULL value means it is a user defined savepoint which can be a SQL savepoint or a JDBC savepoint
	//     A String value for kindOfSavepoint would mean it is SQL savepoint
	//     A JDBC Savepoint object value for kindOfSavepoint would mean it is JDBC savepoint
	private Object kindOfSavepoint;

	/*
	** Constructor
	*/

	SavePoint(String name, Object kindOfSavepoint) {
		super();
		this.name = name;
		this.kindOfSavepoint = kindOfSavepoint;
	}


	void setSavePoint(LogInstant savePoint) {
        if (SanityManager.DEBUG)
        {
    		SanityManager.ASSERT((savePoint == null) || (this.savePoint == null));
    	}

		this.savePoint = savePoint;
	}

	LogInstant getSavePoint() {
		return savePoint;
	}

	String getName() {
		return name;
	}

	boolean isThisUserDefinedsavepoint() {
		return (kindOfSavepoint != null ? true : false);
	}

	Object getKindOfSavepoint() {
		return kindOfSavepoint;
	}

}
