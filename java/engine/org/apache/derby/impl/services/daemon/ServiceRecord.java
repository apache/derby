/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.daemon
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.daemon;

import org.apache.derby.iapi.services.daemon.Serviceable;

/** wrapper class for basic daemon's clients */
class ServiceRecord
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	// immutable fields
	final Serviceable client;	
	private final boolean onDemandOnly;
	final boolean subscriber;

	// we can tolerate spurrious service, so don't synchronized this
	private boolean serviceRequest;

	ServiceRecord(Serviceable client, boolean onDemandOnly, boolean subscriber)
	{
		this.client = client;
		this.onDemandOnly = onDemandOnly;
		this.subscriber = subscriber;
	}

	final void serviced()
	{
		serviceRequest = false;
	}

	final boolean needImmediateService()
	{
		return serviceRequest;
	}

	final boolean needService()
	{
		return serviceRequest || !onDemandOnly;
	}


	final void called()
	{
		serviceRequest = true;
	}
}



