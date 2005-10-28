/*

   Derby - Class org.apache.derby.impl.services.daemon.ServiceRecord

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.services.daemon;

import org.apache.derby.iapi.services.daemon.Serviceable;

/** wrapper class for basic daemon's clients */
class ServiceRecord
{
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



