/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.monitor
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.monitor;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.error.ExceptionSeverity;

/**
	A context that is used during a service boot to
	stop cleanup on the stack at this point.
*/
class ServiceBootContext extends ContextImpl {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	ServiceBootContext(ContextManager cm) {
		super(cm, "ServiceBoot");
	}

	public void cleanupOnError(Throwable t) {
		popMe();
	}

	public boolean isLastHandler(int severity)
	{
		return (severity == ExceptionSeverity.NO_APPLICABLE_SEVERITY) ||
			   (severity == ExceptionSeverity.DATABASE_SEVERITY) ||
			   (severity == ExceptionSeverity.SYSTEM_SEVERITY);
	}
}
