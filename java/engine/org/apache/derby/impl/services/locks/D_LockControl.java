/*

   Derby - Class org.apache.derby.impl.services.locks.D_LockControl

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.services.locks;

import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.locks.Lockable;

import java.util.Properties;
import java.util.List;
import java.util.Iterator;

/**
**/

public class D_LockControl implements Diagnosticable
{
    protected LockControl control;

    public D_LockControl()
    {
    }

    /* Private/Protected methods of This class: */

	/*
	** Methods of Diagnosticable
	*/
    public void init(Object obj)
    {
        control = (LockControl) obj;
    }

    /**
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public String diag()
        throws StandardException
    {
		StringBuffer sb = new StringBuffer(1024);

		sb.append("LockControl:\n  granted list: ");

        int i = 0;

		Object firstGrant = control.getFirstGrant();
		if (firstGrant != null) {
				sb.append("\n    g[" + i + "]:" + DiagnosticUtil.toDiagString(firstGrant));
				i++;
			}

		List granted = control.getGranted();
		
		if (granted != null) {
			for (Iterator dli = granted.iterator(); dli.hasNext(); )
			{
				sb.append("\n    g[" + i + "]:" + DiagnosticUtil.toDiagString(dli.next()));
				i++;
			}
		}


        sb.append("\n  waiting list:");

		List waiting = control.getWaiting();

        int num_waiting = 0;

        if (waiting != null)
        {
			for (Iterator dli = waiting.iterator(); dli.hasNext(); )
            {
                sb.append(
                    "\n    w[" + num_waiting + "]:" + 
                    DiagnosticUtil.toDiagString(dli.next()));

                num_waiting++;
            }
        }

        if (num_waiting == 0)
            sb.append("    no waiting locks.");
 
		return sb.toString();
    }
	public void diag_detail(Properties prop) {}

	/*
	** Static routines that were in SinglePool
	*/

	
	/*
	** Debugging routines
	*/

	static void debugLock(String type, Object compatabilitySpace, Object group, Lockable ref, Object qualifier, int timeout) {

		if (SanityManager.DEBUG) {

			SanityManager.DEBUG(Constants.LOCK_TRACE, type +
                debugLockString(
                    compatabilitySpace, group, ref, qualifier, timeout));
		}
	}
	static void debugLock(String type, Object compatabilitySpace, Object group) {

		if (SanityManager.DEBUG) {

			SanityManager.DEBUG(Constants.LOCK_TRACE, type +
					debugLockString(compatabilitySpace, group));
		}
	}
	static void debugLock(String type, Object compatabilitySpace, Object group, Lockable ref) {

		if (SanityManager.DEBUG) {

			SanityManager.DEBUG(Constants.LOCK_TRACE, type +
					debugLockString(compatabilitySpace, group, ref));
		}
	}


	static String debugLockString(Object compatabilitySpace, Object group) {

		if (SanityManager.DEBUG) {

			StringBuffer sb = new StringBuffer("");

			debugAppendObject(sb, " CompatabilitySpace=", compatabilitySpace);
			debugAppendObject(sb, " Group=", group);

			debugAddThreadInfo(sb);

			return sb.toString();

		} else {
			return null;
		}
	}

	static String debugLockString(Object compatabilitySpace, Object group, Lockable ref) {

		if (SanityManager.DEBUG) {

			StringBuffer sb = new StringBuffer("");

			debugAppendObject(sb, " Lockable ", ref);
			debugAppendObject(sb, " CompatabilitySpace=", compatabilitySpace);
			debugAppendObject(sb, " Group=", group);

			debugAddThreadInfo(sb);

			return sb.toString();

		} else {
			return null;
		}
	}


	static String debugLockString(Object compatabilitySpace, Object group, Lockable ref, Object qualifier, int timeout) {

		if (SanityManager.DEBUG) {

			StringBuffer sb = new StringBuffer("");

			debugAppendObject(sb, " Lockable ", ref);
			debugAppendObject(sb, " Qualifier=", qualifier);
			debugAppendObject(sb, " CompatabilitySpace=", compatabilitySpace);
			debugAppendObject(sb, " Group=", group);

			if (timeout >= 0) {
				sb.append(" Timeout(ms)=");
				sb.append(timeout);
			}

			debugAddThreadInfo(sb);


			return sb.toString();

		} else {
			return null;
		}
	}

	static void debugAddThreadInfo(StringBuffer sb) {

		if (SanityManager.DEBUG) {
			if (SanityManager.DEBUG_ON(Constants.LOCK_TRACE_ADD_THREAD_INFO)) {
				debugAppendObject(sb, " Thread=", Thread.currentThread());
			}
		}
	}

	static void debugAppendObject(StringBuffer sb, String desc, Object item) {
		if (SanityManager.DEBUG) {

			sb.append(desc);

			if (item != null)
				sb.append(item.toString());
			else
				sb.append("<null>");
		}
	}
}

