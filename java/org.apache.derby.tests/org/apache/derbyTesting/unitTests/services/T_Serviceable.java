/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_Serviceable

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.unitTests.services;

import org.apache.derbyTesting.unitTests.harness.T_Generic;
import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.daemon.*;

/**
	This test implements serviceable for testing.  To facility testing, when
	this object is being serviced, it will synchronize on itself and notity all
	waiters.  Test driver may wait on this object and check timesServiced to
	make sure the background daemon has run.
*/
public class T_Serviceable implements Serviceable
{
	// synchronized on this to look at the number 
	// of times this object has been serviced
	protected int timesServiced;

	// constant for checking
	protected final int timesRequeue;
	protected final boolean onDemandOnly;
	protected final boolean subscribed;

	// use this to unsubscribe
	protected int clientNumber;

	// test enqueueing, t = number of times to requeue
	public T_Serviceable(int t)
	{
		timesServiced = 0;
		timesRequeue = t;
		onDemandOnly = false;	// not looked at
		subscribed = false;
		clientNumber = -1;
	}

	// test subscription 
	public T_Serviceable(boolean onDemandOnly)
	{
		timesServiced = 0;
		timesRequeue = 0;		// not looked at
		this.onDemandOnly = onDemandOnly;
		subscribed = true;
	}

	protected void setClientNumber(int n)
	{
		clientNumber = n;
	}

	protected int getClientNumber()
	{
		return clientNumber;
	}

	/*
	 *  Serviceable interface
	 */
	public synchronized int performWork(ContextManager context) 
	{
		context.toString();	// make sure context manager is not null;

		timesServiced++;
		notifyAll();			// notify anyone waiting for me to be serviced

		if (!subscribed && timesRequeue > timesServiced)
			return Serviceable.REQUEUE;
		else
			return Serviceable.DONE;
	}
	
	public boolean serviceASAP()
	{
		return true;
	}


	// @return true, if this work needs to be done on a user thread immediately
	public boolean serviceImmediately()
	{
		return false;
	}	


	/*
	 * test utilities
	 */

	protected synchronized void t_wait(int n)
	{
		try
		{
			while (timesServiced < n)
				wait();
		}
		catch (InterruptedException ie) {}
	}

	protected synchronized void t_check(int n) throws T_Fail
	{
		if (timesServiced != n)
			throw T_Fail.testFailMsg("Expect to be serviced " + n + " times, instead serviced " + timesServiced);
	}

}
