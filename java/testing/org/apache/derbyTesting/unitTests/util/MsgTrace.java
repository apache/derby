/*

   Derby - Class org.apache.derbyTesting.unitTests.util.MsgTrace

   Copyright 1998, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.util;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.iapi.services.stream.InfoStreams;

// static methods
// set up automatically first time it's used
// default trigger is time-bomb, but refer to config for other
//    possibilities
// add timestamps, thread ID's, (stack location info?)

public class MsgTrace implements Runnable {
	//
	// Number of seconds the memory trace waits before
	// dumping its output.
 	public static final String
	DELAY_PARAM_NAME = "derby.memoryTrace.bombDelay";

	public static final String
	RING_BUFFER_SIZE_PARAM_NAME = "derby.memoryTrace.ringBufferSize";

	private static MsgTrace singleton = null;
	long bombDelay; // 30 minutes
	int ringBufferSize;
	// InMemoryTrace recorder;
	HeaderPrintWriter output;

	private MsgTrace() {

		output = Monitor.getMonitor().getSystemStreams().stream();

		bombDelay = PropertyUtil.getSystemInt(DELAY_PARAM_NAME, 30 * 60); // 30 minutes default
		bombDelay *= 1000;

		ringBufferSize = PropertyUtil.getSystemInt(RING_BUFFER_SIZE_PARAM_NAME, 99/*InMemoryTrace.DEFAULT_RING_BUFFER_SIZE*/); 

		// recorder  = new InMemoryTrace(ringBufferSize);

		Thread t = new Thread(this);
		t.setDaemon(true);
		t.start();
	}
	
	public static void traceString(String msg) {
		if (singleton == null)
			singleton = new MsgTrace();
		singleton.trace(msg);
	}

	private void trace(String msg) {
// 		// wrap msg in a Dumpable
// 		d.timestamp = System.currentTimeMillis();
// 		d.threadId = Thread.currentThread().getName();
// 		d.msg = msg;
		// recorder.traceString(msg);
	}

	public void run() {
		try { Thread.sleep(bombDelay); } catch (InterruptedException ie) {}

		// recorder.dump(output);

		System.exit(1);
	}
}
