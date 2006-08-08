/*

	Derby - Class org.apache.derby.ui.util.Logger
	
	Licensed to the Apache Software Foundation (ASF) under one or more
	contributor license agreements.  See the NOTICE file distributed with
	this work for additional information regarding copyright ownership.
	The ASF licenses this file to you under the Apache License, Version 2.0
	(the "License"); you may not use this file except in compliance with
	the License.  You may obtain a copy of the License at
	
	   http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.

*/

package org.apache.derby.ui.util;

import org.apache.derby.ui.DerbyPlugin;
import org.eclipse.core.runtime.ILog;
import org.eclipse.core.runtime.Status;


public class Logger {
	static public void log(String msg, int msgType) {
		ILog log = DerbyPlugin.getDefault().getLog();
		Status status = new Status(msgType, DerbyPlugin.getDefault().getBundle().getSymbolicName(), msgType, msg + "\n", null);
		log.log(status);
		
	}
}
