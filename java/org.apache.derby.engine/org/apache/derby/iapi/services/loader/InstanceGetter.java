/*

   Derby - Class org.apache.derby.iapi.services.loader.InstanceGetter

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

package org.apache.derby.iapi.services.loader;

import java.lang.reflect.InvocationTargetException;

public interface InstanceGetter {

	/**
		Create an instance of a class.

        @return a new instance of the class

		@exception InstantiationException Zero arg constructor can not be executed
		@exception IllegalAccessException Class or zero arg constructor is not public.
		@exception InvocationTargetException Exception throw in zero-arg constructor.
        @exception NoSuchMethodException Missing method exception

	*/
	public Object getNewInstance()
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
		throws InstantiationException,
               IllegalAccessException,
               InvocationTargetException,
               NoSuchMethodException;
}
