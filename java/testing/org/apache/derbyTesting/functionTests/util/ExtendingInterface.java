/*

   Derby - Class org.apache.derbyTesting.functionTests.util.ExtendingInterface

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util;

public interface ExtendingInterface extends Runnable {

	public static int INTERFACE_FIELD = 7541;

	/**
		Force an ambigutity with Object's wait.
	*/
	public void wait(int a, long b);

	public Object eimethod(Object a);
}
