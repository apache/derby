/*

   Derby - Class org.apache.derbyTesting.functionTests.util.SubInterfaceClass

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

public class SubInterfaceClass extends ManyMethods
	implements NoMethodInterface, ExtendingInterface {

	public SubInterfaceClass(int value) {
		super(value);
	}

	/*
	** Methods of Runnable (from ExtendingInterface)
	*/

	public void run() {
	}

	/*
	** Methods of ExtendingInterface
	*/

	public void wait(int a, long b) {}

	public Object eimethod(Object a) {
		return a;
	}
}
