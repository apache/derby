/*

   Derby - Class org.apache.derbyTesting.functionTests.util.SubSubClass

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

package org.apache.derbyTesting.functionTests.util;

/**
 * This class is for testing whether methods in sub-classes are found.
 */

public class SubSubClass extends SubClass
{

	public static int OVEROVERLOADED_INTSTATIC = 3;

	public SubSubClass(int value)
	{
		super(value);
	}

	public String parmType(Integer value)
	{
		return "java.lang.Integer parameter in SubSubClass";
	}

	public String parmType(Boolean value)
	{
		return "java.lang.Boolean parameter in SubSubClass";
	}

	public static int overloadedStaticMethod()
	{
		return 3;
	}
}
