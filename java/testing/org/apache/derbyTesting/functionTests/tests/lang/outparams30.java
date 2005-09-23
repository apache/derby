/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.outparams30

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.math.BigDecimal;

/**
 * outparams30 contains java procedures using java.math.BigDecimal.
 * These are moved to this class to enable tests using other procedures
 * in outparams.java to run in J2ME/CDC/FP.
 *   
 * @author deepa
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Style - Code Templates
 */
public class outparams30 extends outparams {
	
	public static void takesBigDecimal(BigDecimal[] outparam, int type)
	{
		outparam[0] = (outparam[0] == null ? new BigDecimal("33") : outparam[0].add(outparam[0]));
		outparam[0].setScale(4, BigDecimal.ROUND_DOWN);
	}
	
	public static BigDecimal returnsBigDecimal(int type)
	{
		return new BigDecimal(666d);
	}

}
