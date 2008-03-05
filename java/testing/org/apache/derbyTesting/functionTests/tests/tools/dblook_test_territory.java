/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.dblook_test_territory

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.io.File;

public class dblook_test_territory extends dblook_test {

	/* **********************************************
	 * main:
	 ****/

	public static void main (String[] args) {

		territoryBased = ";territory=nl_NL;collation=TERRITORY_BASED";
		testDirectory = "territory_" + testDirectory;
		expectedCollation = "TERRITORY_BASED";
		separator = System.getProperty("file.separator");
		new dblook_test_territory().doTest();
		System.out.println("\n[ Done. ]\n");
		renameDbLookLog("dblook_test_territory");

	}

}
