/*

Derby - org.apache.derbyTesting.functionTests.tests.upgradeTests.Upgrade_10_1_10_2

Copyright 1999, 2006 The Apache Software Foundation or its licensors, as applicable.

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.net.MalformedURLException;
import java.sql.SQLException;

/**
 * Test upgrade from 10.1 to 10.2 
 */
public class Upgrade_10_1_10_2 {

	public static void main(String[] args) {
		
		// This test will use jar locations provided in command-line input till 
		// the 10.1 jars are available in the repository (DERBY-1049)
		if(args.length != 2) {
			System.out.println("USAGE: java UpgradeTester_10_1_10_2 <location of old jars> <location of new jars>");
			System.out.println("e.g: java UpgradeTester C:\\derby\\10.1\\lib C:\\derby\\trunk\\lib");
			return;
		}
		
		String oldJarLoc = args[0];
		String newJarLoc = args[1];
		
		int oldMajorVersion = 10;
		int oldMinorVersion = 1;
		int newMajorVersion = 10;
		int newMinorVersion = 2;
		boolean allowPreReleaseUpgrade = true;
		
		try {
			UpgradeTester upgradeTester = new UpgradeTester(oldJarLoc, newJarLoc,
												oldMajorVersion, oldMinorVersion,
												newMajorVersion, newMinorVersion,
												allowPreReleaseUpgrade);
			upgradeTester.runUpgradeTests();
		} catch(MalformedURLException mue) {
			System.out.println("MalformedURLException: " + mue.getMessage());
			mue.printStackTrace();
		} catch (SQLException sqle) {
			System.out.println("SQLException:");
			UpgradeTester.dumpSQLExceptions(sqle);
		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}
}
