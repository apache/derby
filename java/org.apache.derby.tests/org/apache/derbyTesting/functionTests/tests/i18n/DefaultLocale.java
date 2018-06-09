/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.i18n.DefaultLocale

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

package org.apache.derbyTesting.functionTests.tests.i18n;

import java.sql.SQLException;
import org.apache.derbyTesting.junit.LocaleTestSetup;

public class DefaultLocale { 

	static String savedLocale;

	static {
		savedLocale=java.util.Locale.getDefault().toString();
		setDefaultLocale("rr", "TT");
	}


	// used in messageLocale test
	public static void checkDefaultLocale() throws SQLException
	{
		String defLocale = java.util.Locale.getDefault().toString();
		//System.out.println(defLocale);
		if (!defLocale.equals("rr_TT"))
			throw new SQLException("wrong_locale");
	}

	// used in urlLocale test
	public static void checkRDefaultLocale() throws SQLException
	{
		String dbLocale = org.apache.derby.iapi.db.Factory.getDatabaseOfConnection().getLocale().toString();
		//System.out.println(savedLocale);
		//System.out.println(dbLocale);
		if (!savedLocale.equals(dbLocale))
			throw new SQLException("wrong_locale");
	}

	// used in urlLocale test and messageLocale test
	public static void checkDatabaseLocale(String Locale) throws SQLException
	{
		String dbLocale = org.apache.derby.iapi.db.Factory.getDatabaseOfConnection().getLocale().toString();
		//System.out.println(dbLocale + "-");
		//System.out.println(Locale + "-");
		if (!dbLocale.toUpperCase().equals(Locale.toUpperCase().trim()))
			throw new SQLException("wrong locale");
	}

	// used in messageLocale test
	public static void setDefaultLocale(final String Locale, final String Code)
	{
		// needs to run in a privileged block as it will be
		// called through a SQL statement and thus a generated
		// class. The generated class on the stack has no permissions
		// granted to it. Needs write permission on user.language
        LocaleTestSetup.setDefaultLocale(
                new java.util.Locale(Locale.trim(), Code.trim()));
	}

}
