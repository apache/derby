/*

   Derby - Class org.apache.derby.iapi.services.i18n.LocaleFinder

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.services.i18n;

import org.apache.derby.iapi.error.StandardException;

import java.util.Locale;
import java.text.DateFormat;
import java.text.RuleBasedCollator;

/**
	A LocaleFinder gets a Locale and things associated with Locales.
 */
public interface LocaleFinder {

	/**
	 * @exception StandardException		Thrown on error
	 */
	Locale getCurrentLocale() throws StandardException;

	/**
	 * Get a RuleBasedCollator corresponding to the Locale returned by
	 * getCurrentLocale().
	 *
	 * @exception StandardException		Thrown on error
	 */
	RuleBasedCollator getCollator() throws StandardException;

	/**
	 * Get a formatter for formatting dates. The implementation may cache this
	 * value, since it never changes for a given Locale.
	 *
	 * @exception StandardException		Thrown on error
	 */
	DateFormat getDateFormat() throws StandardException;

	/**
	 * Get a formatter for formatting times. The implementation may cache this
	 * value, since it never changes for a given Locale.
	 *
	 * @exception StandardException		Thrown on error
	 */
	DateFormat getTimeFormat() throws StandardException;

	
	/**
	 * Get a formatter for formatting timestamps. The implementation may cache
	 * this value, since it never changes for a given Locale.
	 *
	 * @exception StandardException		Thrown on error
	 */
	DateFormat getTimestampFormat() throws StandardException;
}
