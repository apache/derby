/*

   Derby - Class org.apache.derby.shared.common.i18n.LocaleFinder

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

package org.apache.derby.shared.common.i18n;

import org.apache.derby.shared.common.error.StandardException;

import java.util.Locale;
import java.text.DateFormat;

/**
	A LocaleFinder gets a Locale and things associated with Locales.
 */
public interface LocaleFinder {

	/**
     *
     * @return the current locale
     *
	 * @exception StandardException		Thrown on error
	 */
	Locale getCurrentLocale() throws StandardException;

	/**
	 * Get a formatter for formatting dates. The implementation may cache this
	 * value, since it never changes for a given Locale.
	 *
     * @return the date formatter used for this locale
     * 
	 * @exception StandardException		Thrown on error
	 */
	DateFormat getDateFormat() throws StandardException;

	/**
	 * Get a formatter for formatting times. The implementation may cache this
	 * value, since it never changes for a given Locale.
     *
     * @return the time formatter used for this locale
	 *
	 * @exception StandardException		Thrown on error
	 */
	DateFormat getTimeFormat() throws StandardException;

	
	/**
	 * Get a formatter for formatting timestamps. The implementation may cache
	 * this value, since it never changes for a given Locale.
	 *
     * @return the timestamp formatter used for this locale
     *
	 * @exception StandardException		Thrown on error
	 */
	DateFormat getTimestampFormat() throws StandardException;
}
