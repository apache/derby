/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.i18n
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

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
