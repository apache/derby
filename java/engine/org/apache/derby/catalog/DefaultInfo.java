/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.catalog
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.catalog;

/**
	

 <p>An interface for describing a default for a column or parameter in Cloudscape systems.
 */
public interface DefaultInfo
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	 * Get the text of a default.
	 *
	 * @return The text of the default.
	 */
	public String getDefaultText();
}
