/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.stream
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.stream;

/**
 *
 * The Basic Services provide InfoStreams for reporting
 * information.
 * <p>
 * When creating a message for a stream,
 * you can create an initial entry with header information
 * and then append to it as many times as desired.
 * <p>
 * 
 * @see HeaderPrintWriter
 * @author ames
 */
public interface InfoStreams { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	/**
	 Return the default stream. If the default stream could not be set up as requested then
	 it points indirectly to System.err.
	 * 
	 * @return the default stream.
	 */
	HeaderPrintWriter stream();
}
