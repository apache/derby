/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;
import java.io.IOException;

/**
 * This is a simple interface that is used by
 * streams that can initialize and reset themselves.
 * The purpose is for the implementation of BLOB/CLOB.
 * It defines a methods that can be used to initialize and reset a stream.
 */
public interface Resetable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	/**
	 *  Reset the stream to the beginning.
	 */
	public void resetStream() throws IOException, StandardException;

	/**
	 *  Initialize. Needs to be called first, before a resetable stream can
     *  be used.
     *
	 */
    public void initStream() throws StandardException;

	/**
	 *  Close. Free resources (such as open containers and locks) associated
     *  with the stream.
	 */
    public void closeStream();

}
