/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;
import org.apache.derby.iapi.error.StandardException;
import java.io.InputStream;

/**
  Formatable for holding SQL data (which may be null).
  It supports streaming columns.

  @see Formatable
 */
public interface StreamStorable
{
	/**
	  Return the stream state of the object.
	  
	**/
	public InputStream returnStream();

	/**
	  sets the stream state for the object.
	**/
	public void setStream(InputStream newStream);

	/**
	  sets the stream state for the object.
	
		@exception StandardException on error
	**/
	public void loadStream() throws StandardException;
}
