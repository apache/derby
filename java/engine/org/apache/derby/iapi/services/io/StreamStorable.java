/*

   Derby - Class org.apache.derby.iapi.services.io.StreamStorable

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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
