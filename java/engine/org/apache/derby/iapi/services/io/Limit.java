/*

   Derby - Class org.apache.derby.iapi.services.io.Limit

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

import java.io.IOException;

/**
	Methods that allow limits to be placed on an input or output stream to
	avoid clients reading or writing too much information.
*/
public interface Limit {

	/**
		Set the limit of the data that can be read or written. After this
		call up to and including length bytes can be read from or skipped in
		the stream.
		
		<P> On input classes (e.g. InputStreams) any attempt to read or skip
		beyond the limit will result in an end of file indication
		(e.g. read() methods returning -1 or throwing EOFException).

		<P> On output classes (e.g. OutputStream) any attempt to write
		more beyond the limit will result in an EOFException

		@exception IOException IOException from some underlying stream
		@exception EOFException The set limit would exceed
		the available data in the stream.
	*/
	public void setLimit(int length)
		throws IOException;

	/**
		Clear any limit set by setLimit. After this call no limit checking
		will be made on any read until a setLimit()) call is made.

		@return the number of bytes within the limit that have not been read or written.
	*/
	public int clearLimit();
}
