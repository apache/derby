/*

   Derby - Class org.apache.derby.iapi.types.Resetable

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
