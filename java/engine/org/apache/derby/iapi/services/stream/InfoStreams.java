/*

   Derby - Class org.apache.derby.iapi.services.stream.InfoStreams

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
	 Return the default stream. If the default stream could not be set up as requested then
	 it points indirectly to System.err.
	 * 
	 * @return the default stream.
	 */
	HeaderPrintWriter stream();
}
