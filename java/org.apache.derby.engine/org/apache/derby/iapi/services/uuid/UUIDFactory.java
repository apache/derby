/*

   Derby - Class org.apache.derby.iapi.services.uuid.UUIDFactory

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

package org.apache.derby.iapi.services.uuid;

import org.apache.derby.catalog.UUID;

/*
	Internal comment (not for user documentation):
  Although this is an abstract interface, I believe that the
  underlying implementation of UUID will have to be DCE UUID.
  This is because the string versions of UUIDs get stored in
  the source code.  In other words, no matter what implementation
  is used for UUIDs, strings that look like this
  <blockquote><pre>
	E4900B90-DA0E-11d0-BAFE-0060973F0942
  </blockquote></pre>
  will always have to be turned into universally unique objects
  by the recreateUUID method
 */
/**

//IC see: https://issues.apache.org/jira/browse/DERBY-6945
  <p>
  Generates and recreates unique identifiers.
  </p>
  
  <p>
  An example of such an identifier is:
  </p>

  <blockquote><pre>
	E4900B90-DA0E-11d0-BAFE-0060973F0942
  </pre></blockquote>
  
  <p>
  These resemble DCE UUIDs, but use a different implementation.
  </p>
  <p>
  The string format is designed to be the same as the string
  format produced by Microsoft's UUIDGEN program, although at
  present the bit fields are probably not the same.
  </p>
  
 **/
public interface UUIDFactory 
{
	/**
	  Create a new UUID.  The resulting object is guaranteed
	  to be unique "across space and time".
	  @return		The UUID.
	**/
 	public UUID createUUID();

	/**
	  Recreate a UUID from a string produced by UUID.toString.

      @param uuidstring       A UUID as a string
	  @return		The UUID.
	**/
	public UUID recreateUUID(String uuidstring);
}

