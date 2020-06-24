/*

   Derby - Class org.apache.derby.iapi.services.io.TypedFormat

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

package org.apache.derby.iapi.services.io;
/**
//IC see: https://issues.apache.org/jira/browse/DERBY-2400
  Derby interface for identifying the format id for the
  stored form of an object. Objects of different classes may
  have the same format id if:

  <UL>
  <LI> The objects read and write the same stored forms.
  <LI> The object's getTypeId() method returns the same
  identifier.
  <LI> The objects support all the interfaces the type
  implies.
  </UL>
  */
public interface TypedFormat
{
	/**
	  Get a universally unique identifier for the type of
	  this object. 

	  @return The identifier. (A UUID stuffed in an array
	  of 16 bytes).
	 */	
	int getTypeFormatId();
}
