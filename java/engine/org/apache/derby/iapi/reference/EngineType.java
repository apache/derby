/*

   Derby - Class org.apache.derby.iapi.reference.EngineType

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.reference;


public interface EngineType
{

	// Cloudscape engine types

	int			STANDALONE_DB			=	0x00000002;	
	int         STORELESS_ENGINE        =   0x00000080;

	int NONE = STANDALONE_DB;

	String PROPERTY = "derby.engineType";

}
