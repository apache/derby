/*

   Derby - Class org.apache.derby.catalog.DefaultInfo

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.catalog;

/**
	

 <p>An interface for describing a default for a column or parameter in Cloudscape systems.
 */
public interface DefaultInfo
{
	/**
	 * Get the text of a default.
	 *
	 * @return The text of the default.
	 */
	public String getDefaultText();
}
