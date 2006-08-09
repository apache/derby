/*

   Derby - Class org.apache.derby.iapi.sql.depend.ProviderList

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

package org.apache.derby.iapi.sql.depend;

import java.util.Hashtable;

/**
 * ProviderList is a list of Providers that is being
 * tracked for some object other than the current dependent.
 */

public class ProviderList extends Hashtable
{
	/**
	 * Add a Provider to the list.
	 *
	 * @param prov	The Provider to add to the list.
	 */
	public void addProvider(Provider prov)
	{
		put (prov.getObjectID(), prov);
	}
}
