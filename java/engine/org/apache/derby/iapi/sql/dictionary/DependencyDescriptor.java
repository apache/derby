/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.DependencyDescriptor

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;

/**
 * DependencyDescriptor represents a persistent dependency between
 * SQL objects, such as a TRIGGER being dependent on a TABLE.
 * 
 * A DependencyDescriptor is stored in SYSDEPENDS as four
 * separate columms corresponding to the getters of this class. 
 * 
 * 
 */
public class DependencyDescriptor extends UniqueTupleDescriptor 
{
	/** public interface for this class is:
		<ol>
		<li>public DependableFinder getDependentFinder();</li>
		<li>public UUID getProviderID();</li>
		<li>public DependableFinder getProviderFinder();</li>
		</ol>
	*/

	// implementation
	private final UUID					dependentID;
	private final DependableFinder		dependentBloodhound;
	private final UUID					providerID;
	private final DependableFinder		providerBloodhound;

	/**
	 * Constructor for a DependencyDescriptor
	 *
	 * @param dependent			The Dependent
	 * @param provider			The Provider
	 */

	public DependencyDescriptor(
			Dependent dependent,
			Provider provider
			)
	{
		dependentID = dependent.getObjectID();
		dependentBloodhound = dependent.getDependableFinder();
		providerID = provider.getObjectID();
		providerBloodhound = provider.getDependableFinder();
	}

	/**
	 * Constructor for a DependencyDescriptor
	 *
	 * @param dependentID			The Dependent ID
	 * @param dependentBloodhound	The bloodhound for finding the Dependent
	 * @param providerID			The Provider ID
	 * @param providerBloodhound	The bloodhound for finding the Provider
	 */

	public DependencyDescriptor(
			UUID dependentID, DependableFinder dependentBloodhound,
			UUID providerID, DependableFinder providerBloodhound
			)
	{
		this.dependentID = dependentID;
		this.dependentBloodhound = dependentBloodhound;
		this.providerID = providerID;
		this.providerBloodhound = providerBloodhound;
	}

	// DependencyDescriptor interface

	/**
	 * Get the dependent's ID for the dependency.
	 *
	 * @return 	The dependent's ID.
	 */
	public UUID getUUID()
	{
		return dependentID;
	}

	/**
	 * Get the dependent's type for the dependency.
	 *
	 * @return The dependent's type.
	 */
	public DependableFinder getDependentFinder()
	{
		return dependentBloodhound;
	}

	/**
	 * Get the provider's ID for the dependency.
	 *
	 * @return 	The provider's ID.
	 */
	public UUID getProviderID()
	{
		return providerID;
	}

	/**
	 * Get the provider's type for the dependency.
	 *
	 * @return The provider's type.
	 */
	public DependableFinder getProviderFinder()
	{
		return providerBloodhound;
	}
}
