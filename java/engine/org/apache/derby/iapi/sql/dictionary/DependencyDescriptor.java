/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.dictionary
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.Dependable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.Provider;

/**
 * This interface is used to get information from a DependencyDescriptor.
 *
 * @version 0.1
 * @author Jerry Brenner
 */

public class DependencyDescriptor extends TupleDescriptor 
	implements UniqueTupleDescriptor
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/** public interface for this class is:
		<ol>
		<li>public DependableFinder getDependentFinder();</li>
		<li>public UUID getProviderID();</li>
		<li>public DependableFinder getProviderFinder();</li>
		</ol>
	*/

	// implementation
	private UUID					dependentID;
	private DependableFinder		dependentBloodhound;
	private UUID					providerID;
	private DependableFinder		providerBloodhound;

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
