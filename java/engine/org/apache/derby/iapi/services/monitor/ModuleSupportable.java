/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.monitor
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.monitor;

import java.util.Properties;

/**
	Allows a module to check its environment
	before it is selected as an implementation.
*/

public interface ModuleSupportable { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	/**
		See if this implementation can support any attributes that are listed in properties.
		This call may be made on a newly created instance before the
		boot() method has been called, or after the boot method has
		been called for a running module.
		<P>
		The module can check for attributes in the properties to
		see if it can fulfill the required behaviour. E.g. the raw
		store may define an attribute called RawStore.Recoverable.
		If a temporary raw store is required the property RawStore.recoverable=false
		would be added to the properties before calling bootServiceModule. If a
		raw store cannot support this attribute its canSupport method would
		return null. Also see the Monitor class's prologue to see how the
		identifier is used in looking up properties.
		<BR><B>Actually a better way maybe to have properties of the form
		RawStore.Attributes.mandatory=recoverable,smallfootprint and
		RawStore.Attributes.requested=oltp,fast
		</B>

		@return true if this instance can be used, false otherwise.
	*/
	public boolean canSupport(Properties properties);

}
