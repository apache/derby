/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access.conglomerate
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access.conglomerate;

import java.util.Properties;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;

/**

  The interface of all access method factories.  Specific method factories
  (sorts, conglomerates), extend this interface.

**/

public interface MethodFactory extends ModuleSupportable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	Used to identify this interface when finding it with the Monitor.
	**/
	public static final String MODULE = 
	  "org.apache.derby.iapi.store.access.conglomerate.MethodFactory";

	/**
	Return the default properties for this access method.
	**/
	Properties defaultProperties();

	/**
	Return whether this access method implements the implementation
	type given in the argument string.
	**/
	boolean supportsImplementation(String implementationId);

	/**
	Return the primary implementation type for this access method.
	Although an access method may implement more than one implementation
	type, this is the expected one.  The access manager will put the
	primary implementation type in a hash table for fast access.
	**/
	String primaryImplementationType();

	/**
	Return whether this access method supports the format supplied in
	the argument.
	**/
	boolean supportsFormat(UUID formatid);

	/**
	Return the primary format that this access method supports.
	Although an access method may support more than one format, this
	is the usual one.  the access manager will put the primary format
	in a hash table for fast access to the appropriate method.
	**/
	UUID primaryFormat();
}

