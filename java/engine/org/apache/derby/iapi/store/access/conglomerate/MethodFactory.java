/*

   Derby - Class org.apache.derby.iapi.store.access.conglomerate.MethodFactory

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

