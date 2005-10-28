/*

   Derby - Class org.apache.derby.iapi.store.access.ConglomPropertyQueryable

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException; 

import java.util.Properties;

/**

ConglomPropertyable provides the interfaces to read properties from a 
conglomerate.
<p>
RESOLVE - If language ever wants these interfaces on a ScanController it 
          should not be too difficult to add them.

@see ConglomerateController

**/

public interface ConglomPropertyQueryable
{
    /**
     * Request the system properties associated with a table. 
     * <p>
     * Request the value of properties that are associated with a table.  The
     * following properties can be requested:
     *     derby.storage.pageSize 
     *     derby.storage.pageReservedSpace
     *     derby.storage.minimumRecordSize
     *     derby.storage.initialPages
     * <p>
     * To get the value of a particular property add it to the property list,
     * and on return the value of the property will be set to it's current 
     * value.  For example:
     *
     * get_prop(ConglomerateController cc)
     * {
     *     Properties prop = new Properties();
     *     prop.put("derby.storage.pageSize", "");
     *     cc.getTableProperties(prop);
     *
     *     System.out.println(
     *         "table's page size = " + 
     *         prop.getProperty("derby.storage.pageSize");
     * }
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    void getTableProperties(Properties prop)
		throws StandardException;

    /**
     * Request set of properties associated with a table. 
     * <p>
     * Returns a property object containing all properties that the store
     * knows about, which are stored persistently by the store.  This set
     * of properties may vary from implementation to implementation of the
     * store.
     * <p>
     * This call is meant to be used only for internal query of the properties
     * by jbms, for instance by language during bulk insert so that it can
     * create a new conglomerate which exactly matches the properties that
     * the original container was created with.  This call should not be used
     * by the user interface to present properties to users as it may contain
     * properties that are meant to be internal to jbms.  Some properties are 
     * meant only to be specified by jbms code and not by users on the command
     * line.
     * <p>
     * Note that not all properties passed into createConglomerate() are stored
     * persistently, and that set may vary by store implementation.
     *
     * @param prop   Property list to add properties to.  If null, routine will
     *               create a new Properties object, fill it in and return it.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    Properties getInternalTablePropertySet(Properties prop)
		throws StandardException;
}
