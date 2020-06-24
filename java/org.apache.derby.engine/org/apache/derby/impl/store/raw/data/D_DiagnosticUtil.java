/*

   Derby - Class org.apache.derby.impl.store.raw.data.D_DiagnosticUtil

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

package org.apache.derby.impl.store.raw.data;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.store.raw.RawStoreFactory;


// import com.ibm.db2j.impl.BasicServices.TestService.TestTemplate.T_MultiIterations;
// import com.ibm.db2j.impl.BasicServices.TestService.TestTemplate.T_Fail;
import org.apache.derby.shared.common.reference.Property;

// import java.util.Properties;

// DEBUGGING:

/**

  This class provides some utility functions used to debug on disk structures
  of the store.

**/


public class D_DiagnosticUtil
{


    /* Constructors for This class: */

    /**
     * No arg Constructor.
     **/
    public D_DiagnosticUtil()
    {
    }

    /* Private/Protected methods of This class: */

    /**
     * Given a database name come up with a module.
     * <p>
     *
	 * @return The store module associated with given database name.
     *
     * @param db_name name of the database.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private static Object getModuleFromDbName(String db_name)
		throws StandardException
    {
        Object   store_module = null;

		Object db = findService(Property.DATABASE_MODULE, db_name);
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

        // RESOLVE (mikem) - find a single way to find the current 
        // AccessFactory that works both for ij and unit tests.
        if (db == null)
        {
            // maybe it is a module test - try this hack:
            store_module = findService(AccessFactory.MODULE, db_name);
        }
        else
        {
            // Find the AccessFactory
            store_module = findServiceModule(db, AccessFactory.MODULE);
        }

        return(store_module);
    }

    /* Public Methods of This class: */
    /**
     * Given a Database name and conglomid print out diagnostic info.
     * <p>
     * Print diagnostic information about a particular conglomerate, can be
     * called for either a btree or heap conglomerate.  This routine
     * prints out the string to "System.out"; "ij", depending on it's 
     * configuration, will only print out a fixed length (default 128 bytes),
     * so having ij print the string can be a problem.
     * <p>
     *
     * Can be called from ij to find out info about conglomid 19 in database
     * 'msgdb' by using the following syntax:
     *
       maximumdisplaywidth 9000;

       CREATE FUNCTION D_CONGLOMID_PRINT(DBNAME VARCHAR(128), CONGLOMID INT)
       RETURNS VARCHAR(32000) RETURNS NULL ON NULL INPUT
       EXTERNAL NAME
       'org.apache.derby.impl.store.raw.data.D_DiagnosticUtil.diag_conglomid_print'
       LANGUAGE JAVA PARAMETER STYLE JAVA;

       values D_CONGLOMID_PRINT('msgdb', 19);
          com.ibm.db2j.protocol.BasicServices.Diagnostic.T_Diagnosticable::
          diag_conglomid_print('msgdb', 19);
     *
     * RESOLVE - An interface that takes a table name would be nice.
     *
     * @param db_name   name of the database 
     * @param conglomid conglomerate id of the conglomerate to debug
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static String diag_conglomid_print(String db_name, long conglomid)
        throws StandardException
    {
        try 
        {
            System.out.println(diag_conglomid(db_name, conglomid));
        }
        catch (Throwable t)
        {
            t.printStackTrace();
        }

        return("");
    }
    
    /**
     * Given a Database name and conglomid, return diagnositic string.
     * <p>
     * Return a string with diagnostic information about a particular
     * conglomerate, can be called for any type of conglomerate (some types
     * may not return any info though).
     * <p>
     * Can be called from ij to find out info about conglomid 19 in database
     * 'msgdb' by using the following syntax:
     *
     *     values 
     *     com.ibm.db2j.protocol.BasicServices.Diagnostic.T_Diagnosticable::
     *     diag_conglomid('msgdb', 19);
       maximumdisplaywidth 9000;

       CREATE FUNCTION DIAG_CONGLOMID(DBNAME VARCHAR(128), CONGLOMID INT)
       RETURNS VARCHAR(32000) RETURNS NULL ON NULL INPUT
       EXTERNAL NAME
       'org.apache.derby.impl.store.raw.data.D_DiagnosticUtil.diag_conglomid'
       LANGUAGE JAVA PARAMETER STYLE JAVA;

       values DIAG_CONGLOMID('msgdb', 19);
          com.ibm.db2j.protocol.BasicServices.Diagnostic.T_Diagnosticable::
          diag_conglomid_print('msgdb', 19);
     *
     * RESOLVE - An interface that takes a table name would be nice.
     *
     * @param db_name   name of the database 
     * @param conglomid conglomerate id of the conglomerate to debug
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static String diag_conglomid(String db_name, long conglomid)
        throws StandardException
    {
        String          ret_string   = null;
        AccessFactory   store_module = null;

        store_module = (AccessFactory) getModuleFromDbName(db_name);

        if (store_module != null)
        {

            TransactionController tc =
                store_module.getTransaction(
                    FileContainer.getContextService().getCurrentContextManager());
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

            ConglomerateController open_table = 
                tc.openConglomerate(
                    conglomid, false, 0, TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE);

            open_table.debugConglomerate();

            Diagnosticable diag_obj = DiagnosticUtil.findDiagnostic(open_table);

            ret_string = diag_obj.diag();

            open_table.close();
        }
        else
        {
            System.out.println(
                "Could not find module for database: " + db_name);
        }

        return(ret_string);
    }

    /**
     * Dump raw contents of a page.
     * <p>
     * A utility routine that can be called from an ij session that will 
     * dump the raw contents of a page, in the raw store dump format.
     *
     * @param db_name       name of the database 
     * @param segmentid     segmentid of the table (usually 0)
     * @param containerid   containerid of the table (not conglomid)
     * @param pagenumber    pagenumber of page to dump.
     *
     **/
	public static void diag_dump_page(
    String  db_name, 
    long    segmentid, 
    long    containerid, 
    long    pagenumber)
	{
		Transaction xact = null;
		try
		{
			Object module = getModuleFromDbName(db_name);

			RawStoreFactory store_module = (RawStoreFactory)
				findServiceModule(module, RawStoreFactory.MODULE);
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			xact = store_module.startInternalTransaction
                (FileContainer.getContextService().getCurrentContextManager());

			ContainerKey id = new ContainerKey(segmentid, containerid);
			ContainerHandle container = 
				xact.openContainer(id,
								   ContainerHandle.MODE_READONLY);
			Page page = container.getPage(pagenumber);

			if (page != null)
			{
				System.out.println(page.toString());
				page.unlatch();
			}
			else
			{
				System.out.println("page " + pagenumber + " not found");
			}
			xact.abort();
			xact.close();
			xact = null;
		}
		catch (StandardException se)
		{
			se.printStackTrace();
		}
		finally
		{
			if (xact != null)
			{
				try
				{
					xact.abort();
					xact.close();
				}
				catch (StandardException se)
				{
				}
			}
		}
	}

    /**
     * Given a Database name and conglomid, return container id.
     * <p>
     * Return the containerid of a given conglomerate id.
     * <p>
     * Can be called from ij to find out info about conglomid 19 in database
     * 'msgdb' by using the following syntax:
     *
          values 
          com.ibm.db2j.protocol.BasicServices.Diagnostic.T_Diagnosticable).
          diag_containerid_to_conglomid('msgdb', 924300359390);
     *
     * RESOLVE - An interface that takes a table name would be nice.
     *
     * @param db_name       name of the database 
     * @param containerid   container id of the conglomerate to look up
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static long diag_containerid_to_conglomid(
    String          db_name,
    long            containerid)
        throws StandardException
    {
        // Find the AccessFactory
        Object store_module = getModuleFromDbName(db_name);

        return(diag_containerid_to_conglomid(store_module, containerid));
    }

    public static long diag_containerid_to_conglomid(
    Object   module,
    long            containerid)
    {
        String          ret_string   = null;
        AccessFactory   store_module = null;
        long            conglom_id   = Long.MIN_VALUE;

        // Find the AccessFactory
        store_module = (AccessFactory) 
            getServiceModule(module, AccessFactory.MODULE);

        if (store_module != null)
        {
            try
            {
                TransactionController tc = 
                    store_module.getTransaction(
                        FileContainer.getContextService().getCurrentContextManager());
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

                conglom_id = tc.findConglomid(containerid);
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                // on error just return the initialized bad value conglom_id
            }
        }
        else
        {
            // during access boot this does not exist, assume for now that
            // is why we got here.  RESOLVE - it would be nice if we could
            // actuallly figure that is why we failed.
            
            /*
            System.out.println(
                "Could not find module for module: " + module);
            */
        }

        return(conglom_id);
    }

    /**
     * Given a Database name and containerid, return conglomerate id.
     * <p>
     * Return the conglomerate id of a given conainer id.
     * <p>
     * Can be called from ij to find out info about conglomid 19 in database
     * 'msgdb' by using the following syntax:
     *
          values 
          com.ibm.db2j.protocol.BasicServices.Diagnostic.T_Diagnosticable).
          diag_conglomid_to_containerid('msgdb', 19);
     *
     * RESOLVE - An interface that takes a table name would be nice.
     *
     * @param db_name   name of the database
     * @param conglomid conglomerate id of the conglomerate to debug
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static long diag_conglomid_to_containerid(
    String          db_name,
    long            conglomid)
        throws StandardException
    {
        String          ret_string   = null;
        Object   store_module = null;
        long            conglom_id   = Long.MIN_VALUE;

        // Find the AccessFactory
        store_module = getModuleFromDbName(db_name);

        return(diag_conglomid_to_containerid(store_module, conglomid));
    }

    public static long diag_conglomid_to_containerid(
    Object   module,
    long            conglomid)
    {
        String          ret_string   = null;
        AccessFactory   store_module = null;
        long            container_id = Long.MIN_VALUE;

        // Find the AccessFactory
        store_module = (AccessFactory) 
            getServiceModule(module, AccessFactory.MODULE);
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

        if (store_module != null)
        {
            try
            {
                TransactionController tc =
                    store_module.getTransaction(
                        FileContainer.getContextService().getCurrentContextManager());
//IC see: https://issues.apache.org/jira/browse/DERBY-6648

                container_id = tc.findContainerid(conglomid);
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                // on error just return the initialized bad value conglom_id
            }
        }
        else
        {
            // during access boot this does not exist, assume for now that
            // is why we got here.  RESOLVE - it would be nice if we could
            // actuallly figure that is why we failed.
            
            /*
            System.out.println(
                "Could not find module for module: " + module);
            */
        }

        return(container_id);
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object findServiceModule( final Object serviceModule, final String factoryInterface)
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        throws StandardException
    {
        try {
            return AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object run()
                         throws StandardException
                     {
                         return Monitor.findServiceModule( serviceModule, factoryInterface );
                     }
                 }
                 );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }

    /**
     * Privileged module lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object getServiceModule( final Object serviceModule, final String factoryInterface )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.getServiceModule( serviceModule, factoryInterface );
                 }
             }
             );
    }

    /**
     * Privileged service lookup. Must be private so that user code
     * can't call this entry point.
     */
    private static  Object findService( final String factoryInterface, final String serviceName )
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<Object>()
             {
                 public Object run()
                 {
                     return Monitor.findService( factoryInterface, serviceName );
                 }
             }
             );
    }
    
}
