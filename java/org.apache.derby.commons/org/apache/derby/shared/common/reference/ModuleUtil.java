/*

   Derby - Class org.apache.derby.shared.common.reference.ModuleUtil

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

package org.apache.derby.shared.common.reference;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.ServiceLoader;

import org.apache.derby.shared.api.DerbyModuleAPI;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.info.JVMInfo;

/**
 * Utility methods for handling the components introduced
 * by the Java Platform Module System (project Jigsaw)
 * in Java 9.
 */
public class ModuleUtil
{
    /////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    /////////////////////////////////////////////////////////

    // module names
    public static final String ENGINE_MODULE_NAME = "org.apache.derby.engine";
    public static final String CLIENT_MODULE_NAME = "org.apache.derby.client";
    public static final String SERVER_MODULE_NAME = "org.apache.derby.server";
    public static final String RUNNER_MODULE_NAME = "org.apache.derby.runner";
    public static final String SHARED_MODULE_NAME = "org.apache.derby.commons";
    public static final String TOOLS_MODULE_NAME = "org.apache.derby.tools";
    public static final String OPTIONALTOOLS_MODULE_NAME = "org.apache.derby.optionaltools";
    public static final String TESTING_MODULE_NAME = "org.apache.derby.tests";
    public static final String LOCALE_MODULE_NAME_PREFIX = "org.apache.derby.locale_";

    /////////////////////////////////////////////////////////
    //
    // STATE
    //
    /////////////////////////////////////////////////////////

    /** Map of module names to Derby modules */
    private static HashMap<String,java.lang.Module> _derbyModules;

    /////////////////////////////////////////////////////////
    //
    // ENTRY POINTS
    //
    /////////////////////////////////////////////////////////

    /**
     * Return the Derby module with the given name.
     *
     * @param moduleName The name of the module
     *
     * @return the corresponding module, or null if none is found
     */
    public static java.lang.Module derbyModule(String moduleName)
    {
        if (!JVMInfo.isModuleAware()) { return null; }
        
        initModuleInfo();  // find all of the derby modules
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        return _derbyModules.get(moduleName);
    }

    /**
     * Get the name of the module containing the localized
     * messages for the given locale.
     *
     * @param localeString The locale suffix to use
     *
     * @return the corresponding module name
     */
    public static String localizationModuleName(String localeString)
    {
        return LOCALE_MODULE_NAME_PREFIX + localeString;
    }

    /**
     * Lookup a resource in all the Derby modules. Returns the first
     * version of the resource which can be found. This should be unambiguous
     * due to the fact that packages cannot straddle multiple modules.
     *
     * @param resourceName The name of the resource to find
     *
     * @return a stream opened on the resource or null if it was not found
     */
    public static InputStream getResourceAsStream(String resourceName)
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        throws StandardException
    {
        initModuleInfo();  // find all of the derby modules

        InputStream retval = null;
        for (java.lang.Module module : _derbyModules.values())
        {
            retval = getResourceAsStream(module, resourceName);
            if (retval != null) { break; }
        }

        return retval;
    }

    /////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    /////////////////////////////////////////////////////////

    /**
     * Initialize the map of Derby modules.
     */
    private static void initModuleInfo()
    {
        if (_derbyModules != null) { return; }
//IC see: https://issues.apache.org/jira/browse/DERBY-6945

        HashMap<String,java.lang.Module> result = new HashMap<String,java.lang.Module>();
        ServiceLoader<DerbyModuleAPI> loader = ServiceLoader.load(DerbyModuleAPI.class);

        for (DerbyModuleAPI provider : loader)
        {
            Class providerClass = provider.getClass();
            java.lang.Module providerModule = providerClass.getModule();

            result.put(providerModule.getName(), providerModule);
        }

        _derbyModules = result;
    }

    /**
     * Lookup a resource in a module.
     *
     * @param module The module in which to look for the resource
     * @param resourceName The name of the resource
     *
     * @return a stream opened on the resource or null we can't find the resource in the module.
     */
    private static InputStream getResourceAsStream
      (
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
         final java.lang.Module module,
         final String resourceName
       )
      throws StandardException
    {
        InputStream retval = null;
        Throwable error = null;
        try
        {
            retval = AccessController.doPrivileged
              (
               new PrivilegedExceptionAction<InputStream>()
               {
                   public InputStream run() throws IOException
                   {
                       return module.getResourceAsStream(resourceName);
                   }
               }
               );
        }
        catch (PrivilegedActionException pae) { error = pae; }

        if (error != null)
        {
            throw StandardException.plainWrapException(error);
        }
        
        return retval;
    }
}
