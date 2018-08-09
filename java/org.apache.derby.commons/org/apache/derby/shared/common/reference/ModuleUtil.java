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

import java.io.File;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;

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

    /** ModuleFinder for modules on the JVM's module path */
    private static ModuleFinder _systemModuleFinder;

    /////////////////////////////////////////////////////////
    //
    // ENTRY POINTS
    //
    /////////////////////////////////////////////////////////

    /**
     * Return the module with the given name located on the system module path.
     *
     * @param moduleName The name of the module
     *
     * @return the corresponding module, or null if none is found
     */
    public static java.lang.Module jvmSystemModule(String moduleName)
    {
        if (!JVMInfo.isModuleAware()) { return null; }
        
        initModulePaths();  // read system module path

        try
        {
            ModuleLayer parent = ModuleLayer.boot();
            Configuration configuration =
                parent
                .configuration()
                .resolve(_systemModuleFinder, ModuleFinder.of(), Set.of(moduleName));
            ModuleLayer layer =
                parent.defineModulesWithOneLoader(configuration, ClassLoader.getSystemClassLoader());

            return layer.findModule(moduleName).orElse(null);
        }
        catch (Exception ex)
        {
            return null;
        }
    }

    /**
     * Return all of the jigsaw modules on the system module path.
     *
     * @param name The name of the module
     *
     * @return the corresponding module, or null if none is found
     */
    public static Set<ModuleReference> allJvmSystemModules()
    {
        if (!JVMInfo.isModuleAware()) { return null; }

        initModulePaths();  // read system module path

        return _systemModuleFinder.findAll();
    }

    /**
     * Get the name of the module containing the localized
     * messages for the given locale.
     *
     * @param locale The locale in question
     *
     * @return the corresponding module name
     */
    public static String localizationModuleName(Locale locale)
    {
        return LOCALE_MODULE_NAME_PREFIX + locale.toString();
    }

    /////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    /////////////////////////////////////////////////////////

    /**
     * Initialize the list of module paths if necessary.
     */
    private static void initModulePaths()
    {
        if (_systemModuleFinder != null) { return; }

        String modulePathString = JVMInfo.getSystemModulePath();
        StringTokenizer tokens = new StringTokenizer(modulePathString, File.pathSeparator);
        int tokenCount = tokens.countTokens();
        Path[] modulePaths = new Path[tokenCount];

        for (int idx = 0; idx < tokenCount; idx++)
        {
            modulePaths[idx] = Paths.get(tokens.nextToken());
        }

        _systemModuleFinder = ModuleFinder.of(modulePaths);
    }

}
