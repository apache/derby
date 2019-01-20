/*

   Derby - Class module-info

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

/**
 * <p>
 * A set of basic tools (including an interactive SQL monitor) can access
 * Derby databases via either the embedded or remote client
 * drivers. Optional functionality includes:
 * </p>
 * 
 * <ul>
 *  <li><b><font color="gray">org.apache.derby.engine</font></b> -
 *  Include this module for embedded access.</li>
 *  <li><b><font color="gray">org.apache.derby.client</font></b> -
 *  Include this module for remote access.</li>
 *  <li><b><font color="gray">org.apache.derby.locale_*</font></b> - Include
 *  these modules for non-English diagnostic messages.</li>
 *  <li><b><font color="gray">org.osgi.framework</font></b> - See the
 *  header comment on the engine module.</li>
 *  <li><b><font color="gray">java.management</font></b> - See the
 *  header comment on the engine module.</li>
 *  <li><b><font color="gray">java.naming</font></b> - This
 *  module supports JNDI lookup of LDAP authenticators when running with
 *  the embedded driver.</li>
 * </ul>
 * 
 * <p><b>Module Diagram:</b></p>
 *
 * <div style="text-align:center;">
 *   <img
 *     src="tools.svg"
 *     alt="module diagram for org.apache.derby.tools"
 *     border="2"
 *   />
 * </div>
 *
 */
module org.apache.derby.tools
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;
    requires java.logging;
    requires java.sql;
    requires java.xml;

    requires org.apache.derby.commons;

    //
    // OPTIONAL IMPORTS
    //
    // REQUIRED AT COMPILE-TIME.
    // OPTIONAL AT RUN-TIME.
    //
    requires static org.apache.derby.engine;
    requires static org.apache.derby.client;
    requires static java.naming;

    //
    // DERBY PUBLIC API
    //
    // VISIBLE TO ALL CLASSES AT RUNTIME.
    //
    exports org.apache.derby.jdbc;

    //
    // SUPPORT MODULE LOOKUP
    //
    provides org.apache.derby.shared.api.DerbyModuleAPI
        with org.apache.derby.info.tools.DerbyModule;

    //
    // ALLOW RESOURCE LOOKUP VIA REFLECTION
    //
    opens org.apache.derby.loc.tools;

    // ALLOW ACCESS TO info.properties
    opens org.apache.derby.info.tools;

    //
    // DERBY INTERNAL EXPORTS
    //
    // ONLY VISIBLE TO OTHER DERBY MODULES.
    //
    exports org.apache.derby.iapi.tools.i18n to
        org.apache.derby.server,
        org.apache.derby.optionaltools,
        org.apache.derby.runner,
        org.apache.derby.tests;

    exports org.apache.derby.impl.tools.ij to
        org.apache.derby.tests;

    exports org.apache.derby.impl.tools.planexporter to
        org.apache.derby.tests;

    exports org.apache.derby.impl.tools.sysinfo to
        org.apache.derby.server,
        org.apache.derby.tests;

    exports org.apache.derby.tools to
        org.apache.derby.optionaltools,
        org.apache.derby.runner,
        org.apache.derby.tests;

    //
    // FIXME! OPEN TO REFLECTIVE ACCESS FROM GENERATED
    // QUERY PLANS.
    //
    opens org.apache.derby.impl.tools.optional;
    
}
