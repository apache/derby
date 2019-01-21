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
 * This convenience module makes it easy to run several tools and to
 * administer the network server. Optional functionality includes:
 * </p>
 * 
 * <ul>
 *  <li><b><font color="gray">org.apache.derby.locale_*</font></b> - Include
 *  these modules for non-English diagnostic messages.</li>
 *  <li><b><font color="gray">java.naming</font></b> - This
 *  module supports JNDI lookup of LDAP authenticators.</li>
 * </ul>
 * 
 * <p><b>Module Diagram:</b></p>
 *
 * <div style="text-align:center;">
 *   <img
 *     src="runner.svg"
 *     alt="module diagram for org.apache.derby.runner"
 *     style="border:2px solid #000000"
 *   />
 * </div>
 *
 */
module org.apache.derby.runner
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;

    requires org.apache.derby.commons;
    requires org.apache.derby.server;
    requires org.apache.derby.tools;

    //
    // DERBY PUBLIC API
    //
    // VISIBLE TO ALL CLASSES AT RUNTIME.
    //
    exports org.apache.derby.run;

    //
    // SUPPORT MODULE LOOKUP
    //
    provides org.apache.derby.shared.api.DerbyModuleAPI
        with org.apache.derby.run.info.DerbyModule;

}
