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

module org.apache.derby.client
{
    //
    // MANDATORY IMPORTS
    //
    // REQUIRED AT COMPILE-TIME AND AT RUN-TIME.
    //
    requires java.base;
    requires java.logging;
    requires java.sql;

    requires org.apache.derby.commons;

    //
    // OPTIONAL IMPORTS
    //
    // REQUIRED AT COMPILE-TIME.
    // OPTIONAL AT RUN-TIME.
    //
    requires static java.naming;

    //
    // SUPPORT MODULE LOOKUP
    //
    provides org.apache.derby.shared.api.DerbyModuleAPI
        with org.apache.derby.info.client.DerbyModule;

    //
    // SUPPORT JDBC AUTOLOADING
    //
    provides java.sql.Driver
        with org.apache.derby.client.ClientAutoloadedDriver;

    //
    // ALLOW RESOURCE LOOKUP VIA REFLECTION
    //
    opens org.apache.derby.loc.client;

    //
    // DERBY INTERNAL EXPORTS
    //
    // ONLY VISIBLE TO OTHER DERBY MODULES.
    //
    exports org.apache.derby.client to
        org.apache.derby.tools,
        org.apache.derby.tests;

    exports org.apache.derby.client.am to
        org.apache.derby.tests;

    exports org.apache.derby.client.am.stmtcache to
        org.apache.derby.tests;

    exports org.apache.derby.client.net to
        org.apache.derby.tests;

    //
    // EXPOSE REFLECTIVE ACCESS TO TOOLS MODULE
    // SO THAT getFunctions() CAN BE CALLED BY IJ.
    //
    opens org.apache.derby.client.am to
        org.apache.derby.tools;
}
