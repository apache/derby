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

module org.apache.derby.tools
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;
    requires java.logging;
    requires java.sql;
    requires java.xml;

    requires org.apache.derby.engine;
    requires org.apache.derby.client;
    requires org.apache.derby.commons;

    //
    // OPTIONAL IMPORTS
    //
    // REQUIRED AT COMPILE-TIME.
    // OPTIONAL AT RUN-TIME.
    //
    requires static java.naming;

    //
    // DERBY PUBLIC API
    //
    // VISIBLE TO ALL CLASSES AT RUNTIME.
    //
    exports org.apache.derby.jdbc;

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
}
