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
 * The smallest footprint, core Derby configuration consists of the embedded engine. In
 * this configuration, Derby is a complete, secure, transactional,
 * recoverable data manager which runs in the same JVM as the user's
 * application. This configuration includes an embedded JDBC driver,
 * accessed via <i>java.sql.DriverManager.getConnection()</i>. More
 * functionality can be enabled by adding other, optional modules to this configuration:
 * </p>
 * 
 * <ul>
 *  <li><b><font color="gray">org.apache.derby.tools</font></b> - This module lets the application access Derby via
 *   <i>javax.sql.DataSource</i>s.</li>
 *  <li><b><font color="gray">org.apache.derby.locale_*</font></b> - These
 *  modules let Derby translate diagnostic messages into
 *  languages other than English.</li>
 *  <li><b><font color="gray">org.osgi.framework</font></b> - This
 *  module lets Derby load as an OSGI component. The user must
 *  supply an OSGI implementation in a module named <b><font color="gray">org.osgi.framework</font></b>.</li>
 *  <li><b><font color="gray">java.management</font></b> - This
 *  module lets JMX MBeans introspect the Derby engine.</li>
 *  <li><b><font color="gray">java.naming</font></b> - This
 *  module supports JNDI lookup of DataSources and LDAP authenticators.</li>
 * </ul>
 *
 * <p><b>Module Diagram:</b></p>
 *
 * <div style="text-align:center;">
 *   <img
 *     src="engine.svg"
 *     alt="module diagram for org.apache.derby.engine"
 *     border="2"
 *   />
 * </div>
 *
 */
module org.apache.derby.engine
{
    //
    // MANDADORY IMPORTS
    //
    // REQUIRED AT COMPILE-TIME AND AT RUN-TIME.
    //
    requires java.base;
    requires java.logging;
    requires java.management;
    requires java.sql;
    requires java.xml;
    
    requires org.apache.derby.commons;

    //
    // OPTIONAL IMPORTS
    //
    // REQUIRED AT COMPILE-TIME.
    // OPTIONAL AT RUN-TIME.
    //
    requires static java.naming;

    requires static org.osgi.framework;

    //
    // DERBY PUBLIC API
    //
    // VISIBLE TO ALL CLASSES AT RUNTIME.
    //
    exports org.apache.derby.agg;
    exports org.apache.derby.authentication;
    exports org.apache.derby.catalog;
    exports org.apache.derby.vti;

    // DOES NOT APPEAR IN PUBLIC API JAVADOC YET:
    exports org.apache.derby.security;

    //
    // SUPPORT MODULE LOOKUP
    //
    provides org.apache.derby.shared.api.DerbyModuleAPI
        with org.apache.derby.info.engine.DerbyModule;

    //
    // SUPPORT JDBC AUTOLOADING
    //
    provides java.sql.Driver
        with org.apache.derby.iapi.jdbc.AutoloadedDriver;

    //
    // ALLOW RESOURCE LOOKUP VIA REFLECTION
    //
    
    // ALLOW THE Monitor TO FIND modules.properties
    opens org.apache.derby;
    // ALLOW ACCESS TO ENGLISH MESSAGES
    opens org.apache.derby.loc;
    // ALLOW ACCESS TO info.properties
    opens org.apache.derby.info.engine;

    //
    // ALLOW CLASS INSTANTIATION VIA REFLECTION
    //
    
    // ALLOW ExceptionFactory TO INSTANTIATE SQLExceptionFactory
    opens org.apache.derby.impl.jdbc to org.apache.derby.commons;

    //
    // FIXME! EXPOSED SO THAT THESE PACKAGES CAN BE ACCESSED
    // BY THE QUERY PLANS WHICH ARE CODE-GENERATED
    // INTO THE UNNAMED MODULE.
    //
    exports org.apache.derby.diag;
    exports org.apache.derby.iapi.db;
    exports org.apache.derby.iapi.services.io;
    exports org.apache.derby.iapi.services.loader;
    exports org.apache.derby.iapi.sql;
    exports org.apache.derby.iapi.sql.conn;
    exports org.apache.derby.iapi.sql.execute;
    exports org.apache.derby.iapi.store.access;
    exports org.apache.derby.iapi.types;
    exports org.apache.derby.iapi.util;
    exports org.apache.derby.impl.sql.execute;
    exports org.apache.derby.impl.load;
    exports org.apache.derby.impl.jdbc;

    //
    // DERBY INTERNAL EXPORTS
    //
    // VISIBLE ONLY INSIDE derby.jar AT RUNTIME.
    //
    exports org.apache.derby.catalog.types to
        org.apache.derby.tests;

    exports org.apache.derby.database to
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.jdbc to
        org.apache.derby.server,
        org.apache.derby.tools,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.cache to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.context to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.crypto to
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.daemon to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.diag to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.jmx to
        org.apache.derby.server;

    exports org.apache.derby.iapi.services.locks to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.monitor to
        org.apache.derby.server,
        org.apache.derby.tools,
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.property to
        org.apache.derby.server,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.services.uuid to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.sql.compile to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.sql.depend to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.sql.dictionary to
        org.apache.derby.tools,
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.store.access.conglomerate to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.store.access.xa to
        org.apache.derby.server,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.store.raw to
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.store.raw.data to
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.iapi.store.raw.log to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.store.raw.xact to
        org.apache.derby.tests;

    exports org.apache.derby.iapi.transaction to
        org.apache.derby.server;

    exports org.apache.derby.impl.io to
        org.apache.derby.tests;

    exports org.apache.derby.impl.io.vfmem to
        org.apache.derby.tests;

    exports org.apache.derby.impl.jdbc.authentication to
        org.apache.derby.optionaltools;

    exports org.apache.derby.impl.services.jce to
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.impl.sql to
        org.apache.derby.tests;

    exports org.apache.derby.impl.sql.catalog to
        org.apache.derby.tests;

    exports org.apache.derby.impl.store.access.btree to
        org.apache.derby.tests;

    exports org.apache.derby.impl.store.access.btree.index to
        org.apache.derby.tests;

    exports org.apache.derby.impl.store.access.conglomerate to
        org.apache.derby.tests;

    exports org.apache.derby.impl.store.access.heap to
        org.apache.derby.tests;

    exports org.apache.derby.impl.store.raw.data to
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.impl.store.raw.log to
        org.apache.derby.tests;

    exports org.apache.derby.io to
        org.apache.derby.optionaltools,
        org.apache.derby.tests;

    exports org.apache.derby.mbeans to
        java.management,
        org.apache.derby.server;
    // must be opened to reflective access by the unnamed module
    opens org.apache.derby.mbeans;

    //
    // STANZAS FOR USE WHEN QUERY PLANS ARE GENERATED INTO
    // SOME MODULE OTHER THAN THE UNNAMED MODULE.
    //
    //
    // QUERY PLANS REFERENCE org.apache.derby.iapi.services.io.Storable
    //
    //    exports org.apache.derby.iapi.services.io to
    //        org.apache.derby.server,
    //        org.apache.derby.optionaltools,
    //        org.apache.derby.tests;
    //
    //
    // QUERY PLANS REFERENCE GeneratedByteCode
    //
    //exports org.apache.derby.iapi.services.loader to
    //    org.apache.derby.optionaltools,
    //    org.apache.derby.tests;
    //
    // QUERY PLANS REFERENCE Row
    //
    //exports org.apache.derby.iapi.sql to
    //    org.apache.derby.tests;
    //
    //
    // QUERY PLANS REFEREENCE LanguageConnectionContext
    //
    //    exports org.apache.derby.iapi.sql.conn to
    //        org.apache.derby.optionaltools,
    //        org.apache.derby.tests;
    //
    //
    // QUERY PLANS REFERENCE ExecutionFactory
    //
    //exports org.apache.derby.iapi.sql.execute to
    //    org.apache.derby.tests;
    //
    //
    // QUERY PLANS REFERENCE org.apache.derby.iapi.store.access.Qualifier
    //
    //exports org.apache.derby.iapi.store.access to
    //    org.apache.derby.tests;
    //
    //
    // QUERY PLANS REFERENCE DataValueFactory
    //
    //exports org.apache.derby.iapi.types to
    //    org.apache.derby.optionaltools,
    //    org.apache.derby.tests;
    //
    //
    // QUERY PLANS REFERENCE org.apache.derby.iapi.util.StringUtil
    //
    //    exports org.apache.derby.iapi.util to
    //        org.apache.derby.server,
    //        org.apache.derby.tools,
    //        org.apache.derby.optionaltools,
    //        org.apache.derby.tests;
    //
    //
    // QUERY PLANS EXTEND BaseActivation
    //
    //exports org.apache.derby.impl.sql.execute to
    //    org.apache.derby.tests;
    //
    //
    // QUERY PLANS REFERENCE org.apache.derby.iapi.db.Factory
    //    exports org.apache.derby.iapi.db to
    //        org.apache.derby.server,
    //        org.apache.derby.tools,
    //        org.apache.derby.optionaltools,
    //        org.apache.derby.tests;
    //
    //
    // QUERY PLANS REFERENCE org.apache.derby.impl.jdbc.LOBStoredProcedure
    //    exports org.apache.derby.impl.jdbc to
    //        org.apache.derby.server,
    //        org.apache.derby.tools,
    //        org.apache.derby.optionaltools,
    //        org.apache.derby.tests;

}
