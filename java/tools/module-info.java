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
        org.apache.derby.runner;

    exports org.apache.derby.impl.tools.sysinfo to
        org.apache.derby.server;

    exports org.apache.derby.tools to
        org.apache.derby.optionaltools,
        org.apache.derby.runner;
}
