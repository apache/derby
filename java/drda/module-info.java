module org.apache.derby.server
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;
    requires java.sql;

    requires org.apache.derby.engine;
    requires org.apache.derby.tools;
    requires org.apache.derby.commons;
    
    //
    // OPTIONAL IMPORTS
    //
    // REQUIRED AT COMPILE-TIME.
    // OPTIONAL AT RUN-TIME.
    //

    // NetServlet needs Java EE, which has not been modularized yet.
    requires static geronimo.spec.servlet;

    //
    // DERBY INTERNAL EXPORTS
    //
    // ONLY VISIBLE TO OTHER DERBY MODULES.
    //
    exports org.apache.derby.drda to
        org.apache.derby.runner,
        org.apache.derby.tests;

    exports org.apache.derby.impl.drda to
        org.apache.derby.tests;
}
