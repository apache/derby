module org.apache.derby.runner
{
    //
    // MANDATORY IMPORTS
    //
    requires java.base;

    requires org.apache.derby.server;
    requires org.apache.derby.tools;

    //
    // DERBY PUBLIC API
    //
    // VISIBLE TO ALL CLASSES AT RUNTIME.
    //
    exports org.apache.derby.run;
}
