package org.apache.derbyTesting.junit;

import junit.extensions.TestSetup;
import junit.framework.Test;

final class ChangeConfigurationSetup extends TestSetup {
    
    private final TestConfiguration config;
    private TestConfiguration old;
    
    ChangeConfigurationSetup(TestConfiguration config, Test test)
    {
        super(test);
        this.config = config;
    }
    
    protected void setUp()
    {
        old = TestConfiguration.getCurrent();
        TestConfiguration.setCurrent(config);
    }
    
    protected void tearDown()
    {
        TestConfiguration.setCurrent(old);
    }
}
