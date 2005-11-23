-- Changed to create individual procedures so that this will work with JSR169. 
-- Direct call to 'installRoutines' uses nested connection
CREATE PROCEDURE TESTROUTINE.SET_SYSTEM_PROPERTY(IN PROPERTY_KEY VARCHAR(32000), IN PROPERTY_VALUE VARCHAR(32000)) NO SQL EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestRoutines.setSystemProperty' language java parameter style java;
CREATE PROCEDURE TESTROUTINE.SLEEP(IN SLEEP_TIME_MS BIGINT) NO SQL EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.TestRoutines.sleep' language java parameter style java;
