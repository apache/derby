@REM ---------------------------------------------------------
@REM -- This batch file is an example of how to start the
@REM -- Derby server in the Network Server framework
@REM --
@REM -- REQUIREMENTS: 
@REM --	 You must have the derby jar files as well as the 
@REM --   derby network server class files in your classpath.
@REM --
@REM --  Check the setNetworkServerCP.bat file for an example of
@REM --   what to set.
@REM -- 
@REM -- This file for use on Windows systems
@REM ---------------------------------------------------------

rem set CLOUDSCAPE_INSTALL=
 
@if !"%CLASSPATH%"==! call "%CLOUDSCAPE_INSTALL%"/frameworks/NetworkServer/bin/setNetworkServerCP.bat
@if "%CLASSPATH%" == "" call "%CLOUDSCAPE_INSTALL%"/frameworks/NetworkServer/bin/setNetworkServerCP.bat

@REM ---------------------------------------------------------
@REM -- start Derby as a Network server
@REM ---------------------------------------------------------
java org.apache.derby.drda.NetworkServerControl start

@REM ---------------------------------------------------------
@REM -- To use a different JVM with a different syntax, simply edit
@REM -- this file
@REM ---------------------------------------------------------

