@REM ---------------------------------------------------------
@REM -- This batch file is an example of how to start ij in 
@REM -- an embedded environment.
@REM --
@REM -- REQUIREMENTS: 
@REM -- You must have the Derby libraries in your classpath
@REM -- 
@REM -- See the setEmbeddedCP.bat for an example of
@REM -- how to do this.
@REM --
@REM -- This file for use on Windows systems
@REM ---------------------------------------------------------

rem set DERBY_INSTALL=
 
@if !"%CLASSPATH%"==! call "%DERBY_INSTALL%"/frameworks/embedded/bin/setEmbeddedCP.bat
@if "%CLASSPATH%" == "" call "%DERBY_INSTALL%"/frameworks/embedded/bin/setEmbeddedCP.bat

@REM ---------------------------------------------------------
@REM -- start ij
@REM ---------------------------------------------------------
java -Dij.protocol=jdbc:derby: org.apache.derby.tools.ij


@REM ---------------------------------------------------------
@REM -- To use a different JVM with a different syntax, simply edit
@REM -- this file
@REM ---------------------------------------------------------

