@REM ---------------------------------------------------------
@REM -- This batch file is an example of how to start ij in 
@REM -- an NetworkServer environment.
@REM --
@REM -- REQUIREMENTS: 
@REM -- You must have the Derby and DB2 JCC libraries in your classpath
@REM -- 
@REM -- See the setNetworkClientCP.bat for an example of
@REM -- how to do this.
@REM --
@REM -- You may need to modify the values below for a different
@REM -- host, port, user, or password
@REM --
@REM -- This file for use on Windows systems
@REM ---------------------------------------------------------
@echo off
rem set DERBY_INSTALL=

@if "%DERBY_HOME%"=="" set DERBY_HOME=%DERBY_INSTALL%
@if "%DERBY_HOME%"=="" goto noderbyhome

@if "%JAVA_HOME%"=="" goto nojavahome
@if not exist "%JAVA_HOME%\bin\java.exe" goto nojavahome

@if !"%CLASSPATH%"==! call "%DERBY_HOME%"/frameworks/NetworkServer/bin/setNetworkClientCP.bat
@if "%CLASSPATH%" == "" call "%DERBY_HOME%"/frameworks/NetworkServer/bin/setNetworkClientCP.bat

set IJ_HOST=localhost
set IJ_PORT=1527
set IJ_USER=APP
set IJ_PASSWORD=APP

@REM ---------------------------------------------------------
@REM -- start ij
@REM -- host, port, user and password may need to be changed
@REM ---------------------------------------------------------
@echo on
"%JAVA_HOME%\bin\java" -Dij.driver=org.apache.derby.jdbc.ClientDriver -Dij.protocol=jdbc:derby://%IJ_HOST%:%IJ_PORT%/ -Dij.user=%IJ_USER% -Dij.password=%IJ_PASSWORD%  org.apache.derby.tools.ij
@goto end

@REM ---------------------------------------------------------
@REM -- To use a different JVM with a different syntax, simply edit
@REM -- this file
@REM ---------------------------------------------------------

:nojavahome
echo JAVA_HOME not set or could not find java executable in JAVA_HOME.
echo Please set JAVA_HOME to the location of a valid Java installation.
goto end

:noderbyhome
echo DERBY_HOME or DERBY_INSTALL not set. Set one of these variables
echo to the location of your Derby installation.
goto end

:end