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
@echo off
rem set DERBY_INSTALL=

@if "%DERBY_HOME%"=="" set DERBY_HOME=%DERBY_INSTALL%
@if "%DERBY_HOME%"=="" goto noderbyhome

@if "%JAVA_HOME%"=="" goto nojavahome
@if not exist "%JAVA_HOME%\bin\java.exe" goto nojavahome

@if !"%CLASSPATH%"==! call "%DERBY_INSTALL%"/frameworks/embedded/bin/setEmbeddedCP.bat
@if "%CLASSPATH%" == "" call "%DERBY_INSTALL%"/frameworks/embedded/bin/setEmbeddedCP.bat

@REM ---------------------------------------------------------
@REM -- start ij
@REM ---------------------------------------------------------
"%JAVA_HOME%\bin\java" -Dij.protocol=jdbc:derby: org.apache.derby.tools.ij
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