@REM ---------------------------------------------------------
@REM -- This batch file sets the CLASSPATH environment variable
@REM -- for use with Derby products in NetworkServer mode
@REM --
@REM -- To use this script from other locations, change the 
@REM -- value assigned to DERBY_INSTALL to be an absolute path 
@REM -- (set DERBY_INSTALL=C:\derby) instead of the current relative path
@REM --
@REM -- This file for use on Windows systems
@REM -- 
@REM ---------------------------------------------------------

@rem set DERBY_INSTALL=

@if "%DERBY_HOME%"=="" set DERBY_HOME=%DERBY_INSTALL%
@if "%DERBY_HOME%"=="" goto noderbyhome

@FOR %%X in ("%DERBY_HOME%") DO SET DERBY_INSTALL=%%~sX

set CLASSPATH=%DERBY_HOME%\lib\derby.jar;%DERBY_HOME%\lib\derbytools.jar;%DERBY_HOME%\lib\derbynet.jar;%CLASSPATH%
@goto end

:noderbyhome
@echo DERBY_HOME or DERBY_INSTALL not set. Set one of these variables
@echo to the location of your Derby installation.

:end