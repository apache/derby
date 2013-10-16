Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Obtaining the Source and Building the Derby 1.0 UI and Help Plug-ins

This document describes how to obtain the source for the
Apache Derby 1.0 UI and Help plug-ins for Eclipse.  Instructions
for importing the source as an Eclipse project and packaging the
plug-ins as a zip file are also included.

Note that the Derby community no longer provides this plugin, and that the source code has not been certified to work with Eclipse versions after Juno.

Contents:

I. Creating the Apache Derby UI and Help Plug-in Projects
II. Creating zip files for the Derby UI and Help Plug-ins via the Project

=========================================================================

I. Creating the Apache Derby UI and Help Plug-in projects:

1) Check out the source for the UI and Help plug-ins via SVN (Subversion).

   Refer to the detailed instructions on readying your environment
   to check out any Derby source here: 

   http://db.apache.org/derby/derby_downloads.html

   To check out the entire development trunk use this command:

   svn checkout https://svn.apache.org/repos/asf/db/derby/code/trunk/

   This includes the source for both the UI and Help plugins.  If you want to
   check out only the UI and Help plugins, and not the entire Derby source code 
   issue this command instead:

   svn checkout https://svn.apache.org/repos/asf/db/derby/code/trunk/plugins

  
2) Install Eclipse 3.x and the JDK needed

   You need an Eclipse build/package that supports Plugin Development.
 
   You need to install JDK 1.5. 
   If you use 1.6, the ui plugin will be unusable with 1.5 jvms.
   On MS Windows systems, you may have to uninstall or temporarily remove the java
   version installed in the system and/or use a command prompt window in which 
   you have explicitly changed the path, to start the eclipse IDE rather than use icons.  

3) Install the Apache Derby 10.x Eclipse Core plug-in from:
   http://db.apache.org/derby/derby_downloads.html

   It is available as a zip file:

   for example; derby_core_plugin_10.1.3.zip

   Unzip this file into the directory where the eclipse executable is located.
   For instance, if Eclipse is installed in C:\eclipse, unzip the Derby
   Core plug-in zip file to C:\eclipse.

4) Invoke the Eclipse IDE, provide an appropriate location as the workspace

   for example: c:\derby\plugin

5) Import the Apache Derby UI project:
   File -> Import -> Existing Project into Workspace
   - Click Next
   - Click Browse and point to the "org.apache.derby.ui" directory (created in 
     Step 1, which is under the plugins/eclipse directory where you checked 
     out the source)
   - Click Finish

6) Switch to the Plug-in perspective
   Window -> Open Perspective -> Other -> Select Plug-in Development

7) To invoke and test the Apache Derby UI Plug-in in this development 
   environment
   
   Run ->Run As -> Eclipse Application (Eclipse 3.1)
   OR
   Run --> Run As --> Run-time Workbench (Eclipse 3.0)
   
   An alternate way would be to open the plugin.xml and select the 
   'Launch a runtime workbench' link.

   This will open a new Eclipse window with all the current plug-ins under 
   development in its environment.

Follow steps 4 - 7 as above for creating the Help plug-in project, substituting
org.apache.derby.plugin.doc for org.apache.derby.ui.
================================================================================

II. Creating zip files for the Derby UI and Help Plug-ins via the Project

In order to install the plug-ins easily in another Eclipse environment creating
a zip file is useful.

To create a zip file for the UI plug-in only:

1) From within the Plug-in Development perspective, right-click the 
   org.apache.derby.ui project.  Select Export --> Deployable plug-ins and 
   fragments, then the Next button.

2) In the Deployable plug-ins and fragments window select the 
   org.apache.derby.ui project listed in the Available Plug-ins and Fragments 
   text area. Depending on the Eclipse version used:

   Eclipse 3.1:
   In the Export Destination, select the Archive File option

   Eclipse 3.0:
   For the Export Options section select Deploy as: a single ZIP file.  Click 
   the Build Options button.  Check any options desired from the Build Options 
   Preferences window and then click OK.

3) Browse to a Destination where you would like to put the zip file and provide 
   a file name for example: derby_ui_plugin_1.1.0.zip, to represent the name and
   version of the plug-in.

4) Finally, click Finish.

5) To install in another Eclipse installation unzip this file in the base
   directory of the Eclipse installation.

To create a zip file for the Doc plug-in only:

1) From within the Plug-in Development perspective, right-click the
   org.apache.derby.plugin.doc project.  Select Export --> Deployable plug-ins 
   and fragments, then the Next button.

2) In the Deployable plug-ins and fragments window select the 
   org.apache.derby.plugin.doc project listed in the left frame.  In the right 
   frame uncheck the .project and build.properties files. Depending on the 
   Eclipse version used:

   Eclipse 3.1:
   In the Export Destination, select the Archive File option

   Eclipse 3.0:
   For the Export Options section select Deploy as: a single ZIP file.  Click 
   the Build Options button.  Check any options desired from the Build Options 
   Preferences window and then click OK.


3) Browse to a Destination where you would like to put the zip file and provide    a file name, for example: derby_doc_plugin_1.1.0.zip, to represent the name 
   and version of the plug-in.

4) Finally, click Finish.

5) Note that the documentation zip files' base directory is
   org.apache.derby.plugin.doc and therefore must be unzipped in the plug-ins
   directory.

To create a single zip file for both UI and Doc plug-ins:

1) From within the Plug-in Development perspective, right-click the
   org.apache.derby.plugin.doc and the org.apache.derby.ui projects.  Select 
   Export --> Deployable plug-ins and fragments, then the Next button.

2) In the Deployable plug-ins and fragments window select the 
   org.apache.derby.plugin.doc project listed in the left frame.  In the right 
   frame uncheck the .project and build.properties files. Depending on the 
   Eclipse version used:

   Eclipse 3.1:
   In the Export Destination, select the Archive File option

   Eclipse 3.2:
   Make sure the Option 'Package plug-ins as individual JAR archives is not checked.

3) Browse to a Destination where you would like to put the zip file and provide    a file name, for example: derby_ui_plugin_1.1.0.zip,  to represent the name 
   and version of the plug-in.

4) Finally, click Finish.


