/*

   Derby - Class org.apache.derbyBuild.eclipse.DerbyEclipsePlugin

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyBuild.eclipse;

import java.util.*;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import org.apache.derby.iapi.services.info.ProductGenusNames;
import org.apache.derby.iapi.services.info.PropertyNames;

/**
 * This class provides the functionality to create the build related
 * related properties, which are used for creating the Derby plug-in
 * for Eclipse by the ANT script.
 *
 * - The Eclipse plugin will be called 'Apache Derby Core Plug-in for Eclipse'
 *
 * - The plugin can be build from the main build.xml using 'ant' with the 'plugin'
 *   argument.
 *
 * - The package name for the Derby plug-in will
 *   be: org.apache.derby.core_<major>.<minor>.<interim> (example: org.apache.derby.core_10.1.0).
 *
 * - The plugin.xml in the Derby plug-in will show the actual version of the
 *   the Derby build (example: 10.1.0.0 (111545M) ). This can be viewed from
 *   Help - About Eclipse Platform - Plug-in Details of Eclipse of the Eclipse IDE
 *
 * - The zip file created for the DerbyEclipse under the jars directory will have the name:
 *   derby_core_plugin_<major>.<minor>.<interim>.zip (example:derby_core_plugin_10.1.0.zip)
 *
 * - The current packaging includes derby.jar, derbynet.jar and
 *   derbytools.jar. The locale jars for Derby are not included yet.
 *
 * @author Rajesh Kartha
 */
public class DerbyEclipsePlugin{
	/*
	 * Derby plug-in package property and name
	 */
	private static String PLUGIN_PKG="plugin.derby.core";
	private static String PLUGIN_PKG_NAME="org.apache.derby.core";
	/*
	 * Derby plug-in zip file property and name
	 */
	private static String PLUGIN_ZIP_FILE="plugin.derby.core.zipfile";
	private static String PLUGIN_ZIP_FILE_NAME="derby_core_plugin";
	/*
	 * Derby plug-in build properties and name
	 */
	private static String PLUGIN_VERSION="plugin.derby.version";
	private static String PLUGIN_VERSION_BUILD_NUMBER="plugin.derby.version.build.number";
	private static int MAINT_DIV=1000000;

	/*
	 * plugin.xml file information, split into three parts
	 */
	private static String part_1="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
				      "<?eclipse version=\"3.0\"?> \n"+
				      "<plugin \n" +
   				      "\t id=\"org.apache.derby.core\" \n" +
   				      "\t name=\"Apache Derby Core Plug-in for Eclipse\" \n" +
   				      "\t version=\"";

	private static String part_2="\n\t provider-name=\"";
	private static String part_3="\n\t <runtime> \n" +
      				      "\t\t <library name=\"derby.jar\"> \n" +
         			      "\t\t\t <export name=\"*\"/> \n" +
      				      "\t\t </library> \n" +
      				      "\t\t <library name=\"derbytools.jar\"> \n"+
         			      "\t\t\t <export name=\"*\"/> \n"+
      				      "\t\t </library> \n"+
      				      "\t\t <library name=\"derbynet.jar\"> \n"+
         			      "\t\t\t <export name=\"*\"/> \n"+
      				      "\t\t </library> \n"+
   				      "\t </runtime> \n"+
   				      "\t <requires> \n"+
   				      "\t </requires> \n"+
   				      "</plugin>";

	private String version="";
	private String tmpPropFile="plugintmp.properties";
	private static File tmpFileLocation=null;
	private static Properties tmpProp=new Properties();
	private String pluginXMLFile="plugin.xml";

	/*
	 * The public main() method to test the working of this class. A valid destination
	 * String is all that is needed as an argument for running this class.
	 * <p>
	 * example: java DerbyEclipsePlugin <destination>
	 * <p>
	 */

	public static void main(String [] args){
		if(args.length<=0){
			System.out.println("Incorrect number of arguments.");
			return;
		}
		try{
			tmpFileLocation=new File(args[0]);
			DerbyEclipsePlugin dep = new DerbyEclipsePlugin();
			dep.getProps();
			dep.createTmpFiles();
		}catch(Exception e)
		{
			e.printStackTrace();
		}




	}
	/*
	 * For internal use only.
	 * getProps() generates the required Properties from the DBMS.properties file.
	 *
	 * @exception	Exception if there is an error
	 */
	private void getProps() throws Exception{
		InputStream versionStream = getClass().getResourceAsStream(ProductGenusNames.DBMS_INFO);
		Properties prop=new Properties();
		prop.load(versionStream);

		//create the tmp Prop file
		tmpProp.put(PLUGIN_PKG,PLUGIN_PKG_NAME);			//package name
		tmpProp.put(PLUGIN_ZIP_FILE,PLUGIN_ZIP_FILE_NAME);	//zip file name
		tmpProp.put(PropertyNames.PRODUCT_VENDOR_NAME,prop.getProperty(PropertyNames.PRODUCT_VENDOR_NAME));
		int maint=Integer.parseInt(prop.getProperty(PropertyNames.PRODUCT_MAINT_VERSION));
		version=prop.getProperty(PropertyNames.PRODUCT_MAJOR_VERSION)+"."+prop.getProperty(PropertyNames.PRODUCT_MINOR_VERSION)+"."+maint/MAINT_DIV;
		tmpProp.put(PLUGIN_VERSION,version);

		//initially thought of using
		//version+="."+maint%MAINT_DIV+"_v"+prop.getProperty(PropertyNames.PRODUCT_BUILD_NUMBER);
		version+="."+maint%MAINT_DIV+" ("+prop.getProperty(PropertyNames.PRODUCT_BUILD_NUMBER)+")";
		tmpProp.put(PLUGIN_VERSION_BUILD_NUMBER,version);

		//add info to plugin.xml strings
		part_1+=version+"\"";
		part_2+=tmpProp.getProperty(PropertyNames.PRODUCT_VENDOR_NAME)+"\">\n";

	}
	/*
	 * For internal use only.
	 * createTmpFiles() create the temporary files with the build properties at the specified location.
	 *
	 * @exception	Exception if there is an error
	 */
	private void createTmpFiles() throws Exception{
		File file=new File(tmpFileLocation.getAbsolutePath()+File.separator+tmpPropFile);
		FileOutputStream fo=new FileOutputStream(file);
		tmpProp.store(fo,null);
		fo.close();
		file=new File(tmpFileLocation.getAbsolutePath()+File.separator+pluginXMLFile);
		FileWriter fw=new FileWriter(file);
		fw.write(part_1+part_2+part_3);
		fw.close();

	}
}

