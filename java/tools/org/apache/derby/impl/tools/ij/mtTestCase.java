/*

   Derby - Class org.apache.derby.impl.tools.ij.mtTestCase

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.tools.ij;

import java.util.Hashtable;
import java.util.Properties;
import java.lang.Math;
import java.io.FileNotFoundException;
import java.io.BufferedInputStream;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.derby.iapi.tools.i18n.*;

/**
 */
public class mtTestCase
{
	public String name = null;
	public String file = null;
	public String propFile = null;
	public float weight = (float).5;
	public Hashtable ignoreErrors = null;
	public String description = null;


	private int iterations;
	private int attempts;

	public void mtTestCase()
	{ };

	public void setName(String name)
	{
		this.name = name;
	}
	public String getName()
	{
		return name;
	}

	public void setFile(String name)
	{
		this.file = name;
	}

	public void setInputDir(String dir)
	{
		file = dir + "/" + file;
	}

	public String getFile()
	{
		return file;
	}
	
	public void setPropFile(String name)
	{
		this.propFile = name;
	}

	public String getPropFile()
	{
		return propFile;
	}

	public void setWeight(int weight)
	{
		this.weight = (float)(weight/100.0);
	}
	
	public void setIgnoreErrors(Hashtable t)
	{
		this.ignoreErrors = t;
	}
	
	public void setDescription(String description)
	{
		this.description = description;
	}

	/**
	** Initialize the test case.  See initialize(String)
	*/
	public synchronized BufferedInputStream initialize() 
			throws FileNotFoundException, IOException
	{
		return initialize(null);
	}

	/**
	** Initizalize the test case.  Loads up the properties
	** file and sets the input stream.  Used to set up
	** prior to running the thread.
	*/
	public synchronized BufferedInputStream initialize(String inputDir) 
			throws FileNotFoundException, IOException
	{
		String filePath; 
		BufferedInputStream	inStream = null;

		// load up properties
		if (propFile != null)
		{	
			BufferedInputStream	propStream;
			Properties		p;
			String propPath = (inputDir == null) ?
						propFile : 
				(inputDir + "/" + propFile);
			
			try 
			{
				propStream = new BufferedInputStream(new FileInputStream(propPath));
			} catch (FileNotFoundException e)
			{
				System.out.println(name+": unable to find properties file "+propPath);
				throw e;
			}

			p = System.getProperties();
			p.load(propStream);
			// for network server need to alter url
			String framework = p.getProperty("framework");
			
			if (framework != null && framework.equals("DB2jNet"))
			{
				String newURLPrefix= "jdbc:derby:net://localhost:1527/";
				updateURLProperties(p,newURLPrefix);
				p.setProperty("ij.user","APP");
				p.setProperty("ij.password","PWD");
			}
			System.setProperties(p);
		}
		// set input stream
		filePath = (inputDir == null) ?
						file : (inputDir + "/" + file);

		try 
		{
			inStream = new BufferedInputStream(new FileInputStream(filePath), 
							utilMain.BUFFEREDFILESIZE);		
		} catch (FileNotFoundException e)
		{
			System.out.println("unable to find properties file "+filePath);
			throw e;
		}
		return inStream;
	}

	/**
	** Attempt to grab this test case.  
	** Uses random number and the weight of this
	** case to determine if the grab was successful.
	** 
	** @return true/false
	*/
	public synchronized boolean grab()
	{
		attempts++;
		if (java.lang.Math.random() < weight)
		{
			iterations++;
			return true;
		}
		else
		{
			return false;
		}
	}

	/**
	** Run the test case.  Invokes IJ to do our
	** dirty work.
	*/
	public void runMe(LocalizedOutput log, LocalizedOutput out, BufferedInputStream infile)
	{
		utilMain	utilInstance;
        LocalizedInput is;
        is = LocalizedResource.getInstance().getNewInput(infile);

		LocalizedInput [] in = { is };
	
		out.println("--------------"+file+"-----------------");
		utilInstance = new utilMain(1, out, ignoreErrors);
		utilInstance.setMtUse(true);
		utilInstance.go(in, out, (java.util.Properties) null);
		log.flush();
		out.flush();
	}

	public void updateURLProperties(Properties p, String newURLPrefix)
	{
		String[] propsToUpdate = {"ij.database", "ij.protocol",
								  "database"};
		for (int i = 0; i < propsToUpdate.length; i++)
		{
			String key = propsToUpdate[i];
			String val = p.getProperty(key);
			if (val != null)
				p.setProperty(key,alterURL(val,newURLPrefix));
		}
	}


	public String alterURL(String url, String newURLPrefix)
	{
		String urlPrefix = "jdbc:derby:";
	
		if (url.startsWith(newURLPrefix))
			return url;

		// If we don't have a URL prefix for this framework
		// just return
		if (newURLPrefix == null)
			return url;
	
		if (url.equals(urlPrefix)) // Replace embedded
			return newURLPrefix;

		if (url.startsWith(urlPrefix))
		{
			// replace jdbc:derby: with our url:
			url = newURLPrefix +
				url.substring(urlPrefix.length());

		}
		else
		{
			if (! (url.startsWith("jdbc:")))
	    {
			url = newURLPrefix + url;
	    }
		}
		//System.out.println("New url:" +url);
		return url;
    }
  

// NOTE: tried invoking ij directly, but had some problems,
// so stick with calling utilMain().	
//	/**
//	** Run the test case.  Invokes IJ to do our
//	** dirty work.
//	*/
//	public void runMe(AppStreamWriter log, AppStreamWriter out, BufferedInputStream infile)
//	{
//		ASCII_UCodeESC_CharStream charStream;
//		ijTokenManager	ijTokMgr;
//		ij	ijParser;
//	
//		
//		out.println("--------------"+file+"-----------------");
//		charStream = new ASCII_UCodeESC_CharStream(in, 1, 1);
//		ijTokMgr = new ijTokenManager(charStream);
//		ijParser = new ij(ijTokMgr, System.out, this);
//		log.flush();
//		out.flush();
//	}

	/**
	** Name says it all
	*/
	public String toString()
	{
		return "name: "+name+
				"\n\tfile: "+file+
				"\n\tproperties: "+propFile+
				"\n\tweight: "+weight+
				"\n\tignoreErrors: "+ignoreErrors+
				"\n\tdescription: "+description;
	}

	
}
