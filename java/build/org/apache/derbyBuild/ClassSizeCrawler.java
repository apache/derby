/*

   Derby - Class org.apache.derby.iapi.services.cache.ClassSizeCrawler

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyBuild;

import org.apache.derby.iapi.services.cache.ClassSize;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.SecurityException;
import java.lang.ClassNotFoundException;
import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Calendar;
import java.util.Date;

/**
 * This class implements a program that catalogs the size estimate coefficients of various classes.
 * @see ClassSize#getSizeCoefficients.
 *<p>
 * The program is invoked as:
 *<p>
 * java -DWS=<i>work-space</i> [-DclassDir=<i>class-dir</i>] [-Dout=<i>out-file</i> [-Dprefix[.<i>x</i>=<i>package-prefix</i>]] [-Dverbose=true] org.apache.derby.iapi.services.cache.ClassSizeCrawler <i>class-or-interface</i> ...<br>
 *<p>
 * This program gets the size coefficients for each class in the <i>class-or-interface</i> list,
 * and for each class that implements an interface in the list. If there is an interface in the list
 * this program crawls through the classes hierarcy, starting at points specified by the prefix
 * properties, looking for classes that implement the interfaces.
 *<p>
 * If the <i>class-or-interface</i> list is empty then this program searches for implementations
 * of org.apache.derby.iapi.types.DataValueDescriptor, and at least one prefix property
 * must be specified
 *<p>
 * The catalog is written as a java source file
 * into <i>out-file</i>, by default
 * <i>work-space</i>/java/org.apache.derby.iapi.services.cache.ClassSizeCatalog.java.
 *<p>
 * <i>work-space</i> is the directory containing the java and classes directories. $WS in the
 * standard development environment. This property is required.
 *<p>
 * <i>class-dir</i> is the directory containing the compiled classes. By default it is <i>work-space</i>/classes.
 *<p>
 * <i>package-prefix</i> is the first part of a package name. e.g. "com.ibm.db2j.impl". At least
 * one prefix property must be specified if there is an interface in the list.
 *<p>
 * For example:<br>
 * <pre>
 * <code>
 * java -DWS=$WS \
 *      -Dprefix.1=org.apache.derby.iapi.types \
 *      org.apache.derby.iapi.services.cache.ClassSizeCrawler \
 *        org.apache.derby.iapi.types.DataValueDescriptor \
 *        java.math.BigDecimal \
 *        org.apache.derby.impl.services.cache.Generic.CachedItem
 *</code>
 *</pre>
 */
public class ClassSizeCrawler
{
    public static void main( String[] arg)
    {
        String[] classAndInterfaceList = {"org.apache.derby.iapi.types.DataValueDescriptor"};
        if(arg.length > 0)
            classAndInterfaceList = arg;
        Class[] interfaceList = new Class[classAndInterfaceList.length];
        int interfaceCount = 0;
        Class[] classList = new Class[classAndInterfaceList.length];
        int classCount = 0;

        Class classSizeClass = ClassSize.class; // Make sure that the garbage collector does not unload it
        ClassSize.setDummyCatalog();
        /* Most of the classes we will catalog invoke ClassSize.estimateBaseFromCatalog in
         * their static initializer. This dummy the catalog out so that this will not generate
         * errors. We will not actually use the classes, just examine their fields.
         */

        for( int i = 0; i < classAndInterfaceList.length; i++)
        {
            Class cls = null;
            try
            {
                cls = Class.forName( classAndInterfaceList[i]);
            }
            catch( ClassNotFoundException cnfe)
            {
                System.err.println( "*** Could not find class " + classAndInterfaceList[i]);
                System.exit(1);
            }
            if( cls.isInterface())
                interfaceList[ interfaceCount++] = cls;
            else
                classList[ classCount++] = cls;
        }

        String WS = System.getProperty( "WS");
        if( WS == null)
        {
            System.err.println( "*** WS is not set.");
            System.exit(1);
        }

        StringBuffer baseDir = new StringBuffer( System.getProperty( "classDir", ""));
        if( baseDir.length() == 0)
        {
            baseDir.append( WS);
            baseDir.append( '/');
            baseDir.append( "classes");
        }
        int baseDirLength = baseDir.length();

        StringBuffer packagePrefix = new StringBuffer( );

        Hashtable classSizes = new Hashtable();

        ClassSizeCrawler crawler = new ClassSizeCrawler(interfaceList, interfaceCount, classSizes);

        if( interfaceCount > 0)
        {
            boolean gotPrefix = false;
            // Crawl through the class hierarchies for classes implementing the interfaces
            for( Enumeration e = System.getProperties().propertyNames();
                 e.hasMoreElements();)
            {
                String propertyName = (String) e.nextElement();
                if( propertyName.equals( "prefix") || propertyName.startsWith( "prefix."))
                {
                    gotPrefix = true;
                    packagePrefix.setLength( 0);
                    packagePrefix.append( System.getProperty( propertyName));
                    baseDir.setLength( baseDirLength);
                    if( packagePrefix.length() > 0)
                    {
                        baseDir.append( '/');
                        for( int offset = 0; offset < packagePrefix.length(); offset++)
                        {
                            char c = packagePrefix.charAt( offset);
                            if( c == '.')
                                baseDir.append( '/');
                            else
                                baseDir.append( c);
                        }
                    }
                    crawler.crawl( new File( baseDir.toString()), packagePrefix);
                }
            }
            if( ! gotPrefix)
            {
                System.err.println( "*** Could not search the class hierarchy because no starting");
                System.err.println( "    prefixes where specified.");
                System.exit(1);
            }
        }
        for( int i = 0; i < classCount; i++)
            crawler.addClass( classList[i]);

        baseDir.setLength( baseDirLength);
        String outputFileName =
          System.getProperty( "out", WS + "/java/org.apache.derby.iapi.services.cache.ClassSizeCatalog.java");
        try
        {
            Calendar cal = Calendar.getInstance();
            cal.setTime( new Date());
            int year = cal.get( Calendar.YEAR);
            PrintWriter out = new PrintWriter( new FileWriter( outputFileName));
            out.print( "/*\n\n" +
                       "    Copyright " + year + " The Apache Software Foundation or its licensors, as applicable.\n" +
                       "    Licensed under the Apache License, Version 2.0 (the \"License\").\n" +
                       " */\n");
            out.print( "package org.apache.derby.iapi.services.cache;\n" +
                       "import java.util.Hashtable;\n" +
                       "class ClassSizeCatalog extends java.util.Hashtable\n" +
                       "{\n" +
                       "    ClassSizeCatalog()\n" +
                       "    {\n");
            for( Enumeration e = classSizes.keys();
                 e.hasMoreElements();)
            {
                String className = (String) e.nextElement();
                int[] coeff = (int[]) classSizes.get( className);
                out.print( "        put( \"" + className + "\", new int[]{" + coeff[0] + "," + coeff[1] + "});\n");
            }
            out.print("    }\n" +
                      "}\n");
            out.flush();
            out.close();
        }
        catch( IOException ioe)
        {
            System.err.println( "*** Cannot write to " + outputFileName);
            System.err.println( "   " + ioe.getMessage());
            System.exit(1);
        }
    } // end of main

    private Class[] interfaceList; // Search for classes that implement these interfaces
    private int interfaceCount;
    private Hashtable classSizes;
    private boolean verbose = false;

    private ClassSizeCrawler( Class[] interfaceList,
                              int interfaceCount,
                              Hashtable classSizes)
    {
        this.interfaceList = interfaceList;
        this.classSizes = classSizes;
        this.interfaceCount = interfaceCount;
        verbose = new Boolean( System.getProperty( "verbose", "false")).booleanValue();
    }

    private void crawl( File curDir, StringBuffer className)
    {
        if( verbose)
            System.out.println( "Searching directory " + curDir.getPath());

        try
        {
            if( ! curDir.isDirectory())
            {
                System.err.println( "*** " + curDir.getPath() + " is not a directory.");
                System.exit(1);
            }
        }
        catch( SecurityException se)
        {
            System.err.println( "Cannot access " + curDir.getPath());
            System.exit(1);
        }
        String[] filenames = curDir.list( );
        if( className.length() != 0)
            className.append( ".");

        int classNameLength = className.length();
        for( int fileIdx = 0; fileIdx < filenames.length; fileIdx++)
        {
            if( filenames[fileIdx].endsWith( ".class"))
            {
                // Strip off the ".class" suffix
                String s = filenames[fileIdx].substring( 0, filenames[fileIdx].length() - 6);
                className.append( s);
                Class targetClass = null;
                String targetClassName = className.toString();
                try
                {
                    targetClass = Class.forName( targetClassName);
                    if( !targetClass.isInterface())
                    {
                        for( int interfaceIdx = 0; interfaceIdx < interfaceCount; interfaceIdx++)
                        {
                            if( interfaceList[interfaceIdx].isAssignableFrom( targetClass))
                                addClass( targetClass);
                        }
                    }
                }
                catch( ClassNotFoundException cnfe)
                {
                    System.err.println( "Could not find class " + targetClassName);
                    System.exit(1);
                }
                catch( Throwable t){}
                className.setLength( classNameLength);
            }
            else
            {
                File nextDir = new File( curDir, filenames[fileIdx]);
                if( nextDir.isDirectory())
                {
                    className.append( filenames[fileIdx]);
                    crawl( nextDir, className);
                    className.setLength( classNameLength);
                }
            }
        }
    } // end of crawl

    private void addClass( Class targetClass)
    {
        int[] coefficients = ClassSize.getSizeCoefficients( targetClass);
        if( verbose)
            System.out.println( targetClass.getName() + " " + coefficients[0] + ", " + coefficients[1]);
        classSizes.put( targetClass.getName(), coefficients);
    } // end of addClass
} // end of ClassSizeCrawler
