/*

   Derby - Class org.apache.derby.optional.api.LuceneUtils

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.optional.api;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.util.Version;

import org.apache.derby.shared.common.error.PublicAPI;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.optional.utils.ToolUtilities;

/**
 * <p>
 * Utility methods for the Lucene optional tool.
 * </p>
 */
public abstract class LuceneUtils
{
    /////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////

    public  static  final   String  TEXT_FIELD_NAME = "luceneTextField";

    /////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////

    /** Map of Analyzers keyed by language code */
    private static  HashMap<String,Class<? extends Analyzer>>   _analyzerClasses;
    static
    {
        _analyzerClasses = new HashMap<String,Class<? extends Analyzer>>();

        storeAnalyzerClass( org.apache.lucene.analysis.ar.ArabicAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.hy.ArmenianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.eu.BasqueAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.br.BrazilianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.bg.BulgarianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.ca.CatalanAnalyzer.class );
        // deprecated, use StandardAnalyzer instead: storeAnalyzerClass( org.apache.lucene.analysis.cn.ChineseAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.cz.CzechAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.da.DanishAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.nl.DutchAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.en.EnglishAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.fi.FinnishAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.fr.FrenchAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.gl.GalicianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.de.GermanAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.el.GreekAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.hi.HindiAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.hu.HungarianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.id.IndonesianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.ga.IrishAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.it.ItalianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.lv.LatvianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.no.NorwegianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.fa.PersianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.pt.PortugueseAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.ro.RomanianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.ru.RussianAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.es.SpanishAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.sv.SwedishAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.th.ThaiAnalyzer.class );
        storeAnalyzerClass( org.apache.lucene.analysis.tr.TurkishAnalyzer.class );
    }

    /////////////////////////////////////////////////////////////////
    //
    //  PUBLIC BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////

    /** Get the version of the Lucene library on the classpath. */
    @SuppressWarnings("deprecation")
    public  static  Version currentVersion()
    {
        Version retval = null;

        // the current version is the highest one
        for ( Version current : Version.values() )
        {
            if ( current == Version.LUCENE_CURRENT ) { continue; }
            
            if ( retval == null ) { retval = current; }
            else
            {
                if ( current.onOrAfter( retval ) ) { retval = current; }
            }
        }
        
        return retval;
    }

    /**
     * <p>
     * Get the default Analyzer associated with the database Locale.
     * </p>
     */
    public  static  Analyzer    defaultAnalyzer()
        throws SQLException
    {
        return getAnalyzerForLocale( ConnectionUtil.getCurrentLCC().getDatabase().getLocale() );
    }
    
    /**
     * <p>
     * Get the Analyzer associated with the given Locale.
     * </p>
     */
    public  static  Analyzer    getAnalyzerForLocale( Locale locale )
        throws SQLException
    {
        String          language = locale.getLanguage();

        try {
            Class<? extends Analyzer>   analyzerClass = _analyzerClasses.get( language );
        
            if ( analyzerClass == null )    { return standardAnalyzer(); }
            else
            {
                Constructor<? extends Analyzer> constructor = analyzerClass.getConstructor( Version.class );

                return constructor.newInstance( currentVersion() );
            }
        }
        catch (IllegalAccessException iae) { throw ToolUtilities.wrap( iae ); }
        catch (InstantiationException ie)   { throw ToolUtilities.wrap( ie ); }
        catch (InvocationTargetException ite)   { throw ToolUtilities.wrap( ite ); }
        catch (NoSuchMethodException nsme)  { throw ToolUtilities.wrap( nsme ); }
    }

    /**
     * <p>
     * Get the StandardAnalyzer for parsing text.
     * </p>
     */
    public  static  Analyzer    standardAnalyzer()
    {
        return new StandardAnalyzer( currentVersion() );
    }
    
    /**
     * <p>
     * Get the default, classic QueryParser.
     * </p>
     */
    public  static  QueryParser defaultQueryParser
        (
         Version version,
         String[] fieldNames,
         Analyzer analyzer
         )
    {
        return new MultiFieldQueryParser( version, fieldNames, analyzer );
    }
    
    /**
     * <p>
     * Get the default index descriptor. This has a single field named TEXT,
     * a defaultAnalyzer() and a defaultQueryParser().
     * </p>
     */
    public  static  LuceneIndexDescriptor   defaultIndexDescriptor()
    {
        return new DefaultIndexDescriptor();
    }
    
    /////////////////////////////////////////////////////////////////
    //
    //  MINIONS
    //
    /////////////////////////////////////////////////////////////////

    /** Store an Analyzer class in the HashMap of Analyzers, keyed by language code */
    private static  void    storeAnalyzerClass( Class<? extends Analyzer> analyzerClass )
    {
        _analyzerClasses.put( getLanguageCode( analyzerClass ), analyzerClass );
    }

    /**
     * <p>
     * Get the language code for a Lucene Analyzer. Each of the Analyzers
     * lives in a package whose last leg is the language code.
     * </p>
     */
    private static  String  getLanguageCode( Class<? extends Analyzer> analyzerClass )
    {
        String  className = analyzerClass.getName();
        String  packageName = className.substring( 0, className.lastIndexOf( "." ) );
        String  languageCode = packageName.substring( packageName.lastIndexOf( "." ) + 1, packageName.length() );

        return languageCode;
    }

    /////////////////////////////////////////////////////////////////
    //
    //  NESTED CLASSES
    //
    /////////////////////////////////////////////////////////////////

    /** The default LuceneIndexDescriptor */
    public  static  class   DefaultIndexDescriptor  implements LuceneIndexDescriptor
    {
        public  DefaultIndexDescriptor()    {}

        /** Return the default array of field names { TEXT_FIELD_NAME }. */
        public  String[]    getFieldNames() { return new String[] { TEXT_FIELD_NAME }; }

        /** Return LuceneUtils.defaultAnalyzer() */
        public Analyzer getAnalyzer()   throws SQLException
        { return LuceneUtils.defaultAnalyzer(); }

        /**
         * Return LuceneUtils.defaultQueryParser(  LuceneUtils.currentVersion(), getFieldNames(), getAnalyzer() ).
         */
        public  QueryParser getQueryParser()
            throws SQLException
        {
            return LuceneUtils.defaultQueryParser
                (
                 LuceneUtils.currentVersion(),
                 getFieldNames(),
                 getAnalyzer()
                 );
        }
    }
    
}
