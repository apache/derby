/* IBM Confidential
 *
 * Product ID: 5697-F53
 *

 * Copyright 1998, 2004.WESTHAM

 *
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office.
 */

package org.apache.derby.iapi.services.info;

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;


/**
  Class to hold a cloudscape Product version.

  This class includes the following product version features.

  <OL>
  <LI>Save the product version information this holds as a String. We call
      the string a 'product version string'.
  <LI>Construct a ProductVersionHolder from a valid 'product version string'.
  <LI>Determine if two product versions are feature compatible. This means
      products of these versions may interoperate with ***NO*** compatibility
	  problems.
  <LI>Determine if two product versions are the same. This is a stronger
      test than the test for feature compatibility.
  </OL>



  Cloudscape 5.1 and older versions used the majorVersion, minorVersion, maintVersion versions
  directly. That is a three part version number, majorVersion.minorVersion.maintVersion, e.g. 5.1.21.

  For Cloudscape 5.2 onwards a four part name is required.
	majorVersion.minorVersion.fixPack.bugVersion e.g. 5.2.1.2

	This follows the IBM standard and allows us to state that a fix pack will be 5.2.3 without worrying
	about how many maintence fixes there are between fix packs.

	We implement this using the existing format of ProductVersionHolder to reduce disruption to the
	code, however we make the maintVersion encode the {fixPack.bugVersion}. Since the maintVersion
	is represented by a int (2G values) we have plenty of room for encoding. If we assign a given
	majorVersion.minorVersion.fixPack a 10 year life, then we about the maximum number of individual releases
	it can have is 10 years * 365 days/year = 3650. Thus with the pre 5.2 scheme we would not expect a 
	5.1.x to have an x > 3650 (approximately). Usually the rate of point releases has been much less than
	one per day, 5.1.31 is released about 225 days after GA which makes around a point release every 7 days.
	But in the encoding we need to be conservative. With fix packs the maximum is about 2 per year and fix
	packs are only made to the current release, thus with a yearly minor release cycle we would imagine
	only 2 fixpacks per major.minor. However like other IBM products or release cycle may be extended thus
	we can expect up to a handful of fix packs.

	Thus we might imagine releases like

	5.2.0.12
	5.2.0.234
	5.2.1.34
	5.2.4.2445

  but highly unlikey to have

	5.2.2.59321
	5.2.23.1
	

    The encoding number must continue to increase so that the
	
		encodedMaintB > encodedMaintA

		if (fixPackB > fixPackA) || ((fixPackB == fixPackA) && (bugB > bugA))


	Selected encoding

	encodedMaint = (fixPack * 1,000,000) + (bugVersion);

	Handles many many fixpacks and upto one million bug fixes per fix pack and remains somewhat human readable.

	Special fix packs

	fixpack == 0 = alpha (version off main codeline)
	fixpack == 1 = first release of major.minor (may be marked with beta)
	fixpack == 2 = first fix pack (displayed as 1)
     

	The drdaMaintVersion is sent in the Network Server PRDID. It never displays
    but may be used by the client for version specific behaviour. It should be 
	reset to 0 with each minor release.	

  The product version string has the form:
  <PRE>
  productVendorName - ProductName - majorVersion.minorVersion.maintVersion [beta] - (buildNumber)

  </PRE>

  */
public final class ProductVersionHolder implements java.security.PrivilegedAction
{

	//
	//Used as an invalid value for numbers. This works because all
	//the numbers in a product version must be non-negative.
	private static final int BAD_NUMBER = -1;
	private static final String ALPHA = "alpha";
	private static final String BETA = "beta";

	private final static int	MAINT_ENCODING = 1000000;
	
	private String productVendorName;
	private String productName;
	private String productTechnologyName;
	private int majorVersion = BAD_NUMBER;
	private int minorVersion = BAD_NUMBER;
	private int maintVersion = BAD_NUMBER;
	private int drdaMaintVersion = BAD_NUMBER;
	private String buildNumber = "????";
	private Boolean isBeta;

	private ProductVersionHolder() {
	}
	
	/**
	  Create a ProductVersionHolder

	  <P>Please see the documentation for the varient of getProductVesionHolder
	  that takes the same parameters as this for a description of the parameters.
	  */
	private ProductVersionHolder(String productVendorName,
								 String productName,
								 String productTechnologyName,
								 int majorVersion,
								 int minorVersion,
								 int maintVersion,
								 int drdaMaintVersion,
								 String buildNumber,
								 Boolean isBeta)
	{
		if (productVendorName != null)
			this.productVendorName = productVendorName.trim();
		if (productName != null)
			this.productName = productName.trim();
		if (productTechnologyName != null)
			this.productTechnologyName = productTechnologyName.trim();
		this.majorVersion = majorVersion;
		this.minorVersion = minorVersion;
		this.maintVersion = maintVersion;
		this.drdaMaintVersion = drdaMaintVersion;
		this.buildNumber = buildNumber;
		this.isBeta = isBeta;
	}

	/**
	  Create a valid ProductVersionHolder. If any of the
	  parameters provided is invalid, this returns null.
	  @param productName The name of the product. productName.length()
	  must be greater than 0. The syntax for a product name is
	  'productGenus[:productSpecies]'. 
	  @param majorVersion The most significant portion of a 3 
	  part product version.  Must be non-negative.
	  @param minorVersion The second portion of a 3 part 
	  product version. Must be non-negative.
	  @param maintVersion The least significant portion of a 3 part
	  product version. Must be non-negative.
	  @param drdaMaintVersion The protocol modification number for minor release.
	  @param buildNumber The buildNumber for a product. 
	  @param isBeta true iff the product is beta.
	  @return A valid ProductVersionHolder of null if any of the parameters
	  provided are not valid.
	  */
	public static ProductVersionHolder
	getProductVersionHolder(
						   String productVendorName,
						   String productName,
						   String productTechnologyName,
						   int majorVersion,
						   int minorVersion,
						   int maintVersion,
						   int drdaMaintVersion,
						   String buildNumber,
						   Boolean isBeta)
	{
		ProductVersionHolder pvh =
			new ProductVersionHolder(productVendorName,
									 productName,
									 productTechnologyName,
									 majorVersion,
									 minorVersion,
									 maintVersion,
									 drdaMaintVersion,
									 buildNumber,
									 isBeta);
		return pvh;
	}
	
	/**
	  Get a ProductVersionHolder for a product of a given genus,
	  that is available in the caller's environment. 
	  Even though this uses a priv bock, it may stil fail when
	  the jar the version is being fetched from, is different to the
	  one that loaded this class, AND the jars are in different security contexts.

	  @param productGenus The genus for the product.
	  @return The ProductVersionHolder or null if
	  a product with the given genus is not available in the
	  caller's environment.
	  */
	public static ProductVersionHolder
	getProductVersionHolderFromMyEnv(String productGenus)
	{

		ProductVersionHolder tempPVH = new ProductVersionHolder();

		tempPVH.productGenus = productGenus;
		Properties p = (Properties) java.security.AccessController.doPrivileged(tempPVH);

		if (p == null)
			return null;

		return getProductVersionHolder(p);
	}


	/**
		Load the version info from the already opened properties files.
		We need to do this because if the jar files (e.g. db2jtools and db2j)
		are in different security contexts (entries in the policy files) then
		we cannot load the version information for one of them correctly.
		This is because the this class will either have been loaded from
		only one of the jars and hence can only access the resource in its own jar.
		By making code specific to the jar open the resource we are guaranteed it will work.
	*/
	public static ProductVersionHolder
	getProductVersionHolderFromMyEnv(InputStream propertiesStream)
	{

		if (propertiesStream == null)
			return null;

		Properties p = new Properties();
		try {
			p.load(propertiesStream);
		}
		catch (IOException ioe) {

			System.out.println("IOE " + ioe.getMessage());
			//
			//This case is a bit ugly. If we get an IOException, we return
			//null. Though this correctly reflects that the product is not
			//available for use, it may be confusing to users that we swallow
			//the IO error here.
			return null;
		} finally {
			try {
				propertiesStream.close();
			} catch (IOException ioe2) {
			}
		}

		return getProductVersionHolder(p);
	}

	/**
	  Get a ProductVersionHolder based on the information in
	  the Properties object provided.

	  @param p The properties object that holds the productVersion
	  information.
	  @return The ProductVersionHolder or null if
	  a product with the given genus is not available in the
	  caller's environment.
	  */
	public static ProductVersionHolder
	getProductVersionHolder(Properties p)
	{
		String pvn = p.getProperty(PropertyNames.PRODUCT_VENDOR_NAME);
		String pn = p.getProperty(PropertyNames.PRODUCT_EXTERNAL_NAME);
		String ptn = p.getProperty(PropertyNames.PRODUCT_TECHNOLOGY_NAME);
		int v1 = parseInt(p.getProperty(PropertyNames.PRODUCT_MAJOR_VERSION));
		int v2 = parseInt(p.getProperty(PropertyNames.PRODUCT_MINOR_VERSION));
		int v3 = parseInt(p.getProperty(PropertyNames.PRODUCT_MAINT_VERSION));
		int v4 = parseInt(p.getProperty(PropertyNames.PRODUCT_DRDA_MAINT_VERSION));
		String bn = p.getProperty(PropertyNames.PRODUCT_BUILD_NUMBER);
		Boolean isBeta =
			Boolean.valueOf(p.getProperty(PropertyNames.PRODUCT_BETA_VERSION));
		return 	getProductVersionHolder(pvn,pn,ptn,v1,v2,v3,v4,bn,isBeta);
	}


	/**
	  Return the product vendor name.
	  */
	public String getProductVendorName()
	{
		return productVendorName;
	}


	/**
	  Return the external product name.
	  */
	public String getProductName()
	{
		return productName;
	}
	public String getProductTechnologyName()
	{
		return productTechnologyName;
	}

	/**
	  Return the major version number.
	  */
	public int getMajorVersion() {return majorVersion;}
	/**
	  Return the minor version number.
	  */
	public int getMinorVersion() {return minorVersion;}
	/**
	  Return the <B>encoded</B> maintainence version number.
	  */
	public int getMaintVersion() {return maintVersion;}

	/** 
		Return the drda protocol maintenance version for this minor release.
		Starts at 0 for each minor release and only incremented 
		when client behaviour changes based on the server version.
	**/
	public int getDrdaMaintVersion() {return drdaMaintVersion; }

	/**
		Return the fix pack version from the maintence encoding.
	*/
	public int getFixPackVersion() { return maintVersion / MAINT_ENCODING; }


	/**
	  Return true if this is a beta product.
	  */
	public boolean isBeta() {return isBeta.booleanValue();}
	/**
	  Return true if this is a alpha product.
	  */
	public boolean isAlpha() {
		return	   (majorVersion >= 5)
				&& (minorVersion > 2)
				&& ((maintVersion / MAINT_ENCODING) == 0);
	}
	/**
	  Return the build number for this product.
	  */
	public String getBuildNumber() {return buildNumber;}

    /**
     * Return the build number as an integer if possible,
     * mapping from the SVN number.
     * nnnnn -> returns nnnnn
     * nnnnnM -> returns -nnnnn indicates a modified code base
     * nnnnn:mmmmm -> returns -nnnnn
     * anything else -> returns -1
    */
    public int getBuildNumberAsInt(){
    	if (buildNumber == null)
    	    return -1;
    	boolean dubiousCode = false;
    	int offset = buildNumber.indexOf('M');
    	if (offset == -1)
    	    offset = buildNumber.indexOf(':');
    	else
    	    dubiousCode = true;
    	if (offset == -1)
    		offset = buildNumber.length();
        else
            dubiousCode = true;
    	
    	try {
    		int bnai = Integer.parseInt(buildNumber.substring(0, offset));
    		if (dubiousCode)
    		    bnai = -bnai;
    		return bnai;
    	} catch (NumberFormatException nfe) 
     	{
     		return -1;
    	}
    }

	/**
	  Parse a string containing a non-negative integer. Return
	  a negative integer is the String is invalid.

	  @param s A string with a non-negative integer (a sequence
	  of decimal digits.)
	  @return the integer or a negative number if s is invalid.
	  */
	private static int parseInt(String s)
	{
		//System.out.println("Parsing integer: "+s);		
		int result = BAD_NUMBER;
		try
			{
				if (s!=null)
					result = Integer.parseInt(s);
			}
		catch (NumberFormatException nfe)
			{}

		if (result < 0) result = BAD_NUMBER;
		return result;
	}

	/**
	  Return  a string representation of this ProductVersion. The
	  difference between this and createProductVersionString, is
	  that this method retruns a String when this ProductVersionHolder
	  holds invalid version information.
	 */
	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(getProductVendorName());
		sb.append(" - ");
		sb.append(getProductName());
		sb.append(" - ");
		sb.append(getVersionBuildString(true));
		return sb.toString();
	}

	/**
		Return the feature version string, ie. major.minor. (e.g. 5.2)
	*/
	public String getSimpleVersionString() {

		return ProductVersionHolder.simpleVersionString(majorVersion, minorVersion, isBeta());
	}

	/**
		Convert a major and minor number with beta status into a string.
	*/
	public static String simpleVersionString(int major, int minor, boolean isBeta) {

		StringBuffer sb = new StringBuffer();

		sb.append(major);
		sb.append('.');
		sb.append(minor);
		if (isBeta) {
			sb.append(' ');
			sb.append(BETA);
		}

		return sb.toString();
	}
	public static String fullVersionString(int major, int minor, int maint, boolean isBeta, String build) {
		StringBuffer sb = new StringBuffer();
		sb.append(major);
		sb.append('.');
		sb.append(minor);
		sb.append('.');

		String preRelease = null;
		if (major == 5 && minor <= 2 && maint < MAINT_ENCODING)
		{
			sb.append(maint);
			if (isBeta)
				preRelease = BETA;
		}
		else
		{
			int fixPack = maint / MAINT_ENCODING;
			int bugVersion = maint % MAINT_ENCODING;
			sb.append(fixPack);
			sb.append('.');
			sb.append(bugVersion);

			if (fixPack == 0)
			{
				preRelease = ALPHA;
			}
			else if (isBeta) {
				preRelease = BETA;
			}
		}

        if (preRelease != null)
        {
			sb.append(' ');
            sb.append(preRelease);
        }
		if (build != null) {
			sb.append(" - (");

			sb.append(build);
			sb.append(')');
		}
        return sb.toString();
	}
	/**
		Returns a short-hand value for the product version string.
		Used by Sysinfo.
		Includes the optional <beta> designation
	*/
    public String getVersionBuildString(boolean withBuild)
    {
		return ProductVersionHolder.fullVersionString(majorVersion, minorVersion, maintVersion, isBeta(),
			withBuild ? buildNumber : null);
    }

	/*
	** Security related methods 
	*/
	private String productGenus;
	public Object run() {

		// SECURITY PERMISSION - IP4
		return loadProperties(this.productGenus);
	}
	// SECURITY PERMISSION - IP4
	private Properties loadProperties(String productGenus) {
		String resourceName = "/org/apache/derby/info/" + productGenus+".properties";
			
		InputStream is = getClass().getResourceAsStream(resourceName);
		if (is==null) {
			return null;
		}

		Properties p = new Properties();
		try {
			p.load(is);
			return p;
		}
		catch (IOException ioe) {
			//
			//This case is a bit ugly. If we get an IOException, we return
			//null. Though this correctly reflects that the product is not
			//available for use, it may be confusing to users that we swallow
			//the IO error here.
			return null;
		}
	}
}
