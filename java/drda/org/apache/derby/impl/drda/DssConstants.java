/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.drda
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */


package org.apache.derby.impl.drda;

/**
	This class defines DSS constants that are shared in the classes implementing
	the DRDA protocol.
*/
class DssConstants
{
	/**
		IBM Copyright &copy notice.
	*/

  private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;

  protected static final int MAX_DSS_LENGTH = 32767;

  // Registered DSS identifier indicating DDM data (xD0 for DDM data).
  protected static final int DSS_ID = 0xD0;

  // DSS chaining bit.
  protected static final int DSSCHAIN = 0x40;

  // DSS chaining bit for continuation on error
  protected static final int DSSCHAIN_ERROR_CONTINUE = 0x20;

  // DSS chaining bit where next DSS has same correlation ID.
  protected static final int DSSCHAIN_SAME_ID = 0x10;

  // DSS formatter for an OBJDSS.
  protected static final int DSSFMT_OBJDSS = 0x03;

  // DSS formatter for an RPYDSS.
  protected static final int DSSFMT_RPYDSS = 0x02;

  // DSSformatter for an RQSDSS.
  protected static final int DSSFMT_RQSDSS = 0x01;

  // DSS request correlation id unknown value
  protected static final int CORRELATION_ID_UNKNOWN = -1;

  // DSS length continuation bit
  protected static final int CONTINUATION_BIT = 0x8000;

 // Registered SNA GDS identifier indicating DDM data (xD0 for DDM data).
  static final int GDS_ID = 0xD0;

  // GDS chaining bits.
  static final int GDSCHAIN = 0x40;

  // GDS chaining bits where next DSS has different correlation ID.
  static final int GDSCHAIN_SAME_ID = 0x10;

  // GDS formatter for an OBJDSS.
  static final int GDSFMT_OBJDSS = 0x03;

  // GDS formatter for an RPYDSS.
  static final int GDSFMT_RPYDSS = 0x02;

  // GDS formatter for an RQSDSS.
  static final int GDSFMT_RQSDSS = 0x01;

  // hide the default constructor
  private DssConstants () {}
}
