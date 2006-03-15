s/fileName/"fileName"/g
s/e.getMessage()/"e.getMessage"/g
s/new Long([^)]*/new Long(0/g
s/new Integer([^)]*/new Integer(0/g
s/)))/))/g
/new MessageId/i\
    e = new SqlException(null,
/[^(]);/a \
    if ( e.getMessage().startsWith("UNKNOWN") ) \
      throw e; \
