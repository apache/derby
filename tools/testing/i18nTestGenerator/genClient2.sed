# For those expressions that start with "new MessageId", pre-append
# a try block and create a SqlException to complete the expression
/^[[:space:]]*new MessageId/i\
    try { \
       testException = new SqlException(null,

# For those expressions that start with "new SqlException", pre-append
# a try block
/new[[:space:]]*SqlException/i\
    try {

# At the end of each statement, check and print out an error
# if there is a problem
/;/a \
    } catch ( Throwable t ) { \
      // We can get this on an assertion failure \
      t.printStackTrace(); \
    } \
    if ( testException.getMessage().startsWith("UNKNOWN") )  {\
      testException.printStackTrace(); \
      System.err.println("FAILURE: message id was not found"); \
    }

#
# Add substitution for various message parameters where you replace
# the variable with a string containing the variable.  This prevents
# compile errors saying "symbol not found"
#
s/fileName/"fileName"/g
s/e.getMessage()/"e.getMessage"/g
s/Configuration.packageNameForDNC/"Configuration.packageNameForDNC"/g
s/Configuration.dncDriverName/"Configuration.dncDriverName"/g
s/[^\.]packageNameForDNC/"packageNameForDNC"/g
s/source,/"source",/g
s/Types.getTypeString([^)]*)/"targetType"/g
s/encoding/"encoding"/g
s/source.getClass().getName()/"source"/g
s/e.getException()/new Exception("foo")/g
s/[[:space:]]e);/testException);/
s/e.getClass().getName()/"exceptionClassName"/g
s/[[:space:]]sourceType/"sourceType"/g
s/[[:space:]]targetType/ "targetType"/g
s/[[:space:]]columnName/ "columnName"/g
s/[[:space:]]charsetName/ "charsetName"/g

#
# Subsitute Long and Integer params with 0 as a default
#
s/new Long[[:space:]]*([^)]*/new Long(0/g
s/new Integer[[:space:]]*([^)]*/new Integer(0/g

# Get rid of logWriter
s/new SqlException[[:space:]]*(.*,/new SqlException(null,/g

# Don't throw, just assign
s/throw new/testException = new/g

# There are some odd situations where there is one too many parens
s/)))/))/g


