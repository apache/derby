create function FMTUNICODE(P1 VARCHAR(100)) RETURNS VARCHAR(300)
EXTERNAL NAME 'org.apache.derbyTesting.functionTests.util.Formatters.format'
NO SQL
LANGUAGE JAVA PARAMETER STYLE JAVA;

 values FMTUNICODE(UCASE('i'));
 values FMTUNICODE(UCASE('I'));
 values FMTUNICODE(LCASE('i'));
 values FMTUNICODE(LCASE('I'));
