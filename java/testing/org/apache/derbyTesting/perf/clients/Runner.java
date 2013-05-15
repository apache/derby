/*

Derby - Class org.apache.derbyTesting.perf.clients.Runner

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

package org.apache.derbyTesting.perf.clients;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;

/**
 * Class used for running a performance test from the command line. To learn
 * how to run the tests, invoke this command:
 * <pre>
 * java org.apache.derbyTesting.perf.clients.Runner
 * </pre>
 */
public class Runner {

    private static final String DERBY_EMBEDDED_DRIVER =
            "org.apache.derby.jdbc.EmbeddedDriver";

    private static final String DEFAULT_URL = "jdbc:derby:db;create=true";

    /** The JDBC driver class to use in the test. */
    private static String driver = DERBY_EMBEDDED_DRIVER;
    /** The JDBC connection URL to use in the test. */
    private static String url = DEFAULT_URL;
    /** Username for connecting to the database. */
    private static String user = "test";
    /** Password for connecting to the database. */
    private static String password = "test";
    /**
     * Flag which tells whether the data needed by this test should be
     * (re)created.
     */
    private static boolean init = false;
    /** The name of the type of load to use in the test. */
    private static String load; // required argument
    /** Map containing load-specific options. */
    private final static HashMap<String, String> loadOpts =
            new HashMap<String, String>();
    /** The name of the load generator to use in the test. */
    private static String generator = "b2b";
    /** The number of client threads to use in the test. */
    private static int threads = 1;
    /**
     * The number of requests to issue to the database per second (for the
     * load generators that take that as an argument).
     */
    private static int requestsPerSecond = 100;
    /** The number of seconds to spend in the warmup phase. */
    private static int warmupSec = 30;
    /** The number of seconds to collect results. */
    private static int steadySec = 60;

    /**
     * Main method which starts the Runner application.
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        try {
            parseArgs(args);
        } catch (Exception e) {
            System.err.println(e);
            printUsage(System.err);
            System.exit(1);
        }

        Class.forName(driver).newInstance();

        if (init) {
            DBFiller filler = getDBFiller();
            Connection conn = DriverManager.getConnection(url, user, password);
            System.out.println("initializing database...");
            filler.fill(conn);
            conn.close();
        }

        Client[] clients = new Client[threads];
        for (int i = 0; i < clients.length; i++) {
            Connection c = DriverManager.getConnection(url, user, password);
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            clients[i] = newClient();
            clients[i].init(c);
        }

        LoadGenerator gen = getLoadGenerator();
        gen.init(clients);
        System.out.println("starting warmup...");
        gen.startWarmup();
        Thread.sleep(1000L * warmupSec);
        System.out.println("entering steady state...");
        gen.startSteadyState();
        Thread.sleep(1000L * steadySec);
        System.out.println("stopping threads...");
        gen.stop();
        gen.printReport(System.out);

        shutdownDatabase();
    }

    /**
     * Parse the command line arguments and set the state variables to
     * reflect the arguments.
     *
     * @param args the command line arguments
     */
    private static void parseArgs(String[] args) throws Exception {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-driver")) {
                driver = args[++i];
            } else if (args[i].equals("-url")) {
                url = args[++i];
            } else if (args[i].equals("-user")) {
                user = args[++i];
            } else if (args[i].equals("-pass")) {
                password = args[++i];
            } else if (args[i].equals("-init")) {
                init = true;
            } else if (args[i].equals("-load")) {
                load = args[++i];
            } else if (args[i].equals("-load_opts")) {
                parseLoadOpts(args[++i]);
            } else if (args[i].equals("-gen")) {
                generator = args[++i];
            } else if (args[i].equals("-threads")) {
                threads = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-rate")) {
                requestsPerSecond = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-wt")) {
                warmupSec = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-rt")) {
                steadySec = Integer.parseInt(args[++i]);
            } else {
                throw new Exception("invalid argument: " + args[i]);
            }
        }
        if (load == null) {
            throw new Exception("required parameter -load not specified");
        }
    }

    /**
     * Parse the load-specific options. It's a comma-separated list of options,
     * where each option is either a keyword or a (keyword, value) pair
     * separated by an equals sign (=). The parsed options will be put into the
     * map {@link #loadOpts}.
     *
     * @param optsString the comma-separated list of options
     */
    private static void parseLoadOpts(String optsString) {
        String[] opts = optsString.split(",");
        for (int i = 0; i < opts.length; i++) {
            String[] keyValue = opts[i].split("=", 2);
            if (keyValue.length == 2) {
                loadOpts.put(keyValue[0], keyValue[1]);
            } else {
                loadOpts.put(opts[i], null);
            }
        }
    }

    /**
     * Checks whether the specified option is set.
     *
     * @param option the name of the option
     * @return {@code true} if the option is set
     */
    private static boolean hasOption(String option) {
        return loadOpts.keySet().contains(option);
    }

    /**
     * Get the {@code int} value of the specified option.
     *
     * @param option the name of the option
     * @param defaultValue the value to return if the option is not set
     * @return the value of the option
     * @throws NumberFormatException if the value is not an {@code int}
     */
    static int getLoadOpt(String option, int defaultValue) {
        String val = (String) loadOpts.get(option);
        return val == null ? defaultValue : Integer.parseInt(val);
    }

    /** String to print when there are errors in the command line arguments. */
    private static final String USAGE =

"Valid parameters:\n" +
"  -driver: JDBC driver class, default: " + DERBY_EMBEDDED_DRIVER + "\n" +
"  -url: JDBC connection url, default: " +  DEFAULT_URL + "\n" +
"  -user: JDBC user name, default: test\n" +
"  -pass: JDBC user password, default: test\n" +
"  -init: initialize database (otherwise, reuse database)\n" +
"  -load: type of load, required argument, valid types:\n" +
"      * sr_select - single-record (primary key) select from table with\n" +
"                    100 000 rows. It accepts the following load-specific\n" +
"                    options (see also -load_opts):\n" +
"            - blob or clob: use BLOB or CLOB data instead of VARCHAR\n" +
"            - secondary: select on a column with a secondary (non-unique)\n" +
"              index instead of the primary key\n" +
"            - nonIndexed: select on a non-indexed column instead of the\n" +
"              primary key\n" +
"      * sr_update - single-record (primary key) update on table with\n" +
"                    100 000 rows. It accepts the same load-specific\n" +
"                    options as sr_select.\n" +
"      * sr_select_big - single-record (primary key) select from table with\n" +
"                    100 000 000 rows\n" +
"      * sr_update_big - single-record (primary key) update on table with\n" +
"                    100 000 000 rows\n" +
"      * sr_select_multi - single-record select from a random table\n" +
"                    (32 tables with a single row each)\n" +
"      * sr_update_multi - single-record update on a random table\n" +
"                    (32 tables with a single row each)\n" +
"      * index_join - join of two tables (using indexed columns)\n" +
"      * group_by - GROUP BY queries against TENKTUP1\n" +
"      * bank_tx - emulate simple bank transactions, similar to TPC-B. The\n" +
"                  following load-specific options are accepted:\n" +
"            - branches=NN: specifies the number of branches in the db\n" +
"                           (default: 1)\n" +
"            - tellersPerBranch=NN: specifies how many tellers each branch\n" +
"              in the database has (default: 10)\n" +
"            - accountsPerBranch=NN: specifies the number of accounts in\n" +
"              each branch (default: 100000)\n" +
"      * seq_gen - sequence generator concurrency. Accepts\n" +
"                    the following load-specific options (see also -load_opts):\n" +
"            - numberOfGenerators: number of sequences to create\n" +
"            - tablesPerGenerator: number of tables to create per sequence\n" +
"            - insertsPerTransaction: number of inserts to perform per transaction\n" +
"            - debugging: 1 means print debug chatter, 0 means do not print the chatter\n" +
"            - identityTest: 1 means do identity column testing, any other number \n" +
"                    means do sequence generator testing. If no identityTest is specified \n" +
"                    then sequence generator testing will be done by default \n" +
"  -load_opts: comma-separated list of load-specific options\n" +
"  -gen: load generator, default: b2b, valid types:\n" +
"      * b2b - clients perform operations back-to-back\n" +
"      * poisson - load is Poisson distributed\n" +
"  -threads: number of threads performing operations, default: 1\n" +
"  -rate: average number of transactions per second to inject when\n" +
"         load generator is \"poisson\", default: 100\n" +
"  -wt: warmup time in seconds, default: 30\n" +
"  -rt: time in seconds to collect results, default: 60";
    /**
     * Print the usage string.
     *
     * @param out the stream to print the usage string to
     */
    private static void printUsage(PrintStream out) {
        out.println(USAGE);
    }

    /**
     * Get the data type to be used for sr_select and sr_update types of load.
     *
     * @return one of the {@code java.sql.Types} data type constants
     */
    private static int getTextType() {
        boolean blob = hasOption("blob");
        boolean clob = hasOption("clob");
        if (blob && clob) {
            System.err.println("Cannot specify both 'blob' and 'clob'");
            printUsage(System.err);
            System.exit(1);
        }
        if (blob) {
            return Types.BLOB;
        }
        if (clob) {
            return Types.CLOB;
        }
        return Types.VARCHAR;
    }

    /**
     * Find the {@code DBFiller} instance for the load specified on the
     * command line.
     *
     * @return a {@code DBFiller} instance
     */
    private static DBFiller getDBFiller() {
        if (load.equals("sr_select") || load.equals("sr_update")) {
            return new SingleRecordFiller(100000, 1, getTextType(),
                                          hasOption("secondary"),
                                          hasOption("nonIndexed"));
        } else if (load.equals("sr_select_big") ||
                       load.equals("sr_update_big")) {
            return new SingleRecordFiller(100000000, 1);
        } else if (load.equals("sr_select_multi") ||
                       load.equals("sr_update_multi")) {
            return new SingleRecordFiller(1, 32);
        } else if (load.equals("index_join")) {
            return new WisconsinFiller();
        } else if (load.equals("group_by")) {
            return new WisconsinFiller(getLoadOpt("numRows", 10000));
        } else if (load.equals("bank_tx")) {
            return new BankAccountFiller(
                getLoadOpt("branches", 1),
                getLoadOpt("tellersPerBranch", 10),
                getLoadOpt("accountsPerBranch", 100000));
        } else if (load.equals("seq_gen")) {
            return new SequenceGeneratorConcurrency.Filler();
        }
        System.err.println("unknown load: " + load);
        printUsage(System.err);
        System.exit(1);
        return null;
    }

    /**
     * Create a new client for the load specified on the command line.
     *
     * @return a {@code Client} instance
     */
    private static Client newClient() {
        if (load.equals("sr_select")) {
            return new SingleRecordSelectClient(100000, 1, getTextType(),
                    hasOption("secondary"), hasOption("nonIndexed"));
        } else if (load.equals("sr_update")) {
            return new SingleRecordUpdateClient(100000, 1, getTextType(),
                    hasOption("secondary"), hasOption("nonIndexed"));
        } else if (load.equals("sr_select_big")) {
            return new SingleRecordSelectClient(100000000, 1);
        } else if (load.equals("sr_update_big")) {
            return new SingleRecordUpdateClient(100000000, 1);
        } else if (load.equals("sr_select_multi")) {
            return new SingleRecordSelectClient(1, 32);
        } else if (load.equals("sr_update_multi")) {
            return new SingleRecordUpdateClient(1, 32);
        } else if (load.equals("index_join")) {
            return new IndexJoinClient();
        } else if (load.equals("group_by")) {
            return new GroupByClient();
        } else if (load.equals("bank_tx")) {
            return new BankTransactionClient(
                getLoadOpt("branches", 1),
                getLoadOpt("tellersPerBranch", 10),
                getLoadOpt("accountsPerBranch", 100000));
        } else if (load.equals("seq_gen")) {
            return new SequenceGeneratorConcurrency.SGClient();
        }
        System.err.println("unknown load: " + load);
        printUsage(System.err);
        System.exit(1);
        return null;
    }

    /**
     * Create a load generator for the load specified on the command line.
     *
     * @return a {@code LoadGenerator} instance
     */
    private static LoadGenerator getLoadGenerator() {
        if (generator.equals("b2b")) {
            return new BackToBackLoadGenerator();
        } else if (generator.equals("poisson")) {
            double avgWaitTime = 1000d * threads / requestsPerSecond;
            return new PoissonLoadGenerator(avgWaitTime);
        }
        System.err.println("unknown load generator: " + generator);
        printUsage(System.err);
        System.exit(1);
        return null;
    }

    /**
     * Shut down the database if it is a Derby embedded database.
     */
    private static void shutdownDatabase() throws SQLException {
        if (driver.equals(DERBY_EMBEDDED_DRIVER)) {
            try {
                DriverManager.getConnection(url + ";shutdown=true");
                System.err.println("WARNING: Shutdown of database didn't " +
                                   "throw expected exception");
            } catch (SQLException e) {
                if (!"08006".equals(e.getSQLState())) {
                    System.err.println("WARNING: Shutdown of database threw " +
                                       "unexpected exception");
                    e.printStackTrace();
                }
            }
        }
    }
}
