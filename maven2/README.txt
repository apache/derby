GENERATING MAVEN 2 ARTIFACTS FOR APACHE DERBY
=============================================

The POMs in the maven2 directory enable you to generate Maven 2 artifacts
for Apache Derby. The following software is required for deploying a release:
 1. Maven 2
 2. GnuPG (for signing the artifacts)
 3. ssh/scp (for site deployment)

Note that Maven 2 will pull down quite a few required plugins the first time
you run it. They will be cached locally, so they are not downloaded again the
next time.

All commands below are to be executed from the directory 'maven2' within the
Derby source code repository.

WARNING: The Maven repository is write-once. This means that you have only one
         chance to deploy artifacts with a given version string. Once they are
         deployed, you cannot overwrite them. The only way to deprecate a set
         of deployed artifacts is to deploy a new set of artifacts with a
         different version string.

WARNING: The Apache server has been configured to ban IP addresses if too many
         unsuccessful login attempts are made within a short time period. For
         this reason it is important that you test the SSH configuration before
         you run the Maven deploy step. The current Maven setup requires that
         issuing "ssh people.apache.org" logs you into the server without
         prompting for a password. See instructions below.
         If your IP gets banned, send a message to infrastructure at apache dot
         org. Include your IP address in the mail.

Short description of the required steps:

 a) Generate the Derby jar files.
    For releases, generate the insane jars. You can override which jars to use
    with the property 'sanity' in the top-level POM.
    The jars are expected to be found in 'jars/[in]sane' relative to the
    checked out code repository.

 b) Specify required information for one or all of the following sub-steps.
    To successfully generate and deploy release artifacts, all of these
    must be specified:

      - The Derby release version.
        The version must be specified in all POMs. Compile and execute the
        Java program SetDerbyVersion, found in the 'maven2' directory, i.e.:
            javac SetDerbyVersion && java -cp . SetDerbyVersion

        Alternatively, use search and replace (i.e. Perl or sed) - make sure
        you don't replace version tags that aren't supposed to be modified.
        Make sure you diff the POMs to verify the changes.
        Note that the Java program performs some extra sanity checks.

      - Passphrase for your GPG signing key.
        Required for step (c) and (d). See the top-level POM for details, brief
        instructions in (c).

      - User credentials for deployment.
        Required for step (d).  Several options for how to configure Maven seem
        to exist, but only one of them has been reported to work for most
        scenarios. If your system doesn't have executables called 'ssh' and
        'scp', then please figure out how to successfully specify alternative
        executables...

        The local username will be used when accessing the Apache server using
        the external SSH commands. If your local username isn't the same as
        your Apache username, you must configure SSH to use the correct
        username. On Unix systems, this is done by adding the following to
        '~/.ssh/config' (the host name pattern must match the host specified
        under the repository tag in the top-level POM):

        Host people.apache.org
            User your_apache_username

        Again, configuring Maven to use a different username should be
        possible, but attempts to do so have failed so far.

 c) 'mvn clean install'
    Generates the artifacts, uses GnuPG to generate signatures for the
    artifacts, and installs the artifacts in the local repository.
    You are required to provide your private key and the passphrase to GnuPG.
    Using a passphrase agent is recommended, but you can also specify it on
    the command line when invoking Maven with -Dgpg.passphrase=PASSPHRASE.

    For instance:

        mvn -Dgpg.passphrase="my secret passphrase" clean install

    WARNING: Do not specify your passphrase in the POM that is deployed on
             the Maven repositories!

    The local repository is typically found in '~/.m2/repository/', and the
    Derby artifacts are located under 'org/apache/derby/'.
    The clean target is included to avoid unintentionally installing/deploying
    artifacts not supposed to be deployed.
    If you just want to build the artifacts, use 'mvn package' or 'mvn verify'.
    The former will generate the artifact jars, the latter will additionally
    generate/include the POMs to be deployed and the signatures.

    NOTE: Do not run 'mvn package|verify install', that is to combine either
          package or verify with install, as this causes the
          signatures of the artifacts to be signed. This shows as files like
          './engine/target/derby-trunk-alpha.jar.asc.asc'.

 d) 'mvn deploy' or 'mvn clean deploy'
    Deploys the artifacts, including signatures and checksum files, to the
    Apache Maven 2 repository. The files will then be distributed to
    mirrors. Remember that you will need to specify your gpg
    passphrase here too.

    For instance:

        mvn -Dgpg.passphrase="my secret passphrase" deploy


    NOTE: This step has been reported to not work when using username and
    password authentication. Unless you prefer to deploy manually, use a
    public key to log into the remote host (people.apache.org).

    If your umask is set to something else than 0002 (the default is 0022),
    you should log into the Apache server and grant write permission to the
    group owner (which should be 'committers'). Alternatively, use this SSH command:

    ssh people.apache.org "find /www/people.apache.org/repo/m2-ibiblio-rsync-repository/org/apache/derby/ -user \$USER -exec chmod g+w {} \;"

    To verify the group ownership and permissions, run the two following SSH
    commands. If everything is set correctly, they should return no file names.

    ssh people.apache.org "find /www/people.apache.org/repo/m2-ibiblio-rsync-repository/org/apache/derby/ \! -group committers"
    ssh people.apache.org "find /www/people.apache.org/repo/m2-ibiblio-rsync-repository/org/apache/derby/ \! -perm -g+w"

For each project, the following files should be found in the
various 'maven2/[project]/target' directories after 'verify' or 'install':
    - ARTIFACT-VERSION.jar
    - ARTIFACT-VERSION.jar.asc
    - ARTIFACT-VERSION.pom
    - ARTIFACT-VERSION.pom.asc

When these are deployed, or installed locally, checksum files (a md5 and a sha1
file for each artifact) will be generated by Maven. Check your local
repository to confirm this (i.e. '~/.m2/repository').
The 'derbywar' project will have a war file instead of a jar file.

Some time after you have deployed the artifacts to the Apache staging
repository (happens when you run 'mvn deploy'), they should appear in the
central Maven repository.
Try one of these to confirm that your artifacts are available:
http://repo1.maven.org/maven2/org/apache/derby/
http://mvnrepository.com/artifact/org.apache.derby

Note that for the 10.6.1 release, within a day the artifacts turned up in the
Apache repository (the first link). It took 6 days for the artifacts
to percolate to the external aggregator site (the second link).

Release history for Maven 2 artifacts
=====================================

The list below shows the Apache Derby artifacts published by the Apache Derby
community.
The dates are when the artifacts were written to the central Maven repository
(repo1.maven.org/maven2 or repo2.maven.org/maven2).

2010-05-18 10.6.1.0      OK
2009-10-07 10.5.3.0_1   OK
2009-08-26 10.5.3.0     BROKEN
    An error in all the POMs made these artifacts unusable (DERBY-4390).
    Use version 10.5.3.0_1 instead.
