GENERATING MAVEN 2 ARTIFACTS FOR APACHE DERBY
=============================================

The POMs in the 'maven2' directory enable you to generate Maven 2 artifacts
for Apache Derby. The following software is required for deploying a release:
 1. Maven 2/3
 2. GnuPG (for signing the artifacts)

Note that Maven 2 will pull down quite a few required plugins the first time
you run it. They will be cached locally, so they are not downloaded again the
next time.

All commands below are to be executed from the directory 'maven2' within the
Derby source code repository.

Overview of the required steps:
 a) Generate the Derby jar files.
 b) Specify required information
 c) 'mvn clean install'
 d) 'mvn deploy' or 'mvn clean deploy'
 e) Close the staging repository in Nexus.
 f) Release the artifacts in Nexus after a successful vote.
 g) Verify that the artifacts appear in the central Maven repository.

Description of the required steps:
 a) Generate the Derby jar files.
    For releases, generate the insane jars. You can override which jars to use
    with the property 'sanity' in the top-level POM.
    The jars are expected to be found in 'jars/[in]sane' relative to the
    checked out code repository.
    
    If generating artifacts for a release you have not built yourself, the
    following steps are recommended:
        1) update the source to the revision of the release. 
        2) build the source using sane=false.
        3) make sure the version reported by sysinfo is correct.
        4) download the lib distribution for the release.
        5) *IMPORTANT* verify the download (checksum and signature).
        6) copy the JAR files and the WAR file from the unpacked distribution
           file into 'jars/insane'.
        7) follow the remaining instructions for generating Maven artifacts

 b) Specify required information.
    To successfully generate and deploy release artifacts, all of these
    must be specified:

      o The Derby release version.
        The version must be specified in all POMs. Compile and execute the
        Java program SetDerbyVersion, found in the 'maven2' directory, i.e.:
            javac SetDerbyVersion && java -cp .:../jars/insane/derbyshared.jar:../jars/insane/derby.jar SetDerbyVersion

        Alternatively, use search and replace (i.e. Perl or sed) - make sure
        you don't replace version tags that aren't supposed to be modified.
        Make sure you diff the POMs to verify the changes.
        Note that the Java program performs some extra sanity checks.

      o Passphrase for your GPG signing key.
        Required for step (c) and (d). See the top-level POM for details, brief
        instructions in (c).

      o User credentials for deployment/upload.
        Required for step (d), usually you do this only once. If you change
        your password or start using a different machine, you'll have to do it
        again. You should encrypt your password(s), and to do that you have to
        edit/create two files in USER_HOME/.m2 (i.e. ~/.m2 on *nix systems).
        See http://maven.apache.org/guides/mini/guide-encryption.html
        for how to do this the right way. The id to use in the server section
        of settings.xml is specified in the ASF parent pom, and is currently
        "apache.releases.https".

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
    temporary Apache staging repository managed by Nexus.
    Remember that you will need to specify your gpg passphrase here too,
    preferably by using a passphrase agent. Alternatively:

        mvn -Dgpg.passphrase="my secret passphrase" deploy

 e) Close the staging repository in Nexus.
    Once you have deployed the artifacts you should close the staging
    repository to allow others to test the artifacts. Log into
    https://repository.apache.org/ using your LDAP credentials.

 f) Release the artifacts in Nexus after a successful vote.
    Once the vote has passed, the artifacts can be released. To do this you
    log in to Nexus using you LDAP credentials, select the correct staging
    repository, and perform the release action on it.

 g) Verify that the artifacts appear in the central Maven repository.
    Some time after you have released the artifacts from the temporary Apache
    staging repository in step (f), they should appear in the central Maven
    repository:
        http://repo1.maven.org/maven2/org/apache/derby/
    After a few more days, the artifacts may also have propagated to other
    repositories / services, for instance the one below:
        http://mvnrepository.com/artifact/org.apache.derby

    Note that for the 10.6.1 release, within a day the artifacts turned up in
    the central Maven repository (the first link). It took 6 days for the
    artifacts to percolate to the external aggregator site (the second link).


Other information:
 o  For each project, the following files should be found in the various
    'maven2/[project]/target' directories after 'verify' or 'install':
      ARTIFACT-VERSION.jar
      ARTIFACT-VERSION.jar.asc
      ARTIFACT-VERSION.pom
      ARTIFACT-VERSION.pom.asc

    When these are deployed, or installed locally, checksum files (a .md5 and
    a .sha1 file for each artifact) will be generated by Maven. Check your
    local repository to confirm this (i.e. '~/.m2/repository').
    The 'derbywar' project will have a war file instead of a jar file.

 o  More ASF generic information about the Maven artifact release process:
    http://www.apache.org/dev/publishing-maven-artifacts.html
