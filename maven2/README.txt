GENERATING MAVEN 2 ARTIFACTS FOR APACHE DERBY
=============================================

The POMs in the maven2 directory are able to generate Maven 2 artifacts for
Apache Derby. The following software is required for deploying a release:
 1. Maven 2
 2. GnuPG (for signing the artifacts)
 3. ssh/scp (for site deployment)

Note that Maven 2 will pull down quite a few required plugins the first time
you run it. They will be cached locally, so they are not downloaded again the
next time.

Short description of the required steps:
 a) Generate the Derby jar files.
    For releases, generate the insane jars. You can specify which jars to use
    with the property 'sanity' in the top-level POM.
    The jars are expected to be found in 'jars/[in]sane' relative to the
    checked out code repository.

 b) Specify required information for one or all of the following steps.
    To generate and deploy release artifacts, these pieces of information must
    be specified:
      - The Derby release version, which must be specified in all POMs.
        One way to do this, is to use search and replace (i.e. Perl or sed).
      - Passphrase for your GPG signing key, see top level POM and step (d).
      - User credentials for deployment, see 'settings.xml'.

 c) 'mvn clean install'
    Generates the artifacts, signatures for the artifacts using GnuPG and
    installs the artifacts in the local repository.
    You are required to provide your private key and the passphrase to GnuPG.
    Using a passphrase agent is recommended, but you can also specify it on
    the command line when invoking maven with -Dgpg.passphrase=PASSPHRASE.
    There are other ways to achieve this too, but please do not specify you
    passphrase in the POM that is deployed on the Maven repositories!
    The local repository is typically found in '~/.m2/repository/', and the
    Derby artifacts are located under "org/apache/derby/".
    The clean target is included to avoid unintentionally installing/deploying
    artifacts not supposed to be deployed.

    NOTE: Do not run 'mvn package|verify install', that is to combine either
          package or verify with install, as this causes the
          signatures of the artifacts to be signed. This shows as files like
          './engine/target/derby-trunk-alpha.jar.asc.asc'.

 d) 'mvn deploy' or 'mvn clean deploy'
   NOTE: This step has been reported not to work. Deploy manually until fixed.
   Deploys the artifacts, including signatures and checksum files, to the
   Apache Maven 2 repository. The files will then be distributed to mirrors.

Basically, for each project, the following files should be found in the
various 'maven2/[project]/target' directories:
    - ARTIFACT-VERSION.jar
    - ARTIFACT-VERSION.jar.asc
    - ARTIFACT-VERSION.pom
    - ARTIFACT-VERSION.pom.asc

When these are deployed or installed locally, there will be a md5 and a sha1
file for each artifact. The 'derbywar' project will have a war file instead
of a jar file.
