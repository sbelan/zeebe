// vim: set filetype=groovy:

def jdkVersion = 'jdk-8-latest'
def mavenVersion = 'maven-3.5-latest'
def mavenSettingsConfig = 'camunda-maven-settings'

def joinJmhResults = '''\
#!/bin/bash -x
cat **/*/jmh-result.json | jq -s add > target/jmh-result.json
'''

def goTests() {
    return '''\
#!/bin/bash -eux
echo "== Go build environment =="
go version
echo "GOPATH=${GOPATH}"

PROJECT_ROOT="${GOPATH}/src/github.com/zeebe-io"
mkdir -p ${PROJECT_ROOT}

PROJECT_DIR="${PROJECT_ROOT}/zeebe"
ln -fvs ${WORKSPACE} ${PROJECT_DIR}

cd ${GOPATH}/src/github.com/zeebe-io/zeebe/clients/go
make install-deps test
'''
}

pipeline {
    agent { node { label 'ubuntu-large' } }

    options {
        buildDiscarder(logRotator(daysToKeepStr:'14', numToKeepStr:'10'))
        timestamps()
        timeout(time: 45, unit: 'MINUTES')
    }

    stages {
        stage('Install') {
            steps {
                withMaven(jdk: jdkVersion, maven: mavenVersion, mavenSettingsConfig: mavenSettingsConfig) {
                    sh 'mvn -B -T 1C clean com.mycila:license-maven-plugin:check com.coveo:fmt-maven-plugin:check install -DskipTests'
                }

                stash name: "zeebe-dist", includes: "dist/target/zeebe-broker/**/*"
            }
        }

        stage('Verify') {
            failFast true
            parallel {
                stage('1 - Java Tests') {
                    steps {
                        withMaven(jdk: jdkVersion, maven: mavenVersion, mavenSettingsConfig: mavenSettingsConfig) {
                            sh 'mvn -B -T 1C verify -P skip-unstable-ci,retry-tests,parallel-tests'
                        }
                    }
                    post {
                        failure {
                            archiveArtifacts artifacts: '**/target/*-reports/**/*-output.txt,**/**/*.dumpstream', allowEmptyArchive: true
                        }
                    }
                }

                stage('2 - JMH') {
                    // delete this line to also run JMH on feature branch
                    when { anyOf { branch 'master'; branch 'develop' } }
                    agent { node { label 'ubuntu-large' } }

                    steps {
                        withMaven(jdk: jdkVersion, maven: mavenVersion, mavenSettingsConfig: mavenSettingsConfig) {
                            sh 'mvn -B integration-test -DskipTests -P jmh'
                        }
                    }

                    post {
                        success {
                            sh joinJmhResults
                            jmhReport 'target/jmh-result.json'
                        }
                    }
                }

                stage('3 - Go Tests') {
                    agent { node { label 'ubuntu' } }

                    steps {
                        unstash name: "zeebe-dist"
                        sh goTests()
                    }
                }
            }
        }

        stage('Deploy') {
            when { branch 'develop' }
            steps {
                withMaven(jdk: jdkVersion, maven: mavenVersion, mavenSettingsConfig: mavenSettingsConfig) {
                    sh 'mvn -B -T 1C generate-sources source:jar javadoc:jar deploy -DskipTests'
                }
            }
        }

        stage('Trigger Performance Tests') {
            when { branch 'develop' }
            steps {
                build job: 'zeebe-cluster-performance-tests', wait: false
            }
        }
    }

    post {
        changed {
            sendBuildStatusNotificationToDevelopers(currentBuild.result)
        }
    }
}

void sendBuildStatusNotificationToDevelopers(String buildStatus = 'SUCCESS') {
    def buildResult = buildStatus ?: 'SUCCESS'
    def subject = "${buildResult}: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'"
    def details = "${buildResult}: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' see console output at ${env.BUILD_URL}'"

    emailext (
        subject: subject,
        body: details,
        recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'RequesterRecipientProvider']]
    )
}
