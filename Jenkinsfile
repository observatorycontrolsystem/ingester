#!/usr/bin/env groovy

@Library('lco-shared-libs@0.1.0') _

pipeline {
	agent any
	stages {
		stage('Build') {
			steps {
				sh 'make docker-build'
			}
		}
		stage('Push') {
			steps {
				sh 'make docker-push'
			}
		}
	}
	post {
		always { postBuildNotify() }
	}
}
