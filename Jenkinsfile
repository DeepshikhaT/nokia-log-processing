pipeline {

    agent any

    // Nightly run at midnight
    triggers {
        cron('0 0 * * *')
    }

    environment {
        AWS_ACCESS_KEY_ID     = credentials('aws-access-key')
        AWS_SECRET_ACCESS_KEY = credentials('aws-secret-key')
        AWS_DEFAULT_REGION    = 'us-east-1'
        BUCKET_NAME           = 'nokia-log-processing-bucket-2024'
        GLUE_JOB_NAME         = 'nokia-log-processing-job1'
        GLUE_SCRIPT_S3_PATH   = 's3://nokia-log-processing-bucket-2024/scripts/glue_transform.py'
        PATH                  = "/usr/local/bin:/opt/homebrew/bin:${env.PATH}"
    }

    stages {

        stage('Checkout Code') {
            steps {
                echo 'Checking out code from GitHub...'
                checkout scm
            }
        }

        stage('Verify AWS Connection') {
            steps {
                echo 'Verifying AWS connection...'
                sh 'aws s3 ls'
            }
        }

        stage('Upload Glue Script to S3') {
            steps {
                echo 'Uploading glue_transform.py to S3...'
                sh '''
                    aws s3 cp scripts/glue_transform.py ${GLUE_SCRIPT_S3_PATH}
                    echo "Script uploaded successfully!"
                '''
            }
        }

        stage('Trigger Glue Job') {
            steps {
                echo 'Triggering AWS Glue Job...'
                sh '''
                    aws glue start-job-run --job-name ${GLUE_JOB_NAME}
                    echo "Glue job triggered successfully!"
                '''
            }
        }

    }

    post {
        success {
            echo 'Pipeline completed successfully!'
        }
        failure {
            echo 'Pipeline failed — check logs for details!'
        }
    }
}