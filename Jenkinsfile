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
            # Start Glue job and capture Job Run ID
            JOB_RUN_ID=$(aws glue start-job-run --job-name ${GLUE_JOB_NAME} --query 'JobRunId' --output text)
            echo "Glue Job started with Run ID: $JOB_RUN_ID"

            # Wait for job to complete
            echo "Waiting for Glue job to complete..."
            while true; do
                STATUS=$(aws glue get-job-run --job-name ${GLUE_JOB_NAME} --run-id $JOB_RUN_ID --query 'JobRun.JobRunState' --output text)
                echo "Current status: $STATUS"

                if [ "$STATUS" = "SUCCEEDED" ]; then
                    echo "Glue job completed successfully!"
                    break
                elif [ "$STATUS" = "FAILED" ] || [ "$STATUS" = "STOPPED" ] || [ "$STATUS" = "TIMEOUT" ]; then
                    echo "Glue job failed with status: $STATUS"
                    exit 1
                fi

                echo "Job still running... checking again in 30 seconds"
                sleep 30
            done
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