pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                // Clone the Git repository
                git url: 'https://github.com/lakkireddysatheeshreddy/pysparkCICD.git'
            }
        }
        
        stage('Build') {
            steps {
                // Set up the Python environment
                sh 'python3 -m venv venv'
                sh 'source venv/bin/activate'
                
                // Install dependencies
                sh 'pip install -r requirements.txt'
                
                // Build or prepare the code as needed
                
                // Copy the code to the dev server
                sh 'scp -r * user@dev-server:/path/to/destination'
            }
        }
    }
    
    post {
        success {
            // Perform actions when the pipeline succeeds
            echo 'Deployment to dev server successful!'
        }
        failure {
            // Perform actions when the pipeline fails
            echo 'Deployment to dev server failed!'
        }
    }
}
