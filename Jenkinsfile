pipeline {
    agent any
    stages {
        stage('Install Dependencies') {
            steps {
                sh 'pip install google-api-python-client' 
                sh 'pip install pip install snowflake-connector-python'// Or the specific library name if different
            }
        }
        // ... other stages
    }
}
