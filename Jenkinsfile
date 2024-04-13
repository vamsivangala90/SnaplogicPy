pipeline {
    agent {
        // ... (Specify agent details, potentially with a label for specific Python version)
    }
    environment {
        // ... (Define environment variables for your virtual environment if needed)
    }
    stages {
        stage('Activate Virtual Environment') {
            steps {
                sh 'source /home/gaian/PycharmProjects/pythonProject/.venv/bin/activate'  // Replace with your virtual environment activation command
            }
        }
        stage('Install Dependencies') {
            steps {
                sh 'pip install google-api-python-client'  // Or the specific library name if different
            }
        }
        // ... other stages
    }
}
