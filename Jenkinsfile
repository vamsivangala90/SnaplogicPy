pipeline {
    agent any  // Or a specific agent label with Python installed

    environment {
        # Define environment variables if needed for your virtual environment (e.g., path to venv)
        VIRTUAL_ENV = '/home/gaian/PycharmProjects/pythonProject/.venv/bin/python'  # Replace with your actual path
    }

    stages {
        stage('Activate Virtual Environment') {
            steps {
                sh 'source /home/gaian/PycharmProjects/pythonProject/.venv/bin/activate'  // Activate virtual environment using environment variable
            }
        }

        stage('Install Dependencies') {
            steps {
                sh 'pip install google-api-python-client'
                sh 'pip install google-cloud'
                // Install library within activated virtual environment
            }
        }

        // ... other stages for running your script and other tasks
    }
}
