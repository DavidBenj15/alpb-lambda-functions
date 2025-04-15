# ALPB Lambda Functions
This repository contains the AWS Lambda functions used in for ALPB Analytics Platorm, their associated tests, and some additional Python scripts used in the data pipeline.

## Developer Instructions
### (Optional) Creating a virtual environment for your Python lambda function:
1. <code>cd</code> into your function's directory.
2. Execute `$ python -m venv <my-virtual-env>`
3. Activating your virtual environment - on Linux:
`source my-virtual-env/bin/activate`\
On Windows: `.\my-virtual-env\scripts\activate`
### Deploying a Lambda function to AWS:
#### I. via bash script (for Python scripts)
Ensure your lambda function and its dependencies are organized like so:
```
functions/
└── <your-function_directory>/
    └── venv/                   # virtual environment
    └── lambda_function.py
    └── requirements.txt
```
Next:
1. <code>cd</code> into the <code>functions</code> directory.
2. Run the bash deployment script:
```
./deploy_lambda_py.sh -f <your_function_on_lambda>
```
Follow the instructions provided and your code should be deployed!\
Note: since the bash script is primarily used to deploy API endpoints that interact with our database, the psycopg2 modules for __Python 3.11__ will be included in your deployment. Ensure your lambda function has the same runtime.

#### II. via Docker (for process_trackman)
Ensure Docker engine is running on your machine, and <code>cd</code> into your function's directory (ex: /functions/process_trackman). You can check if your AWS CLI is configured properly by running <code>$ aws sts get-caller-identity</code>
1. Run <code>$ cdk bootstrap aws://*accountId*/*us-east-2*</code>. If you have already successfully run this command with the same account in the same region, you won't need to run it again.
2. Next, run <code>$ cdk deploy</code>. If everything was successful, your image should be deployed to Lambda!

For more detailed instructions, check out [this video](https://www.youtube.com/watch?v=wbsbXfkv47A&t=431s).

## Where to Find Main Content for Data Pipeline
Our script to load trackman files can be found at `functions/process_trackman/image/src/main.py`.\
Associated tests can be found at `functions/process_trackman/test/test-process-trackman.py`.
