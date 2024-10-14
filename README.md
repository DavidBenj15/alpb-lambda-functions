# ALPB Lambda Functions
This repository contains the AWS Lambda functions used in for ALPB Analytics Platorm, their associated tests, and some additional Python scripts used in the data pipeline.

## Developer Instructions
### To deploy a Lambda function to AWS:
Ensure Docker engine is running on your machine, and <code>cd</code> into your function's directory (ex: /functions/process_trackman). You can check if your AWS CLI is configured properly by running <code>$ aws sts get-caller-identity</code>
1. Run <code>$ cdk bootstrap aws://*accountId*/*us-east-2*</code>. If you have already successfully run this command with the same account in the same region, you won't need to run it again.
2. Next, run <code>$ cdk deploy</code>. If everything was successful, your image should be deployed to Lambda!

For more detailed instructions, check out [this video](https://www.youtube.com/watch?v=wbsbXfkv47A&t=431s).
