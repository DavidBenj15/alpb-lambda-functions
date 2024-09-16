FROM public.ecr.aws/lambda/python:3.12

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip3 install -r requirements.txt

# Copy all files in ./src
COPY src/* ${LAMBDA_TASK_ROOT}

# Set the CMD to our handler
CMD [ "main.handler" ]
