#!/bin/bash

# Exit if any command fails
set -e

FUNCTIONS_DIR="functions"
PSYCOPG2_DIR="$(cd ../psycopg2-3.11 && pwd)"  # Path to your psycopg2 files

# Function to display usage instructions
usage() {
    echo "Usage: $0 -f <aws-lambda-function-name>"
    echo "Options:"
    echo "  -f, --function-name   Specify the AWS Lambda function name to deploy to"
    echo "  -h, --help            Show this help message"
    exit 1
}

# Parse command line arguments
AWS_LAMBDA_FUNCTION_NAME=""
while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--function-name)
            AWS_LAMBDA_FUNCTION_NAME="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Check if AWS Lambda function name is provided
if [ -z "$AWS_LAMBDA_FUNCTION_NAME" ]; then
    echo "Error: AWS Lambda function name is required."
    usage
fi

# Ensure script is run inside the functions directory
if [[ $(basename "$PWD") != "$FUNCTIONS_DIR" ]]; then
    echo "Error: Please run this script from within the '$FUNCTIONS_DIR' directory."
    exit 1
fi

# List valid Python Lambda function directories (must contain 'lambda_function.py')
echo "Searching for Python Lambda functions..."
VALID_FUNCTIONS=()
for dir in */; do
    if [ -f "$dir/lambda_function.py" ]; then
        VALID_FUNCTIONS+=("${dir%/}")  # Remove trailing slash
    fi
done

if [ ${#VALID_FUNCTIONS[@]} -eq 0 ]; then
    echo "No Python Lambda functions found in '$FUNCTIONS_DIR'."
    exit 1
fi

# Display function options
echo "Available Python Lambda functions:"
for i in "${!VALID_FUNCTIONS[@]}"; do
    echo "$((i+1)). ${VALID_FUNCTIONS[$i]}"
done

# Ask the user to select a function
read -p "Enter the number of the function to deploy: " CHOICE
if ! [[ "$CHOICE" =~ ^[0-9]+$ ]] || [ "$CHOICE" -lt 1 ] || [ "$CHOICE" -gt ${#VALID_FUNCTIONS[@]} ]; then
    echo "Invalid selection."
    exit 1
fi

SELECTED_FUNCTION="${VALID_FUNCTIONS[$((CHOICE-1))]}"
FUNCTION_NAME=$(basename "$SELECTED_FUNCTION")

echo "Deploying Lambda function: $FUNCTION_NAME"

# Navigate into the selected function's directory
cd "$FUNCTION_NAME"

# Find the virtual environment directory
VENV_DIR=$(find . -maxdepth 1 -type d -name "venv" -o -name "env" | head -n 1)
if [ -z "$VENV_DIR" ]; then
    echo "Error: No virtual environment found in '$FUNCTION_NAME'."
    exit 1
fi

# Activate the virtual environment
source "$VENV_DIR/bin/activate"

# Define build variables
BUILD_DIR="lambda_package"
ZIP_FILE="deployment_package.zip"

# Clean up old build artifacts
rm -rf "$BUILD_DIR" "$ZIP_FILE"
mkdir "$BUILD_DIR"

# Install dependencies into build directory
pip install -r requirements.txt --python-version 3.11 --target "$BUILD_DIR" --platform manylinux2014_x86_64 --only-binary=:all:

# Copy Lambda function code
cp -r *.py "$BUILD_DIR"

# Copy psycopg2 precompiled files
if [ -d "$PSYCOPG2_DIR" ]; then
    echo "Including psycopg2 dependencies from $PSYCOPG2_DIR..."
    cp -r "$PSYCOPG2_DIR/"* "$BUILD_DIR/"
else
    echo "Error: psycopg2 directory not found at $PSYCOPG2_DIR"
    exit 1
fi

# Package everything into a zip file
cd "$BUILD_DIR"
zip -r9 "../$ZIP_FILE" .
cd ..

# Deploy to AWS Lambda
echo "Deploying to AWS Lambda function: $AWS_LAMBDA_FUNCTION_NAME"
aws lambda update-function-code --function-name "$AWS_LAMBDA_FUNCTION_NAME" --zip-file fileb://"$ZIP_FILE"

echo "Deployment complete!"