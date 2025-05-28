#!/bin/bash

profile=$1
if [ -z "$profile" ]; then
    echo "Usage: $0 <aws-profile> <credentials-file>"
    exit 1
fi

credentials_file=$2
if [ -z "$2" ]; then
    credentials_file="$HOME/.aws/credentials"
    echo "Using default credentials file: $credentials_file"
fi

# Check if the credentials file exists
if [ ! -f "$credentials_file" ]; then
    echo "Error: AWS credentials file '$credentials_file' not found."
    exit 1
fi

# Extract AWS credentials for the specified profile
export AWS_ACCESS_KEY_ID=$(awk -F '=' '/^\['$profile'\]/{p=1}p&&/aws_access_key_id/{print $2;exit}' $credentials_file)
export AWS_SECRET_ACCESS_KEY=$(awk -F '=' '/^\['$profile'\]/{p=1}p&&/aws_secret_access_key/{print $2;exit}' $credentials_file)


# echo "$profile $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY"
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Error: AWS credentials not found for profile '$profile'."
    exit 1
fi

docker run -d -it --rm --user root \
    -v .:/home/hadoop/workspace/ \
    -v ./jars/:/opt/spark/jars/ \
    -v aws-glue-vscode-server:/root/.vscode-server \
    -e AWS_PROFILE=$profile \
    -e AWS_CONFIG_FILE=/home/hadoop/workspace/.aws/config \
    -e AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    --name glue5_pyspark \
    public.ecr.aws/glue/aws-glue-libs:5 \
    pyspark