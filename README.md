# AWS Glue Jobs Development Environment

This project provides a development environment for creating and testing AWS Glue ETL jobs locally using Docker containers.

## Features

- Local development environment for AWS Glue jobs using official AWS Glue container image
- Built on AWS Glue 5.0 image (`public.ecr.aws/glue/aws-glue-libs:5`)
- Includes sample ETL jobs and data pipeline examples
- VS Code integration for development and debugging

## Project Structure

```
.
├── .aws/                 # AWS credentials and config (create locally)
├── .vscode/              # VS Code configuration
├── jars/                 # Additional JARs and dependencies
├── source/              
│   ├── samples/          # Sample Glue job implementations
│   └── *.py              # Glue ETL job scripts
├── startDevContainer.sh  # Script to launch dev container
└── run.sh                # Sample job execution script
```

## Setup

1. Create `.aws` directory with your AWS credentials and config:

```
.aws/
  ├── credentials        # AWS credentials file
  └── config             # AWS config file with region etc
```

2. Add any additional JAR dependencies to `jars/` directory

3. Start the development container:

```bash
./startDevContainer.sh <aws-profile> [credentials-file]
```

Arguments:
- `aws-profile`: AWS credentials profile to use
- `credentials-file`: Optional path to AWS credentials file (default: ~/.aws/credentials)

## Development

- Use VS Code with dev container integration for development
- Sample jobs are available in `source/samples` 
- Write new Glue jobs in `source` directory
- Test jobs locally before deploying to AWS

### VS Code Extensions

When first launching the container, VS Code will automatically create a volume named `aws-glue-vscode-server` that persists extensions between container launches. The following extensions are recommended for Python development in the container:

- [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python): Python language support
- [Black Formatter](https://marketplace.visualstudio.com/items?itemName=ms-python.black-formatter): Python code formatting

Additionally, for VS Code Tasks to work properly, the following extension must be installed in your host VS Code instance:

- [Command Variable](https://marketplace.visualstudio.com/items?itemName=rioj7.command-variable): Required for dynamic task variables

### VS Code Tasks

This project includes preconfigured VS Code tasks to streamline the development workflow:

1. **Launch Container**: Starts the AWS Glue development container
   ```bash
   Tasks > Run Task > Launch Container
   ```
   This will prompt for your AWS profile and use credentials from `.aws/credentials`

2. **Attach to Container**: Opens a new VS Code window attached to the running container
   ```bash
   Tasks > Run Task > Attach to Container
   ```
   This connects to the container named `glue5_pyspark`, giving you full access to the development environment

3. **Start Container**: (Recommended Workflow) Opens VS Code directly in the container
   ```bash
   Tasks > Run Task > Start Container
   ```
   This combines both launching and attaching in a single step - the most convenient way to start development

Using these tasks eliminates the need to manually run commands in the terminal and provides a smoother development experience.

## Requirements

- Docker
- AWS Account and credentials
- VS Code (recommended)

## References

- [Develop and test AWS Glue jobs locally using Docker](https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-5-0-jobs-locally-using-a-docker-container/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)

## License

MIT