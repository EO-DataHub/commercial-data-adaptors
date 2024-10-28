# commercial-data-adaptors

### Building and Deploying the Adaptors

To deploy a data adaptor, follow these steps:

1. Navigate to the adaptor directory:
   `cd airbus-sar`
2. Build the Docker image:
   `docker build -t airbus-sar-adaptor .`
3. Tag and push the image to Docker Hub or another Docker repository (e.g., AWS ECR):
  ` docker tag airbus-sar-adaptor <your-repo>/airbus-sar-adaptor`
   `docker push <your-repo>/airbus-sar-adaptor`
4. Update the `.cwl` file with the correct image name.
5. Execute the workflow using the provided `.http` file after setting up your environment. We recommend using the `vs-code` extension `REST Client` to execute the `.http` file.
