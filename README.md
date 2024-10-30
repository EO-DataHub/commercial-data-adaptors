# Commercial Data Adaptors

This repository contains a collection of commercial data adaptors. These modules are designed to be run as workflows within the ADES component of the EODH. Each adaptor contains cwl and http scripts to demonstrate deployment and usage in an EODH environment. 

Adaptors will be deployed to a centralised workspace per data provider, which users will be able to call. The adaptors will require elevated permissions to run successfully, such as access to api keys and data transfer S3 buckets.

A typical data adaptor has a flow of:
1. Submitting an order with the commercial data provider for a specified item
2. Waiting for the order to be fulfilled
3. Collecting the data and transferring it to the ordering user's workspace
4. Updating the user's STAC catalogue to refer to the downloaded data

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
