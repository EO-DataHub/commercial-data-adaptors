# Commercial Data Adaptors

This repository contains a collection of commercial data adaptors. These modules are designed to be run as workflows within the ADES component of the EODH. Each adaptor contains cwl and http scripts to demonstrate deployment and usage in an EODH environment. 

Adaptors will be deployed to a centralised workspace per data provider, which users will be able to call as a user service. These workspaces are granted additional permissions, such as access to api keys and data transfer S3 buckets.

A typical data adaptor has a flow of:
1. Submitting an order with the commercial data provider for a specified item
2. Waiting for the order to be fulfilled
3. Collecting the data and recording it as assets in an existing STAC item

Once the workflow step is complete, the STAC item and assets are uploaded and ingested into the user's workspace by the ADES.

## Available Adaptors

This repository currently includes the following four adaptors, each tailored for different commercial data providers and data types:

| Adaptor Name         | Data Provider | Data Type         | Key Differences / Notes                                      |
|----------------------|--------------|-------------------|--------------------------------------------------------------|
| **airbus-sar**       | Airbus       | SAR (Radar)       | Handles Airbus SAR imagery, SAR-specific ordering and .tar.gz archive format. |
| **airbus-optical**   | Airbus       | Optical           | Handles optical PNEO, Pleiades, and SPOT imagery, delivered in a .zip archive. |
| **airbus-optical-multi** | Airbus   | Optical (Multi)   | Experimental; designed for stereo and multi PNEO orders which refer to multiple items. Not in active use. |
| **planet**           | Planet       | Optical           | Integrates with Planet custom API module, manages Planet-specific order flow and assets delivered in a folder structure.        |

> **Note:**  
> The `airbus-optical-multi` adaptor is currently experimental and not in active use. It is included for future development and may require updates.

Each adaptor is implemented with its own CWL and HTTP scripts, providing an example of basic inputs and usage, although all adaptors are designed to be called by a service within the Hub infrastructure that will manage the backend STAC item correctly.

### Building and Deploying the Adaptors

Adaptors are organized into either the `planet` or `airbus` folders, depending on the data provider.

To deploy a data adaptor, follow these steps:

1. Navigate to the adaptor directory:
   - For Airbus adaptors: `cd airbus`
   - For Planet adaptor: `cd planet`

2. Build the Docker image:
   - For **Airbus SAR** and **Airbus Optical**:
     ```
     docker build --no-cache -t airbus-sar-adaptor:<tag> -f airbus_sar_adaptor/Dockerfile .
     ```
     *(Replace the tag as needed, and `airbus_sar_adaptor` with `airbus_optical_adaptor` for optical)*
   - For **Planet**:
     ```
     docker build --no-cache -t planet-adaptor:<tag> .
     ```
3. Tag and push the image to Docker Hub or another Docker repository (e.g., AWS ECR):
   ```
   docker tag <image-name>:<tag> <your-repo>/<image-name>:<tag>
   docker push <your-repo>/airbus-sar-adaptor:<version-tag>
   ```
4. Update the `.cwl` file with the correct image name.
5. Using the provided `.http` file with an api key scoped to the data provider workspace, delete any existing workflow and then redeploy the latest version. We recommend using the VS Code extension **REST Client** to execute the `.http` file.
