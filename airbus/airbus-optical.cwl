cwlVersion: v1.0
$namespaces:
  s: https://schema.org/
s:softwareVersion: 0.1.2
schemas:
  - http://schema.org/version/9.0/schemaorg-current-http.rdf
$graph:
  # Workflow entrypoint
  - class: Workflow
    id: airbus-optical-adaptor
    label: Airbus Optical Adaptor
    doc: Order and load data from the Airbus optical catalogue into a workspace
    inputs:
      workspace:
        label: Workspace name
        doc: Name of workspace
        type: string
      cluster_prefix:
        label: cluster_prefix
        doc: Platform prefix
        type: string
      workspace_bucket:
        label: Bucket within which workspace data is stored
        doc: Bucket within which workspace data is stored
        type: string
      commercial_data_bucket:
        label: Bucket from which commercial data will be received
        doc: Bucket from which commercial data will be received
        type: string
      pulsar_url:
        label: URL to inform the pulsar environment of STAC updates
        doc: URL to inform the pulsar environment of STAC updates
        type: string
      product_bundle:
        label: Reference to a group of settings describing parameters for an order
        doc: Reference to a group of settings describing parameters for an order
        type: string
      coordinates:
        label: List of coordinates for the area of interest
        doc: List of coordinates for the area of interest
        type: string
      stac_key:
        label: Path to stac item in s3 describing data to order and download
        doc: Path to stac item in s3 describing data to order and download
        type: Directory
      end_users:
        label: List of end users and nationalities. Only required for PNEO orders
        doc: List of end users and nationalities. Only required for PNEO orders
        type: string
      licence:
        label: Licence used for the order
        doc: Licence used for the order
        type: string
    outputs:
      - id: results
        type: Directory
        outputSource:
          - airbus-optical-adaptor/results
    steps:
      airbus-optical-adaptor:
        run: "#airbus-optical-adaptor"
        in:
          workspace: workspace
          workspace_bucket: workspace_bucket
          commercial_data_bucket: commercial_data_bucket
          pulsar_url: pulsar_url
          product_bundle: product_bundle
          coordinates: coordinates
          stac_key: stac_key
          end_users: end_users
          licence: licence
          cluster_prefix: cluster_prefix
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: airbus-optical-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/eodh/airbus-optical-adaptor:0.0.7
    requirements:
      EnvVarRequirement:
        envDef:
          CLUSTER_PREFIX: $(inputs.cluster_prefix)
    baseCommand: ["python", "-m", "airbus_optical_adaptor"]
    inputs:
      workspace:
        type: string
        inputBinding:
          position: 1
      cluster_prefix:
        type: string
      workspace_bucket:
        type: string
        inputBinding:
          position: 2
      commercial_data_bucket:
        type: string
        inputBinding:
          position: 3
      pulsar_url:
        type: string
        inputBinding:
          position: 4
      product_bundle:
        type: string
        inputBinding:
          position: 5
      coordinates:
        type: string
        inputBinding:
          prefix: --coordinates
          position: 6
      stac_key:
        type: Directory
        inputBinding:
          prefix: --catalogue_dirs
          position: 7
      end_users:
        type: string
        inputBinding:
          prefix: --end_users
          position: 8
      licence:
        type: string
        inputBinding:
          prefix: --licence
          position: 9

    outputs:
      results:
        type: Directory
        outputBinding:
          glob: .
