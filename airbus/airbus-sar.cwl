cwlVersion: v1.0
$namespaces:
  s: https://schema.org/
s:softwareVersion: 0.1.2
schemas:
  - http://schema.org/version/9.0/schemaorg-current-http.rdf
$graph:
  # Workflow entrypoint
  - class: Workflow
    id: airbus-sar-adaptor
    label: Airbus SAR Adaptor
    doc: Order and load data from the Airbus SAR catalogue into a workspace
    inputs:
      workspace_bucket:
        label: Bucket within which workspace data is stored
        doc: Bucket within which workspace data is stored
        type: string
      cluster_prefix:
        label: cluster_prefix
        doc: Platform prefix
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
      licence:
        label: Licence used for the order
        doc: Licence used for the order
        type: string
      workspace:
        label: Workspace final destination of the order
        doc: Workspace final destination of the order
        type: string
    outputs:
      - id: results
        type: Directory
        outputSource:
          - airbus-sar-adaptor/results
    steps:
      airbus-sar-adaptor:
        run: "#airbus-sar-adaptor"
        in:
          workspace_bucket: workspace_bucket
          commercial_data_bucket: commercial_data_bucket
          pulsar_url: pulsar_url
          product_bundle: product_bundle
          coordinates: coordinates
          stac_key: stac_key
          licence: licence
          workspace: workspace
          cluster_prefix: cluster_prefix
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: airbus-sar-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/eodh/airbus-sar-adaptor:0.0.11
    requirements:
      EnvVarRequirement:
        envDef:
          CLUSTER_PREFIX: $(inputs.cluster_prefix)
    baseCommand: ["python", "-m", "airbus_sar_adaptor"]
    inputs:
      workspace_bucket:
        type: string
        inputBinding:
          position: 1
      cluster_prefix:
        type: string
      commercial_data_bucket:
        type: string
        inputBinding:
          position: 2
      pulsar_url:
        type: string
        inputBinding:
          position: 3
      product_bundle:
        type: string
        inputBinding:
          position: 4
      coordinates:
        type: string
        inputBinding:
          prefix: --coordinates
          position: 5
      stac_key:
        type: Directory
        inputBinding:
          prefix: --catalogue_dirs
          position: 6
      licence:
        type: string
        inputBinding:
          prefix: --licence
          position: 7
      workspace:
        type: string
        inputBinding:
          prefix: --workspace
          position: 8


    outputs:
      results:
        type: Directory
        outputBinding:
          glob: .
