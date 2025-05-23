# NOTE: This script is experimental and not in active use. Future updates may be required.
cwlVersion: v1.0
$namespaces:
  s: https://schema.org/
s:softwareVersion: 0.1.2
schemas:
  - http://schema.org/version/9.0/schemaorg-current-http.rdf
$graph:
  # Workflow entrypoint
  - class: Workflow
    id: airbus-optical-adaptor-multi
    label: Airbus Optical Adaptor Multi
    doc: Order and load multiple data items from the Airbus optical catalogue into a workspace
    inputs:
      commercial_data_bucket:
        label: bucket from which commercial data will be received
        doc: bucket from which commercial data will be received
        type: string
      cluster_prefix:
        label: cluster_prefix
        doc: Platform prefix
        type: string
      product_bundle:
        label: Reference to a group of settings describing parameters for an order
        doc: Reference to a group of settings describing parameters for an order
        type: string
      coordinates:
        label: List of coordinates for the area of interest
        doc: List of coordinates for the area of interest
        type: string
      stac_keys:
        label: paths to stac item in s3 describing data to order and download
        doc: paths to stac item in s3 describing data to order and download
        type: Directory[]
      workspace:
        label: workspace final destination of the order
        doc: workspace final destination of the order
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
          commercial_data_bucket: commercial_data_bucket
          product_bundle: product_bundle
          coordinates: coordinates
          stac_keys: stac_keys
          workspace: workspace
          cluster_prefix: cluster_prefix
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: airbus-optical-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/eodh/airbus-optical-adaptor:0.0.3-rc2
    requirements:
      EnvVarRequirement:
        envDef:
          CLUSTER_PREFIX: $(inputs.cluster_prefix)
    baseCommand: ["python", "-m", "airbus_optical_adaptor"]
    inputs:
      commercial_data_bucket:
        type: string
        inputBinding:
          position: 1
      cluster_prefix:
        type: string
      product_bundle:
        type: string
        inputBinding:
          position: 2
      coordinates:
        type: string
        inputBinding:
          prefix: --coordinates
          position: 3
      stac_keys:
        type: Directory[]
        inputBinding:
          prefix: --catalogue_dirs
          position: 4
      workspace:
        type: string
        inputBinding:
          prefix: --workspace
          position: 5

    outputs:
      results:
        type: Directory
        outputBinding:
          glob: .
