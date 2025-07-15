cwlVersion: v1.0
$namespaces:
  s: https://schema.org/
s:softwareVersion: 0.1.2
schemas:
  - http://schema.org/version/9.0/schemaorg-current-http.rdf
$graph:
  # Workflow entrypoint
  - class: Workflow
    id: planet-adaptor
    label: Planet Adaptor
    doc: Order and load data from the Planet catalogue into a workspace
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
        label: Product bundles comprise of a group of assets for an item. In the Planet API, an item is an entry in our catalog, and generally represents a single logical observation (or scene) captured by a satellite. Each item is defined by an item_type, which represents the class of spacecraft and/or processing level of the item
        doc: Product bundles comprise of a group of assets for an item. In the Planet API, an item is an entry in our catalog, and generally represents a single logical observation (or scene) captured by a satellite. Each item is defined by an item_type, which represents the class of spacecraft and/or processing level of the item
        type: string
      coordinates:
        label: Coordinates of any AOI
        doc: Coordinates of any AOI
        type: string
      stac_key:
        label: Path to stac item in s3 describing data to order and download
        doc: Path to stac item in s3 describing data to order and download
        type: Directory
    outputs:
      - id: results
        type: Directory
        outputSource:
          - planet-adaptor/results
    steps:
      planet-adaptor:
        run: "#planet-adaptor"
        in:
          workspace: workspace
          workspace_bucket: workspace_bucket
          commercial_data_bucket: commercial_data_bucket
          pulsar_url: pulsar_url
          product_bundle: product_bundle
          coordinates: coordinates
          stac_key: stac_key
          cluster_prefix: cluster_prefix
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: planet-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/eodh/planet-adaptor:0.1.8
    requirements:
      EnvVarRequirement:
        envDef:
          CLUSTER_PREFIX: $(inputs.cluster_prefix)
    baseCommand: ["python", "-m", "planet_adaptor"]
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
          position: 6
      stac_key:
        type: Directory
        inputBinding:
          position: 7

    outputs:
      results:
        type: Directory
        outputBinding:
          glob: .
