cwlVersion: v1.0
$namespaces:
  s: https://schema.org/
s:softwareVersion: 0.1.2
schemas:
  - http://schema.org/version/9.0/schemaorg-current-http.rdf
$graph:
  # Workflow entrypoint
  - class: Workflow
    id: open-cosmos-adaptor
    label: Open Cosmos Adaptor
    doc: Order and load data from the Open Cosmos catalogue into a workspace
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
          - open-cosmos-adaptor/results
    steps:
      open-cosmos-adaptor:
        run: "#open-cosmos-adaptor"
        in:
          workspace: workspace
          cluster_prefix: cluster_prefix
          workspace_bucket: workspace_bucket
          commercial_data_bucket: commercial_data_bucket
          pulsar_url: pulsar_url
          coordinates: coordinates
          stac_key: stac_key
        out:
          - results
  - class: CommandLineTool
    id: open-cosmos-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/eodh/open-cosmos-adaptor:0.1.0
    requirements:
      EnvVarRequirement:
        envDef:
          CLUSTER_PREFIX: $(inputs.cluster_prefix)
    baseCommand: ["python", "-m", "open_cosmos_adaptor"]
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
      coordinates:
        type: string
        inputBinding:
          position: 5
      stac_key:
        type: Directory
        inputBinding:
          position: 6

    outputs:
      results:
        type: Directory
        outputBinding:
          glob: .
