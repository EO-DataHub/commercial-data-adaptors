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
      stac_key:
        label: path to stac item in s3 describing data to order and download
        doc: path to stac item in s3 describing data to order and download
        type: string
      workspace_bucket:
        label: bucket containing workspace contents to download data to
        doc: bucket containing workspace contents to download data to
        type: string
      workspace_domain:
        label: domain for the EODH workspace environment within which the data is stored
        doc: domain for the EODH workspace environment within which the data is stored
        type: string
      env:
        label: environment identifier within the Airbus ecosystem
        doc: environment identifier within the Airbus ecosystem
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
          stac_key: stac_key
          workspace_bucket: workspace_bucket
          workspace_domain: workspace_domain
          env: env
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: airbus-sar-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/n1b3o1k2/airbus-sar-adaptor:0.0.2
    baseCommand: ["python", "-m", "airbus_sar_adaptor"]
    inputs:
      stac_key:
        type: string
        inputBinding:
          position: 1
      workspace_bucket:
        type: string
        inputBinding:
          position: 2
      workspace_domain:
        type: string
        inputBinding:
          position: 3
      env:
        type: string
        inputBinding:
          position: 4

    outputs:
      results:
        type: Directory
        outputBinding:
          glob: .
