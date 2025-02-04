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
      stac_keys:
        label: paths to stac item in s3 describing data to order and download
        doc: paths to stac item in s3 describing data to order and download
        type: Directory[]
    outputs:
      - id: results
        type: Directory
        outputSource:
          - airbus-optical-adaptor/results
    steps:
      airbus-optical-adaptor:
        run: "#airbus-optical-adaptor"
        in:
          stac_keys: stac_keys
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: airbus-optical-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/n1b3o1k2/airbus-optical-adaptor:0.0.1-rc21
    baseCommand: ["python", "-m", "airbus_optical_adaptor"]
    inputs:
      stac_keys:
        type: Directory[]
        inputBinding:
          position: 1

    outputs:
      results:
        type: Directory
        outputBinding:
          glob: .
