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
      commercial_data_bucket:
        label: bucket from which commercial data will be recieved
        doc: bucket from which commercial data will be recieved
        type: string
      product_bundle:
        label: Reference to a group of settings describing parameters for an order
        doc: Reference to a group of settings describing parameters for an order
        type: string
      stac_key:
        label: path to stac item in s3 describing data to order and download
        doc: path to stac item in s3 describing data to order and download
        type: Directory
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
          stac_key: stac_key
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: airbus-optical-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/n1b3o1k2/airbus-optical-adaptor:0.0.1-rc22
    baseCommand: ["python", "-m", "airbus_optical_adaptor"]
    inputs:
      commercial_data_bucket:
        type: string
        inputBinding:
          position: 1
      product_bundle:
        type: string
        inputBinding:
          position: 2
      stac_key:
        type: Directory
        inputBinding:
          position: 3

    outputs:
      results:
        type: Directory
        outputBinding:
          glob: .
