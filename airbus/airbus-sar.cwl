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
      commercial_data_bucket:
        label: bucket from which commercial data will be received
        doc: bucket from which commercial data will be received
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
        label: path to stac item in s3 describing data to order and download
        doc: path to stac item in s3 describing data to order and download
        type: Directory
      workspace:
        label: workspace final destination of the order
        doc: workspace final destination of the order
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
          commercial_data_bucket: commercial_data_bucket
          product_bundle: product_bundle
          coordinates: coordinates
          stac_key: stac_key
          workspace: workspace
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: airbus-sar-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/eodh/airbus-sar-adaptor:0.0.6-rc3
    baseCommand: ["python", "-m", "airbus_sar_adaptor"]
    inputs:
      commercial_data_bucket:
        type: string
        inputBinding:
          position: 1
      product_bundle:
        type: string
        inputBinding:
          position: 2
      coordinates:
        type: string
        inputBinding:
          prefix: --coordinates
          position: 3
      stac_key:
        type: Directory
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
