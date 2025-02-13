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
      commercial_data_bucket:
        label: bucket from which commercial data will be received
        doc: bucket from which commercial data will be received
        type: string
      product_bundle:
        label: Product bundles comprise of a group of assets for an item. In the Planet API, an item is an entry in our catalog, and generally represents a single logical observation (or scene) captured by a satellite. Each item is defined by an item_type, which represents the class of spacecraft and/or processing level of the item
        doc: Product bundles comprise of a group of assets for an item. In the Planet API, an item is an entry in our catalog, and generally represents a single logical observation (or scene) captured by a satellite. Each item is defined by an item_type, which represents the class of spacecraft and/or processing level of the item
        type: string
      stac_key:
        label: path to stac item in s3 describing data to order and download
        doc: path to stac item in s3 describing data to order and download
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
          commercial_data_bucket: commercial_data_bucket
          product_bundle: product_bundle
          stac_key: stac_key
        out:
          - results
  # convert.sh - takes input args `--url`
  - class: CommandLineTool
    id: planet-adaptor
    hints:
      DockerRequirement:
        dockerPull: public.ecr.aws/eodh/planet-adaptor:latest
    baseCommand: ["python", "-m", "planet_adaptor"]
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
