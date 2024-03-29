This is a Python-based client enabling interaction with GHGA's file services.
Contrary to the design of the actual services, the client does not follow the triple-hexagonal architecture.
The client is roughly structured into three parts:

1. A command line interface using typer is provided at the highest level of the package, i.e. directly within the ghga_connector directory.
2. Functionality dealing with intermediate transformations, delegating work and handling state is provided within the core module.
3. core.api_calls provides abstractions over S3 and work package service interactions.
