## Introduction 

The esgpull tool is designed to help users access and manage climate data from the Earth System Grid Federation (ESGF), which is commonly used for climate modeling projects like CMIP (Coupled Model Intercomparison Project). It enables data discovery, downloading, and storage management of these large datasets, offering a simplified command-line interface to interact with ESGF’s API.

With esgpull, users can search for datasets based on criteria like project name, variable ID, or experiment ID. After finding the desired datasets, they can add them to a local database, track updates, and download files using asynchronous, multi-file HTTP downloads. Additionally, esgpull provides conversion capabilities for queries initially designed for the synda tool, making it versatile for users who previously used that system.

For more information, you can view the tool’s quickstart guide and documentation on its official GitHub page or documentation site.

## Using esgpull via conda

In order to activate esgpull command line tool, you have to first
activate a Conda environment, follow these steps:

1.	Open a terminal (or Anaconda Prompt if on Windows).
2.	Activate the environment:
•	To activate an environment, use:

    conda activate <environment_name>


•	Replace <environment_name> with the name of the already existing environment, for example:

    conda activate esgpull_rucio


	3.	Verify Activation:
	•	Once activated, you should see the environment name in your terminal prompt, indicating that it’s active.
	4.	Deactivate the environment:
	•	To deactivate an active Conda environment, simply use:

conda deactivate



If you encounter issues, ensure Conda is properly installed and initialized in your terminal by running conda init, which sets up Conda to recognize the activation command across sessions.