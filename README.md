# race-simulation

This is an example on how to run MonteCarlo analysis for F1 race strategy. You can find out a lot more about the background [here](https://blogs.oracle.com/cloud-infrastructure/post/modernize-your-motor-sport-race-strategy-on-oracle-cloud-infrastructure) The actual simulation of the race is using the work found [here](https://github.com/TUMFTM/race-simulation)
Running billions of race simulations can take a significant amount of time. This is why we wil use cloud native applications to parallelize this work in order to get the information faster. 
Actual teams are using a similar approach live during the race to predict pit stops and tyres compounds. This example will focus on making pre-race decisions and looking at how the strategies are giving different expected results. 

## Overal architecture:

![Demo Architecture](images/Race_sim_demo_arch.png)

## Demo

### Setting up the OKE cluster
- Connect to your tenancy through the [cloud console](https://cloud.oracle.com/)
- Go to Menu/Developer Services/Kubernetes Clusters (OKE)
- Click on **Create Cluster** and select **Quick Create**
- Choose a name, select **Public Endpoint** and **Private Workers** as well as a shape. Depending on your limits, you can choose add more or less nodes to your pool with varying size. We can start with 3 VM of Standard.E3.Flex with 8 cores and 8GB or RAM. 

While Oracle Cloud is doing it's magic in the background, let's configure the cloud shell. 

### Connect to the cluster through the Cloud Shell

Click on this icon on the top right corner to launch a cloud shell: ![Cloud Shell Access](images/Cloud_shell.png)\

This will spin up a small free VM inside of the console that will allow you to connect easily to the OKE cluster. You can also spin up an actual VM to do this and follow the instructions from [here]() to access your cluster 

### Launch pods and services



### Submit Initial Job



