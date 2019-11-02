# Running pre configured hadoop locally
This folder contains the dockerfile for:
 - building an hadoop image
 - exposing necessary ports
 - copying the sample files inside the image
 - creating a directory in hadoop hdfs where to place the copied samples (via hadoop command)
 - putting the sample files in the hdfs directory created (via hadoop command)

You'll be able to start an hadoop container with the sample files already added in hdfs under `/user/gdd`.

## Steps to run 
First of all place the `sample.json` and `twitter-sample.json` files inside this directory, together with this readme.

Then open a terminal, cd to this directory and execute the following:
```
#builds the image
$> docker build -t rvacaru/hdfs-gdd .

#runs the container and opens bash terminal for it
$> docker run --name hdfs-local --network gdd-net -p 22022:22 -p 8020:8020 -p 50010:50010 -p 50020:50020 -p 50070:50070 -p 50075:50075 -it rvacaru/hdfs-gdd

```

## See hdfs via webui
Check out the sample files in hdfs @ http://localhost:50070/explorer.html#/user/gdd

## Clean up the docker container
To stop and remove the docker container run `docker rm -f hdfs-local`
