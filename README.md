A simple alternative manual Ceph pg balancer.  
It will move a number of pgs from the most full OSDs to the least full OSDs taking into acount node failure domain.  

It is very simple and straightforward and I havent seen one written in bash so here you go. Hope it helps.  

Usage examples:
```
  ./plankton-swarm.sh 90 15 5 60
    - Detect OSDs above 90% utilization, move 15 PGs from each of the top 5 to OSDs below 60% utilization.

  ./plankton-swarm.sh 91 5 all
    - Detect all OSDs above 91% usage, move 5 PGs from each one to OSDs below 65% (default) utilization.

  ./plankton-swarm.sh source-osds osd.1,osd.2
    - Move 10 pgs (default) from osd.1 and osd.2 to OSDs below 65% (default) utilization.
```

This tool is harmless to run as it only generates an executable **swam-file**.
You can run it directly:
```
./plankton-swarm.sh <your-options> && bash swarm-file
```

  
![A Swarm of Planktons](https://images.squarespace-cdn.com/content/v1/5bb9f390da50d330b261fdc8/1581283147743-OFR7YJ9ZF69KWW2ZYS2G/GPTempDownload+2.JPG)



