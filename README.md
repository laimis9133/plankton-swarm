## Plankton Swarm
A simple alternative manual Ceph pg balancer.  
It will move a number of pgs from the most full OSDs to the least full OSDs taking into acount node failure domain.  

It is very simple and straightforward and I havent seen one written in bash so here you go. Hope it helps.  

Usage examples:
```   
  ./plankton-swarm.sh source-osds osd.1,osd.2 target-osds osd.3,osd.4 pgs 5
  - Moves 5 PGs from OSDs 1,2 to OSDs 3,4

  ./plankton-swarm.sh source-osds 85-90 5 target-osds lt60 pgs 4
  - Moves 4 PGs from 5 top OSDs between 85% and 90% to OSDs below 60% utilization

  ./plankton-swarm.sh source-osds 1,2
  - Move 3 pgs (default) from osd.1 and osd.2 to OSDs below 65% (default) utilization

  ./plankton-swarm.sh source-osds gt88 10 target-osds lt60 5 pgs 10 keep-upmaps
  - Moves 10 PGs from top 10 OSDs above 88% to 5 least utilized OSDs below 60%
  - Skips pgs that already have upmap

  ./plankton-swarm.sh help
  - Displays help and various other usage examples
```

This tool is harmless to run as it only generates an executable **swarm-file**.
You can run it directly:
```
./plankton-swarm.sh <your-options> && bash swarm-file
```

  
![A Swarm of Planktons](https://images.squarespace-cdn.com/content/v1/5bb9f390da50d330b261fdc8/1581283147743-OFR7YJ9ZF69KWW2ZYS2G/GPTempDownload+2.JPG)

Massive shoutouts to the famous [jj-balancer](https://github.com/TheJJ/ceph-balancer) and CERN's [upmap-remapped](https://github.com/cernceph/ceph-scripts/blob/master/tools/upmap/upmap-remapped.py) for the inspiration!

