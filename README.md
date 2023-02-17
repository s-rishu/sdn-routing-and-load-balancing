# sdn-routing-and-load-balancing
In this project we implement the following SDN controllers -

Shortest Path Switching Controller: This will be our layer 3 routing controller application which is responsible for building a global shortest-path switching table and forwarding rules for each switch in our network. To build this application we assume that we have access to global topology information. The routing rules will route data packets based on the shortest (least cost) path between the hosts. 

Load Balancer: A SDN controller application can also be used to work as a distributed load balancer, and here we attempt to do the same. We use virtual IPs to test our application against. The load balancer is responsible for rewriting the destination address to one of the actual hosts (after choosing from a set of mapped hosts) when a client sends a packet meant for load balancer IP. Vice versa, when a packet is sent from the server to the client, the load balancer is responsible to rewrite the source address to its own. 

Apart from the above mentioned, we also provide functionality for dependencies such as handling ARP requests which will be discussed when we go into the implementation details. We are going to be building on top of Floodlight - a Java based OpenFlow controller and will use Mininet to simulate networks and test our implementation. 

# How to run?
To run just the shortest path switching controller, type the below command in your terminal-

```java -jar FloodlightWithApps.jar -cf shortestPathSwitching.prop```

To run load balancing along with shortest path switching, type the below command in your terminal-

```java -jar FloodlightWithApps.jar -cf loadBalancer.prop```

Once the control has started runnning, start a mininet network in a seperate terminal-

```sudo run ./run_mininet.py mesh,5```
