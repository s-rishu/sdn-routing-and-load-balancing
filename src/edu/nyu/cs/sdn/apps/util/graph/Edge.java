package edu.nyu.cs.sdn.apps.util.graph;

public class Edge  {
    private final String id;
    private final Vertex source;
    private final Vertex destination;
    private final int weight;

    private final int srcPort;

    private final int destPort;

    public Edge(String id, Vertex source, Vertex destination, int weight, int sp, int dp) {
        this.id = id;
        this.source = source;
        this.destination = destination;
        this.weight = weight;
        this.srcPort = sp;
        this.destPort = dp;
    }

    public String getId() {
        return id;
    }
    public Vertex getDestination() {
        return destination;
    }

    public Vertex getSource() {
        return source;
    }
    public int getWeight() {
        return weight;
    }
    public int getDestPort() {
        return destPort;
    }
    public int getSrcPort() {
        return srcPort;
    }
    @Override
    public String toString() {
        return source + " " + destination;
    }


}
