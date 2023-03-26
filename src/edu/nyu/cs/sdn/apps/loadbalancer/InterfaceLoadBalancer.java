package edu.nyu.cs.sdn.apps.loadbalancer;

import net.floodlightcontroller.core.module.IFloodlightService;

public interface InterfaceLoadBalancer extends IFloodlightService
{
    /**
     * Get the table in which this application installs rules.
     */
    public byte getTable();
}