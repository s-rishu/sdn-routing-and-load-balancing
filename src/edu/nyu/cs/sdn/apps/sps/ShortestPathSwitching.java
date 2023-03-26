package edu.nyu.cs.sdn.apps.sps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nyu.cs.sdn.apps.util.Host;
import edu.nyu.cs.sdn.apps.util.SwitchCommands;
import edu.nyu.cs.sdn.apps.util.Dijkstra;
import edu.nyu.cs.sdn.apps.util.graph.Graph;
import edu.nyu.cs.sdn.apps.util.graph.Vertex;
import edu.nyu.cs.sdn.apps.util.graph.Edge;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceListener;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.packet.Ethernet;

import org.openflow.protocol.OFMatch;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.openflow.protocol.instruction.OFInstruction;
import org.openflow.protocol.instruction.OFInstructionActions;
import org.openflow.protocol.instruction.OFInstructionApplyActions;

public class ShortestPathSwitching implements IFloodlightModule, IOFSwitchListener, 
		ILinkDiscoveryListener, IDeviceListener, InterfaceShortestPathSwitching
{
	public static final String MODULE_NAME = ShortestPathSwitching.class.getSimpleName();
	
	// Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);
    
    // Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;

    // Interface to link discovery service
    private ILinkDiscoveryService linkDiscProv;

    // Interface to device manager service
    private IDeviceService deviceProv;
    
    // Switch table in which rules should be installed
    private byte table;
    
    // Map of hosts to devices
    private Map<IDevice,Host> knownHosts;

	/**
     * Loads dependencies and initializes data structures.
     */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Initializing %s...", MODULE_NAME));
		Map<String,String> config = context.getConfigParams(this);
        this.table = Byte.parseByte(config.get("table"));
        
		this.floodlightProv = context.getServiceImpl(
				IFloodlightProviderService.class);
        this.linkDiscProv = context.getServiceImpl(ILinkDiscoveryService.class);
        this.deviceProv = context.getServiceImpl(IDeviceService.class);
        
        this.knownHosts = new ConcurrentHashMap<IDevice,Host>();
        
        /*********************************************************************/
        /* TODO: Initialize other class variables, if necessary              */
        
        /*********************************************************************/
	}

	/**
     * Subscribes to events and performs other startup tasks.
     */
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException 
	{
		log.info(String.format("Starting %s...", MODULE_NAME));
		this.floodlightProv.addOFSwitchListener(this);
		this.linkDiscProv.addListener(this);
		this.deviceProv.addListener(this);
		
		/*********************************************************************/
		/* TODO: Perform other tasks, if necessary                           */
		
		/*********************************************************************/
	}
	
	/**
	 * Get the table in which this application installs rules.
	 */
	public byte getTable()
	{ return this.table; }
	
    /**
     * Get a list of all known hosts in the network.
     */
    private Collection<Host> getHosts()
    { return this.knownHosts.values(); }
	
    /**
     * Get a map of all active switches in the network. Switch DPID is used as
     * the key.
     */
	private Map<Long, IOFSwitch> getSwitches()
    { return floodlightProv.getAllSwitchMap(); }
	
    /**
     * Get a list of all active links in the network.
     */
    private Collection<Link> getLinks()
    { return linkDiscProv.getLinks().keySet(); }
	/**
	 * Format MAC address to the standard MAC format.
	 */
	private String formatMAC(Long ma)
	{
		StringBuilder na = new StringBuilder(Long.toHexString(ma));
		while(na.length() < 12)
		{
			na.insert(0, "0");
		}
		String newmac = na.toString();
		newmac = newmac.replaceAll("(.{2})", "$1" + ':').substring(0,17);
		return newmac;
	}

	private void updateRulesSingleHost(Host host){
		if (host.isAttachedToSwitch() == true) {
			IOFSwitch host_sw = host.getSwitch();
			Long host_sw_id = host_sw.getId();

			//match criteria of the host added
			OFMatch match = new OFMatch();
			match.setDataLayerType(Ethernet.TYPE_IPv4);
			match.setDataLayerDestination(this.formatMAC(host.getMACAddress()));

			//create a graph of switches
			log.info("Creating graph of switches.");
			ArrayList<Vertex> nodes = new ArrayList<Vertex>();
			ArrayList<Edge> edges = new ArrayList<Edge>();
			Vertex source;
			Map<Long, IOFSwitch> all_sw = this.getSwitches();
			Map<Long, Vertex> sw_to_v = new HashMap<Long, Vertex>();
			for (Long id : all_sw.keySet()) {
				Vertex v = new Vertex(id, id);
				nodes.add(v);
				sw_to_v.put(id, v);

			}
			source = sw_to_v.get(host_sw_id);
			//add all the links as edges of the graph
			Collection<Link> all_links = this.getLinks();
			for (Link l : all_links) {    //only switch to switch links
				String id = String.valueOf(l.getSrc()) + String.valueOf(l.getDst());
				Edge e = new Edge(id, sw_to_v.get(l.getSrc()), sw_to_v.get(l.getDst()), 1, l.getSrcPort(), l.getDstPort());
				edges.add(e);
			}
			Graph graph = new Graph(nodes, edges);

			//run dikstras algorithm
			log.info("Running Dijkstra's from source switch.");
			Dijkstra d = new Dijkstra(graph);
			d.execute(source);

			//update rules
			log.info(String.format("Installing rules for the newly added host: %s", host.getName()));
			for (Long id : all_sw.keySet()) {
				log.info(String.format("Installing rules for host %s in switch s%d.", host.getName(), id));
				IOFSwitch sw = all_sw.get(id);
				OFAction a;
				if (id == host_sw_id) {
					a = new OFActionOutput(host.getPort());
				} else {
					a = new OFActionOutput(d.getPredecessorPort(sw_to_v.get(id)));
				}
				OFInstruction i = new OFInstructionApplyActions(Arrays.asList(a));
				SwitchCommands.installRule(sw, this.getTable(), SwitchCommands.DEFAULT_PRIORITY, match, Arrays.asList(i));
			}
		}

	}

	private void updateRulesAllHosts(){
		for(Host host : this.getHosts()){
			this.updateRulesSingleHost(host);
		}
	}

    /**
     * Event handler called when a host joins the network.
     * @param device information about the host
     */
	@Override
	public void deviceAdded(IDevice device) 
	{
		Host host = new Host(device, this.floodlightProv);
		// We only care about a new host if we know its IP
		if (host.getIPv4Address() != null) {
			log.info(String.format("Host %s added", host.getName()));
			this.knownHosts.put(device, host);

			/*****************************************************************/
			/* TODO: Update routing: add rules to route to new host          */
			//update all switches using new host's switch shortest path info
			/*****************************************************************/
			this.updateRulesSingleHost(host);
		}

	}


	/**
     * Event handler called when a host is no longer attached to a switch.
     * @param device information about the host
     */
	@Override
	public void deviceRemoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{
			host = new Host(device, this.floodlightProv);
			this.knownHosts.put(device, host);
		}
		
		log.info(String.format("Host %s is no longer attached to a switch", 
				host.getName()));
		
		/*********************************************************************/
		/* TODO: Update routing: remove rules to route to host               */
		//remove all rules with the removed host as destination
		/*********************************************************************/
		//create match criteria
		log.info(String.format("Creating match criteria for the removed host ", host.getName()));
		log.info(String.format("MAC address of the removed host: ", host.getMACAddress()));
		OFMatch match = new OFMatch();
		match.setDataLayerType(Ethernet.TYPE_IPv4);
		match.setDataLayerDestination(this.formatMAC(host.getMACAddress()));

		//remove rules for all switches that connect to the removed host
		log.info("Removing required rules..");
		Map<Long, IOFSwitch> all_sw = this.getSwitches();
		for(Long id : all_sw.keySet()){
			IOFSwitch sw = all_sw.get(id);
			SwitchCommands.removeRules(sw, table, match);

		}
	}

	/**
     * Event handler called when a host moves within the network.
     * @param device information about the host
     */
	@Override
	public void deviceMoved(IDevice device) 
	{
		Host host = this.knownHosts.get(device);
		if (null == host)
		{
			host = new Host(device, this.floodlightProv);
			this.knownHosts.put(device, host);
		}

		if (!host.isAttachedToSwitch())
		{
			this.deviceRemoved(device);
			return;
		}
		log.info(String.format("Host %s moved to s%d:%d", host.getName(),
				host.getSwitch().getId(), host.getPort()));

		this.updateRulesSingleHost(host);
		
		/*********************************************************************/
		/* TODO: Update routing: change rules to route to host               */
		//update all removed host rules with shortest path info of the new switch
		/*********************************************************************/
	}
	
    /**
     * Event handler called when a switch joins the network.
     * @param DPID for the switch
     */
	@Override		
	public void switchAdded(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d added", switchId));
		this.updateRulesAllHosts();
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */

		/*********************************************************************/
	}

	/**
	 * Event handler called when a switch leaves the network.
	 * @param DPID for the switch
	 */
	@Override
	public void switchRemoved(long switchId) 
	{
		IOFSwitch sw = this.floodlightProv.getSwitch(switchId);
		log.info(String.format("Switch s%d removed", switchId));
		this.updateRulesAllHosts();
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		//remove rules with destination as removed switch or any of the connected hosts
		/*********************************************************************/

	}

	/**
	 * Event handler called when multiple links go up or down.
	 * @param updateList information about the change in each link's state
	 */
	@Override
	public void linkDiscoveryUpdate(List<LDUpdate> updateList) 
	{
		for (LDUpdate update : updateList)
		{
			// If we only know the switch & port for one end of the link, then
			// the link must be from a switch to a host
			if (0 == update.getDst())
			{
				log.info(String.format("Link s%s:%d -> host updated", 
					update.getSrc(), update.getSrcPort()));
				//find updated host
				log.info("Finding updated host..");
				for(Host h : this.getHosts()){
					if(h.getIPv4Address() == null || h.isAttachedToSwitch() == false)
					{
						continue;
					}
					if(h.getSwitch().getId()==update.getSrc() && h.getPort()==update.getSrcPort()){
						Host host = h;
						log.info("Host found. Updating Rules..");
						this.updateRulesSingleHost(host);
						break;
					}
				}

			}
			// Otherwise, the link is between two switches
			else
			{
				log.info(String.format("Link s%s:%d -> %s:%d updated", 
					update.getSrc(), update.getSrcPort(),
					update.getDst(), update.getDstPort()));
				this.updateRulesAllHosts();
			}
		}
		
		/*********************************************************************/
		/* TODO: Update routing: change routing rules for all hosts          */
		/*********************************************************************/
	}

	/**
	 * Event handler called when link goes up or down.
	 * @param update information about the change in link state
	 */
	@Override
	public void linkDiscoveryUpdate(LDUpdate update) 
	{ this.linkDiscoveryUpdate(Arrays.asList(update)); }
	
	/**
     * Event handler called when the IP address of a host changes.
     * @param device information about the host
     */
	@Override
	public void deviceIPV4AddrChanged(IDevice device) 
	{ this.deviceAdded(device); }

	/**
     * Event handler called when the VLAN of a host changes.
     * @param device information about the host
     */
	@Override
	public void deviceVlanChanged(IDevice device) 
	{ /* Nothing we need to do, since we're not using VLANs */ }
	
	/**
	 * Event handler called when the controller becomes the master for a switch.
	 * @param DPID for the switch
	 */
	@Override
	public void switchActivated(long switchId) 
	{ /* Nothing we need to do, since we're not switching controller roles */ }

	/**
	 * Event handler called when some attribute of a switch changes.
	 * @param DPID for the switch
	 */
	@Override
	public void switchChanged(long switchId) 
	{ /* Nothing we need to do */ }
	
	/**
	 * Event handler called when a port on a switch goes up or down, or is
	 * added or removed.
	 * @param DPID for the switch
	 * @param port the port on the switch whose status changed
	 * @param type the type of status change (up, down, add, remove)
	 */
	@Override
	public void switchPortChanged(long switchId, ImmutablePort port,
			PortChangeType type) 
	{ /* Nothing we need to do, since we'll get a linkDiscoveryUpdate event */ }

	/**
	 * Gets a name for this module.
	 * @return name for this module
	 */
	@Override
	public String getName() 
	{ return this.MODULE_NAME; }

	/**
	 * Check if events must be passed to another module before this module is
	 * notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPrereq(String type, String name) 
	{ return false; }

	/**
	 * Check if events must be passed to another module after this module has
	 * been notified of the event.
	 */
	@Override
	public boolean isCallbackOrderingPostreq(String type, String name) 
	{ return false; }
	
    /**
     * Tell the module system which services we provide.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() 
	{
		Collection<Class<? extends IFloodlightService>> services =
					new ArrayList<Class<? extends IFloodlightService>>();
		services.add(InterfaceShortestPathSwitching.class);
		return services; 
	}

	/**
     * Tell the module system which services we implement.
     */
	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> 
			getServiceImpls() 
	{ 
        Map<Class<? extends IFloodlightService>, IFloodlightService> services =
        			new HashMap<Class<? extends IFloodlightService>, 
        					IFloodlightService>();
        // We are the class that implements the service
        services.put(InterfaceShortestPathSwitching.class, this);
        return services;
	}

	/**
     * Tell the module system which modules we depend on.
     */
	@Override
	public Collection<Class<? extends IFloodlightService>> 
			getModuleDependencies() 
	{
		Collection<Class<? extends IFloodlightService >> modules =
	            new ArrayList<Class<? extends IFloodlightService>>();
		modules.add(IFloodlightProviderService.class);
		modules.add(ILinkDiscoveryService.class);
		modules.add(IDeviceService.class);
        return modules;
	}
}
