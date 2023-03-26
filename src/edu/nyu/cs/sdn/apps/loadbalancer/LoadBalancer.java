package edu.nyu.cs.sdn.apps.loadbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Arrays;

import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.nyu.cs.sdn.apps.sps.InterfaceShortestPathSwitching;
import edu.nyu.cs.sdn.apps.util.ArpServer;
import edu.nyu.cs.sdn.apps.util.SwitchCommands;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch.PortChangeType;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.ImmutablePort;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.internal.DeviceManagerImpl;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.util.MACAddress;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.IPv4;

import org.openflow.protocol.*;
import org.openflow.protocol.action.*;
import org.openflow.protocol.instruction.*;

public class LoadBalancer implements IFloodlightModule, IOFSwitchListener,
        IOFMessageListener, InterfaceLoadBalancer
{
    public static final String MODULE_NAME = LoadBalancer.class.getSimpleName();

    private static final byte TCP_FLAG_SYN = 0x02;
    private static final byte TCP_FLAG_RESET = 0x04;

    private static final short IDLE_TIMEOUT = 20;

    // Interface to the logging system
    private static Logger log = LoggerFactory.getLogger(MODULE_NAME);

    // Interface to Floodlight core for interacting with connected switches
    private IFloodlightProviderService floodlightProv;

    // Interface to device manager service
    private IDeviceService deviceProv;

    // Interface to L3Routing application
    private InterfaceShortestPathSwitching l3RoutingApp;

    // Switch table in which rules should be installed
    private byte table;

    // Set of virtual IPs and the load balancer instances they correspond with
    private Map<Integer,LoadBalancerInstance> instances;

    /**
     * Get the table in which this application installs rules.
     */
    public byte getTable()
    { return this.table; }

    /**
     * Loads dependencies and initializes data structures.
     */
    @Override
    public void init(FloodlightModuleContext context)
            throws FloodlightModuleException
    {
        log.info(String.format("Initializing %s...", MODULE_NAME));

        // Obtain table number from config
        Map<String,String> config = context.getConfigParams(this);
        this.table = Byte.parseByte(config.get("table"));

        // Create instances from config
        this.instances = new HashMap<Integer,LoadBalancerInstance>();
        String[] instanceConfigs = config.get("instances").split(";");
        for (String instanceConfig : instanceConfigs)
        {
            String[] configItems = instanceConfig.split(" ");
            if (configItems.length != 3)
            {
                log.error("Ignoring bad instance config: " + instanceConfig);
                continue;
            }
            LoadBalancerInstance instance = new LoadBalancerInstance(
                    configItems[0], configItems[1], configItems[2].split(","));
            this.instances.put(instance.getVirtualIP(), instance);
            log.info("Added load balancer instance: " + instance);
        }

        this.floodlightProv = context.getServiceImpl(
                IFloodlightProviderService.class);
        this.deviceProv = context.getServiceImpl(IDeviceService.class);
        this.l3RoutingApp = context.getServiceImpl(InterfaceShortestPathSwitching.class);
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
        this.floodlightProv.addOFMessageListener(OFType.PACKET_IN, this);

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

        OFMatch match;
        OFAction action;
        ArrayList<OFAction> actions;
        OFInstruction i;
        ArrayList<OFInstruction> inst;

        //rulkes to send packets from new connections to each virtual load balancer IP to the controller
        log.info("Installing load balancer rules..");
        for (Integer id : this.instances.keySet()){
            LoadBalancerInstance ins = this.instances.get(id);

            match = new OFMatch();
            match.setDataLayerType((short)0x800);
            match.setNetworkDestination(OFMatch.ETH_TYPE_IPV4), ins.getVirtualIP());
            action = new OFActionOutput(OFPort.OFPP_CONTROLLER);
            i = new OFInstructionApplyActions(Arrays.asList(action));
            SwitchCommands.installRule(sw, table, SwitchCommands.DEFAULT_PRIORITY, match, Arrays.asList(i));

        }

        //install arp forwarding rule
        log.info("Installing arp forwarding to controller rule..");
        for (Integer id : this.instances.keySet()) {
            LoadBalancerInstance ins = this.instances.get(id);
            OFMatchField fieldEthTypeARP = new OFMatchField(OFOXMFieldType.ETH_TYPE, Ethernet.TYPE_ARP);
            OFMatchField fieldARPsIP = new OFMatchField(OFOXMFieldType.ARP_TPA, id);

            ArrayList<OFMatchField> matchFieldsARPPackets = new ArrayList<OFMatchField>();
            matchFieldsARPPackets.add(fieldEthTypeARP);
            matchFieldsARPPackets.add(fieldARPsIP);

            match = new OFMatch();
            match.setMatchFields(matchFieldsARPPackets);

            action = new OFActionOutput(OFPort.OFPP_CONTROLLER);
            actions = new ArrayList<OFAction>();
            actions.add(action);
            i = new OFInstructionApplyActions(actions);
            SwitchCommands.installRule(sw, table, (short)(SwitchCommands.DEFAULT_PRIORITY+1), match, Arrays.asList(i));
        }

        //install rules to pass all other packets to the next routing table
        log.info("Installing table forwarding rules..");
        match = new OFMatch();
        match.setDataLayerType((short)0x800);
        if (this.l3RoutingApp.getTable() == 0){
            log.info("Table is null");
        }
        OFInstructionGotoTable itable = new OFInstructionGotoTable(this.l3RoutingApp.getTable());
        inst = new ArrayList<OFInstruction>();
        inst.add(itable);
        SwitchCommands.installRule(sw, table, SwitchCommands.DEFAULT_PRIORITY, match, inst);

    }

    /**
     * Handle incoming packets sent from switches.
     * @param sw switch on which the packet was received
     * @param msg message from the switch
     * @param cntx the Floodlight context in which the message should be handled
     * @return indication whether another module should also process the packet
     */
    @Override
    public net.floodlightcontroller.core.IListener.Command receive(
            IOFSwitch sw, OFMessage msg, FloodlightContext cntx)
    {
        // We're only interested in packet-in messages
        if (msg.getType() != OFType.PACKET_IN)
        { return Command.CONTINUE; }
        OFPacketIn pktIn = (OFPacketIn)msg;

        // Handle the packet
        Ethernet ethPkt = new Ethernet();
        ethPkt.deserialize(pktIn.getPacketData(), 0,
                pktIn.getPacketData().length);

        /*********************************************************************/
        /*       Send an ARP reply for ARP requests for virtual IPs; for TCP */
        /*       SYNs sent to a virtual IP, select a host and install        */
        /*       connection-specific rules to rewrite IP and MAC addresses;  */
        /*       for all other TCP packets sent to a virtual IP, send a TCP  */
        /*       reset; ignore all other packets                             */

        /*********************************************************************/

        if (ethPkt.getEtherType() == Ethernet.TYPE_ARP) { //send ARP reply
            ARP arp = (ARP)ethPkt.getPayload();
            //int deviceIp = IPv4.toIPv4Address(arp.getTargetProtocolAddress());

            //filter ARP requests
            if (arp.getOpCode() == ARP.OP_REQUEST && arp.getProtocolType() == ARP.PROTO_TYPE_IP) {
                log.info("Receiveed ARP request..");
                int destip = IPv4.toIPv4Address(arp.getTargetProtocolAddress());
                LoadBalancerInstance ins = this.instances.get(destip);
                int deviceIp = ins.getNextHostIP();
                //byte[] deviceMac = ins.getVirtualMAC();

                log.info(String.format("Received ARP request for %s from %s",
                        IPv4.fromIPv4Address(deviceIp),
                        MACAddress.valueOf(arp.getSenderHardwareAddress()).toString()));
                Iterator<? extends IDevice> deviceIterator =
                        this.deviceProv.queryDevices(null, null, deviceIp, null, null);
                if (!deviceIterator.hasNext())
                { return Command.CONTINUE; }

                // Create ARP reply
                IDevice device = deviceIterator.next();
                byte[] deviceMac = MACAddress.valueOf(device.getMACAddress()).toBytes();

                log.info("Building ARP packet..");
                ARP arpSend = new ARP();
                arpSend.setHardwareType(ARP.HW_TYPE_ETHERNET);
                arpSend.setProtocolType(ARP.PROTO_TYPE_IP);
                arpSend.setHardwareAddressLength((byte)Ethernet.DATALAYER_ADDRESS_LENGTH);
                arpSend.setProtocolAddressLength((byte) 4);
                arpSend.setOpCode(ARP.OP_REPLY);
                log.info(Arrays.toString(deviceMac));
                arpSend.setSenderHardwareAddress(deviceMac);
                arpSend.setSenderProtocolAddress(destip);
                arpSend.setTargetHardwareAddress(arp.getSenderHardwareAddress());
                arpSend.setTargetProtocolAddress(arp.getSenderProtocolAddress());

                log.info("Building Ethernet packet..");
                Ethernet sendPkt = new Ethernet();
                sendPkt.setEtherType(Ethernet.TYPE_ARP);
                sendPkt.setDestinationMACAddress(ethPkt.getSourceMACAddress());
                sendPkt.setSourceMACAddress(deviceMac);
                sendPkt.setPayload(arpSend);

                log.info("Sending ARP reply..");
                SwitchCommands.sendPacket(sw, (short) pktIn.getInPort(), sendPkt);
                log.info("ARP reply sent.");
                return Command.STOP;
            }
            else if (ethPkt.getEtherType() == Ethernet.TYPE_IPv4) { //IP/MAC rewrite rules for TCP syn connections
                log.info("Received TCP packet..");
                IPv4 ip = (IPv4)ethPkt.getPayload();
                //filter TCP requests
                if (ip.getProtocol() == IPv4.PROTOCOL_TCP) {
                    TCP tcp = (TCP)ip.getPayload();
                    //filter TCP syn requests
                    if (tcp.getFlags() == TCP_FLAG_SYN) {
                        //install rules to rewrite destination ip/mac for client to server
                        log.info("Installing ip/mac rewrite rules..");
                        OFMatch match = new OFMatch(); //match criteria
                        match.setDataLayerType((short)0x800);
                        match.setNetworkProtocol(IPv4.PROTOCOL_TCP);
                        match.setNetworkSource(OFMatch.ETH_TYPE_IPV4, ip.getSourceAddress());
                        match.setNetworkDestination(OFMatch.ETH_TYPE_IPV4, ip.getDestinationAddress());
                        match.setTransportSource(tcp.getSourcePort());
                        match.setTransportDestination(tcp.getDestinationPort());

                        LoadBalancerInstance instance = instances.get(ip.getDestinationAddress());
                        int nextIP = instance.getNextHostIP();

                        OFActionSetField set_mac = new OFActionSetField(OFOXMFieldType.ETH_DST, getHostMACAddress(nextIP));
                        OFActionSetField set_ip = new OFActionSetField(OFOXMFieldType.IPV4_DST, nextIP);
                        List<OFAction> actions = new ArrayList<OFAction>();
                        actions.add(set_mac);
                        actions.add(set_ip);

                        List<OFInstruction> i = new ArrayList<OFInstruction>();
                        OFInstructionApplyActions ins = new OFInstructionApplyActions(actions);
                        OFInstructionGotoTable fwd_table = new OFInstructionGotoTable(this.l3RoutingApp.getTable());
                        i.add(ins);
                        i.add(fwd_table);
                        SwitchCommands.installRule(sw, table, SwitchCommands.MAX_PRIORITY, match, i, (short)IDLE_TIMEOUT, (short)IDLE_TIMEOUT);

                        //install rules to rewrite source ip/mac for server to client
                        //update match criteria
                        match.setNetworkSource(OFMatch.ETH_TYPE_IPV4, nextIP);
                        match.setNetworkDestination(OFMatch.ETH_TYPE_IPV4, ip.getSourceAddress());
                        match.setTransportSource(tcp.getDestinationPort());
                        match.setTransportDestination(tcp.getSourcePort());

                        set_mac = new OFActionSetField(OFOXMFieldType.ETH_SRC, instance.getVirtualMAC());
                        set_ip = new OFActionSetField(OFOXMFieldType.IPV4_SRC, instance.getVirtualIP());
                        actions = new ArrayList<OFAction>();
                        actions.add(set_mac);
                        actions.add(set_ip);

                        i = new ArrayList<OFInstruction>();
                        ins = new OFInstructionApplyActions(actions);
                        fwd_table = new OFInstructionGotoTable(this.l3RoutingApp.getTable());
                        i.add(ins);
                        i.add(fwd_table);
                        SwitchCommands.installRule(sw, table, SwitchCommands.MAX_PRIORITY, match, i, (short)20, (short)20);

                    }
                    else{
                        //send tcp reset if other tcp requests received..
                        log.info("TCP request is not SYN, creating a TCP RESET packet..");
                        //create tcp reset packet
                        tcp.setSourcePort(tcp.getDestinationPort());
                        tcp.setDestinationPort(tcp.getSourcePort());
                        tcp.setFlags((short) TCP_FLAG_RESET);
                        tcp.setSequence(tcp.getAcknowledge());
                        tcp.setWindowSize((short) 0);
                        tcp.setChecksum((short) 0);
                        tcp.serialize();

                        //create ip packet
                        ip.setPayload(tcp);
                        int destIp = ip.getSourceAddress();
                        int srcIp = ip.getDestinationAddress();
                        ip.setSourceAddress(srcIp);
                        ip.setDestinationAddress(destIp);
                        ip.setChecksum((short) 0);
                        ip.serialize();

                        //create ethernet packet
                        ethPkt.setPayload(ip);
                        byte[] destMac = ethPkt.getSourceMACAddress();
                        byte[] srcMac = ethPkt.getDestinationMACAddress();
                        ethPkt.setSourceMACAddress(srcMac);
                        ethPkt.setDestinationMACAddress(destMac);

                        log.info("Sending TCP RESET to the switch..");
                        short port = (short) pktIn.getInPort();
                        SwitchCommands.sendPacket(sw, port, ethPkt);
                    }

                }
            }
        }


        return Command.STOP;
    }

    /**
     * Returns the MAC address for a host, given the host's IP address.
     * @param hostIPAddress the host's IP address
     * @return the hosts's MAC address, null if unknown
     */
    private byte[] getHostMACAddress(int hostIPAddress)
    {
        Iterator<? extends IDevice> iterator = this.deviceProv.queryDevices(
                null, null, hostIPAddress, null, null);
        if (!iterator.hasNext())
        { return null; }
        IDevice device = iterator.next();
        return MACAddress.valueOf(device.getMACAddress()).toBytes();
    }

    /**
     * Event handler called when a switch leaves the network.
     * @param DPID for the switch
     */
    @Override
    public void switchRemoved(long switchId)
    { /* Nothing we need to do, since the switch is no longer active */ }

    /**
     * Event handler called when the controller becomes the master for a switch.
     * @param DPID for the switch
     */
    @Override
    public void switchActivated(long switchId)
    { /* Nothing we need to do, since we're not switching controller roles */ }

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
    { /* Nothing we need to do, since load balancer rules are port-agnostic */}

    /**
     * Event handler called when some attribute of a switch changes.
     * @param DPID for the switch
     */
    @Override
    public void switchChanged(long switchId)
    { /* Nothing we need to do */ }

    /**
     * Tell the module system which services we provide.
     */
    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices()
    { return null; }

    /**
     * Tell the module system which services we implement.
     */
    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService>
    getServiceImpls()
    { return null; }

    /**
     * Tell the module system which modules we depend on.
     */
    @Override
    public Collection<Class<? extends IFloodlightService>>
    getModuleDependencies()
    {
        Collection<Class<? extends IFloodlightService >> floodlightService =
                new ArrayList<Class<? extends IFloodlightService>>();
        floodlightService.add(IFloodlightProviderService.class);
        floodlightService.add(IDeviceService.class);
        return floodlightService;
    }

    /**
     * Gets a name for this module.
     * @return name for this module
     */
    @Override
    public String getName()
    { return MODULE_NAME; }

    /**
     * Check if events must be passed to another module before this module is
     * notified of the event.
     */
    @Override
    public boolean isCallbackOrderingPrereq(OFType type, String name)
    {
        return (OFType.PACKET_IN == type
                && (name.equals(ArpServer.MODULE_NAME)
                || name.equals(DeviceManagerImpl.MODULE_NAME)));
    }

    /**
     * Check if events must be passed to another module after this module has
     * been notified of the event.
     */
    @Override
    public boolean isCallbackOrderingPostreq(OFType type, String name)
    { return false; }
}
