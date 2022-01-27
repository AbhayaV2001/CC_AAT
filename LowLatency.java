package examples.org.cloudbus.cloudsim.examples;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

/**
 * A Self test simulation will show how to 10 cloudlets will be distributed among
 * 05 Vms with different BW requirements.
 */
public class LowLatency {

	/** The cloudlet list. */
	private static List<Cloudlet> cloudletList;

	/** The vmlist. */
	private static List<Vm> vmlist;

	private static List<Vm> createVM(int userId, int vms, int idShift) {
		//Creates a container to store VMs. This list is passed to the broker later
		LinkedList<Vm> list = new LinkedList<Vm>();

		//VM Parameters
		long size = 1000; 
		int ram = 512; 
		int mips = 250;
		long bw = 100;
		int pesNumber = 1; 
		String vmm = "Xen"; 

		//create VMs
		Vm[] vm = new Vm[vms];
		
		for(int i=0;i<vms;i++){
			vm[i] = new Vm(idShift + i, userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
			list.add(vm[i]);
		}

		return list;
	}

	private static List<Cloudlet> createCloudlet(int userId, int cloudlets, int idShift){
		// Creates a container to store Cloudlets
		LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

		//cloudlet parameters
		long length;
		long fileSize = 300;
		long outputSize = 300;
		int pesNumber = 1;
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Cloudlet[] cloudlet = new Cloudlet[cloudlets];

		for(int i=0;i<cloudlets;i++){
			length = (long) ((Math.random())*10000);
			cloudlet[i] = new Cloudlet(idShift + i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
			// setting the owner of these Cloudlets
			cloudlet[i].setUserId(userId);
			list.add(cloudlet[i]);
            Log.printLine(i+" cloudlet length = " +cloudlet[i].getCloudletTotalLength());
		}

		return list;
	}	
	
	// method to find the average response time in a virtual machine
	private static double VmArt(List<Cloudlet> list, int VmId)
	{
		int c = 0;
		double art = 0; 
		for(int i=0;i<list.size();i++)
			if (list.get(i).getVmId() == VmId)
			{
				art = art + list.get(i).getExecStartTime();    
				c++;
			}
			art =  art / c;
		return art;
	}
	
	/**
	 * Creates main() to run this example
	 */
	public static void main(String[] args) {
		Log.printLine("Starting CloudSim Low Latency Scheduling...\n");

		try {
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1;   // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false;  // mean trace events

			int vms = 5;
            int cloudlets = 10;
            
			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			Log.printLine();
			
			// Second step: Create Datacenters
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			Log.printLine();
			
			//Third step: Create Broker                     
			DatacenterBroker broker = createBroker("Broker_0");
			
			Log.printLine();
			
			int brokerId = broker.getId();

			//Fourth step: Create virtual machine
			vmlist  = createVM(brokerId, vms, 0); 
			
			Log.printLine();
			
			//submit vm list to the broker
			broker.submitVmList(vmlist);
			
			//Fifth step: Create Cloudlets
			cloudletList = createCloudlet(brokerId, cloudlets, 0); // creating 10 cloudlets			
			
			Log.printLine();
			
			// assign latency
            int[][] latency = new int[cloudlets][vms];
            
            Log.printLine("Latency:");
            for(int i=0;i<cloudlets;i++)
            {
            	for(int j=0;j<vms;j++)
            	{
            		latency[i][j] = (int) (Math.random()*100);
            		Log.print(latency[i][j]+" ");
            	}
            	Log.printLine();
            }
            
            Log.printLine();
			
			//submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);
			
			// to keep track of tasks assigned to VMs
			Dictionary<Integer, Long> busy = new Hashtable<Integer, Long>();
			
			for(int i=0;i<vms;i++)
			{
				busy.put(i, (long) 0); // initial 0
			}
			
			Log.printLine("Latency Rank Matrix:");
			
			for(int i=0;i<cloudlets;i++)
			{
				int min = 0;
				for(int j=1;j<vms;j++)
				{
					if(latency[i][min]>latency[i][j])
					{
						min = j;
					}
				}
				
				// assign cloudlet to vm
				broker.bindCloudletToVm(cloudletList.get(i).getCloudletId(),vmlist.get(min).getId());
			}
			
			Log.printLine();
			
			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			Log.printLine();
			
			CloudSim.stopSimulation();

			Log.printLine();
			
        	printCloudletList(cloudletList);
        	
        	Log.printLine();
        	
			Log.printLine("CloudSim Low Latency Scheduling finished!");
		}
		catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}
	}

	private static Datacenter createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store one or more Machines
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should create a list to store these PEs before creating a Machine.
		List<Pe> peList1 = new ArrayList<Pe>();

		int mips = 102400;

		// 3. Create PEs and add these into the list. for a quad-core machine, a list of 4 PEs is required:
		peList1.add(new Pe(0, new PeProvisionerSimple(mips))); 
		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(3, new PeProvisionerSimple(mips)));

		//4. Create Hosts with its id and list of PEs and add them to the list of machines
		int hostId=0;
		int ram = 102400;
		long storage = 1000000; 
		int bw = 200000;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList1,
    				new VmSchedulerTimeShared(peList1)
    			)
    		); 

		// 5. Create a DatacenterCharacteristics object that stores the properties of a data center: architecture, OS, list of
		//    Machines, allocation policy: time- or space-shared, time zone and its price (G$/Pe time unit).
		String arch = "x86";      
		String os = "Linux";          
		String vmm = "Xen";
		double time_zone = 10.0;         
		double cost = 3.0;              
		double costPerMem = 0.05;		
		double costPerStorage = 0.1;	
		double costPerBw = 0.1;			
		LinkedList<Storage> storageList = new LinkedList<Storage>();	

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}
	
	private static DatacenterBroker createBroker(String name){

		DatacenterBroker broker = null;
		try {
			broker = new DatacenterBroker(name);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}
	
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;
                
        double avrt = 0;
        double avwt = 0;
        double avrs = 0;
        
		Log.printLine();
		Log.printLine("==================================== OUTPUT ======================================");
		Log.printLine("CloudletID\tSTATUS\tDatacenterID\tVMID\tTime\tStartTime\tFinishTime");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print("\t"+ cloudlet.getCloudletId()+"\t");

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");
                avrt += cloudlet.getActualCPUTime();  
                avwt += cloudlet.getExecStartTime();
				Log.printLine("\t      "+cloudlet.getResourceId() + "\t\t  " + cloudlet.getVmId() +
						"\t" + dft.format(cloudlet.getActualCPUTime()) +
						"\t  " + dft.format(cloudlet.getExecStartTime())+ "\t\t" + dft.format(cloudlet.getFinishTime()));
				}
			else
			{
				Log.print("Failure");
			}
		}
		
		Log.printLine();
		
		for (int a=0; a<size;a++)
    		Log.printLine("Waiting Time for Task-" + list.get(a).getCloudletId() + "   =  " + dft.format(list.get(a).getExecStartTime()));
		
		Log.printLine();
		
		Log.printLine("\nAverage Waiting Time for Tasks = " + dft.format(avwt/size));
		
		Log.printLine();
	
		
		for (int a=0; a<vmlist.size();a++) 
		{
			avrs += VmArt( cloudletList, vmlist.get(a).getId());
    		Log.printLine("Average Response Time of Vm-" + vmlist.get(a).getId() + "   =  " + dft.format(VmArt( cloudletList, vmlist.get(a).getId())));
		}
		
		Log.printLine();
		
		Log.printLine("\nAverage Response Time of VMs = " + dft.format(avrs/vmlist.size()));
		
		Log.printLine();
		
		Log.printLine("\nAverage Execution Time = " + dft.format(avrt/size));

	}
}