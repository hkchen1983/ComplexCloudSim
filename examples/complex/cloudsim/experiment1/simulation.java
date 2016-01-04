package complex.cloudsim.experiment1;

import complex.cloudsim.utils.Statistics;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.workflowsim.*;
import org.workflowsim.utils.ClusteringParameters;
import org.workflowsim.utils.OverheadParameters;
import org.workflowsim.utils.Parameters;
import org.workflowsim.utils.ReplicaCatalog;
import complex.cloudsim.*;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by JIA on 02/01/2016.
 * Compare the complexity of different scheduling algorithms in Homogeneous cloud
 * Scheduling algorithms: FCFS,RoundRobin, MinMin, MaxMin
 * Workload Montage 1000
 * Random generated runtime damage (within ratio of 0-0.1, 0-0.2, 0-0.3, 0-0.4, 0-0.5) apply to vm which result in PE's MIPS decrease
 * To see how it affect the completion time of the total workload? Minimum, Maximum, Mean, Median, Variance, Std
 */
public class simulation {

    public static int numVMs=5;
    public static int numRuns=100;
    public static double damageRatio=0.1;
    public static double[] FCFSResult = new double[numRuns];
    public static double[] RoundRobinResult = new double[numRuns];
    public static double[] MinMinResult = new double[numRuns];
    public static double[] MaxMinResult = new double[numRuns];

    public static void main(String[] args) {
        try {
            Log.disable();
            // First step: Initialize the WorkflowSim package.
            /**
             * Should change this based on real physical path
             */
            String daxPath = "E:\\PhD\\ComplexCloudSim\\config\\dax\\Montage_1000.xml";
            File daxFile = new File(daxPath);
            if (!daxFile.exists()) {
                Log.printLine("Warning: Please replace daxPath with the physical path in your working environment!");
                return;
            }
            Parameters.PlanningAlgorithm pln_method = Parameters.PlanningAlgorithm.INVALID;
            ReplicaCatalog.FileSystem file_system = ReplicaCatalog.FileSystem.SHARED;
            OverheadParameters op = new OverheadParameters(0, null, null, null, null, 0);
            ClusteringParameters.ClusteringMethod method = ClusteringParameters.ClusteringMethod.NONE;
            ClusteringParameters cp = new ClusteringParameters(0, 0, method, null);

            // For each scheduling algorithm (FCFS,RR,MinMin,MaxMin), run 100 times
            for (int sche = 0;sche<4;sche++){
                Parameters.SchedulingAlgorithm sch_method;
                switch (sche) {
                    case 0:
                        sch_method = Parameters.SchedulingAlgorithm.FCFS;
                        break;
                    case 1:
                        sch_method = Parameters.SchedulingAlgorithm.ROUNDROBIN;
                        break;
                    case 2:
                        sch_method = Parameters.SchedulingAlgorithm.MINMIN;
                        break;
                    case 3:
                        sch_method = Parameters.SchedulingAlgorithm.MAXMIN;
                        break;
                    default:
                        sch_method = Parameters.SchedulingAlgorithm.FCFS;
                }
                for (int runs=0;runs < numRuns;runs++){
                    Parameters.init(numVMs, daxPath, null,
                            null, op, cp, sch_method, pln_method,
                            null, 0);
                    ReplicaCatalog.init(file_system);

                    // before creating any entities.
                    int num_user = 1;   // number of grid users
                    Calendar calendar = Calendar.getInstance();
                    boolean trace_flag = false;  // mean trace events

                    // Initialize the CloudSim library
                    CloudSim.init(num_user, calendar, trace_flag);

                    ComplexDatacenter datacenter0 = createDatacenter("Datacenter_0");

                    /**
                     * Create a WorkflowPlanner with one schedulers.
                     */
                    WorkflowPlanner wfPlanner = new WorkflowPlanner("planner_0", 1);
                    /**
                     * Create a WorkflowEngine.
                     */
                    WorkflowEngine wfEngine = wfPlanner.getWorkflowEngine();
                    /**
                     * Create a list of VMs.The userId of a vm is basically the id of
                     * the scheduler that controls this vm.
                     */
                    List<ComplexVM> vmlist0 = createVM(wfEngine.getSchedulerId(0));

                    /**
                     * Submits this list of vms to this WorkflowEngine.
                     */
                    wfEngine.submitVmList(vmlist0, 0);

                    /**
                     * Binds the data centers with the scheduler.
                     */
                    wfEngine.bindSchedulerDatacenter(datacenter0.getId(), 0);
                    CloudSim.startSimulation();
                    List<Job> outputList0 = wfEngine.getJobsReceivedList();
                    CloudSim.stopSimulation();
                    switch (sche) {
                        case 0:
                            FCFSResult[runs]=wfEngine.getWorkflowFinishTime();
                            break;
                        case 1:
                            RoundRobinResult[runs]=wfEngine.getWorkflowFinishTime();
                            break;
                        case 2:
                            MinMinResult[runs]=wfEngine.getWorkflowFinishTime();
                            break;
                        case 3:
                            MaxMinResult[runs]=wfEngine.getWorkflowFinishTime();
                            break;
                        default:
                            FCFSResult[runs]=wfEngine.getWorkflowFinishTime();
                            break;
                    }

                }
                Log.enable();
                Log.printLine("------ "+ numVMs + " VMs " + numRuns + " Runs with Damage Ratio "+ damageRatio + "------");
                Log.printLine( ">> FCFS");
                printResult(FCFSResult);
                Log.printLine( ">> RoundRobin");
                printResult(RoundRobinResult);
                Log.printLine( ">> MinMin");
                printResult(MinMinResult);
                Log.printLine( ">> MaxMin");
                printResult(MaxMinResult);
            }
        } catch (Exception e) {
            Log.printLine("The simulation has been terminated due to an unexpected error");
        }
    }

    protected static List<ComplexVM> createVM(int userId) {
        //Creates a container to store VMs. This list is passed to the broker later
        LinkedList<ComplexVM> list = new LinkedList<>();

        //VM Parameters
        long size = 10000; //image size (MB)
        int ram = 4000; //vm memory (MB)
        int mips = 8000;
        long bw = 1000;
        int pesNumber = 8; //number of cpus
        String vmm = "Xen"; //VMM name

        //create VMs
        ComplexVM[] vm = new ComplexVM[numVMs];
        for (int i = 0; i < numVMs; i++) {
            double ratio = 1.0;
            vm[i] = new ComplexVM(i, userId, mips * ratio, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
            vm[i].setDamageRatio(damageRatio);
            list.add(vm[i]);
        }
        return list;
    }

    ////////////////////////// STATIC METHODS ///////////////////////
    /**
     * Creates main() to run this example This example has only one datacenter
     * and one storage
     */


    protected static ComplexDatacenter createDatacenter(String name) {

        // Here are the steps needed to create a PowerDatacenter:
        // 1. We need to create a list to store one or more
        //    Machines
        List<Host> hostList = new ArrayList<>();

        // 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
        //    create a list to store these PEs before creating
        //    a Machine.
        for (int i = 1; i <= 20; i++) {
            List<Pe> peList1 = new ArrayList<>();
            int mips = 32000;
            // 3. Create PEs and add these into the list.
            //for a quad-core machine, a list of 4 PEs is required:
            for (int j=0;j<16;j++){
                peList1.add(new Pe(j, new PeProvisionerSimple(mips)));
            }

            int hostId = 0;
            int ram = 64000; //host memory (MB)
            long storage = 1000000; //host storage
            int bw = 10000;
            hostList.add(
                    new Host(
                            hostId,
                            new RamProvisionerSimple(ram),
                            new BwProvisionerSimple(bw),
                            storage,
                            peList1,
                            new VmSchedulerTimeShared(peList1))); // This is our first machine
            //hostId++;
        }

        // 4. Create a DatacenterCharacteristics object that stores the
        //    properties of a data center: architecture, OS, list of
        //    Machines, allocation policy: time- or space-shared, time zone
        //    and its price (G$/Pe time unit).
        String arch = "x86";      // system architecture
        String os = "Linux";          // operating system
        String vmm = "Xen";
        double time_zone = 10.0;         // time zone this resource located
        double cost = 3.0;              // the cost of using processing in this resource
        double costPerMem = 0.05;		// the cost of using memory in this resource
        double costPerStorage = 0.1;	// the cost of using storage in this resource
        double costPerBw = 0.1;			// the cost of using bw in this resource
        LinkedList<Storage> storageList = new LinkedList<>();	//we are not adding SAN devices by now
        ComplexDatacenter datacenter = null;

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);

        // 5. Finally, we need to create a storage object.
        /**
         * The bandwidth within a data center in MB/s.
         */
        int maxTransferRate = 15;// the number comes from the futuregrid site, you can specify your bw

        try {
            // Here we set the bandwidth to be 15MB/s
            HarddriveStorage s1 = new HarddriveStorage(name, 1e12);
            s1.setMaxTransferRate(maxTransferRate);
            storageList.add(s1);
            datacenter = new ComplexDatacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return datacenter;
    }

    protected  static void printResult(double[] result){
        Statistics statisticResult=new Statistics(result);
        Log.printLine("Mean :" + statisticResult.getMean());
        Log.printLine("Median : " + statisticResult.median());
        Log.printLine("Variance : " + statisticResult.getVariance());
        Log.printLine("Std : " + statisticResult.getStdDev());
    }
    /**
     * Prints the job objects
     *
     * @param list list of jobs
     */
    protected static void printJobList(List<Job> list) {
        String indent = "    ";
        Log.printLine();
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Job ID" + indent + "Task ID" + indent + "STATUS" + indent
                + "Data center ID" + indent + "VM ID" + indent + indent
                + "Time" + indent + "Start Time" + indent + "Finish Time" + indent + "Depth");
        DecimalFormat dft = new DecimalFormat("###.##");
        for (Job job : list) {
            Log.print(indent + job.getCloudletId() + indent + indent);
            if (job.getClassType() == Parameters.ClassType.STAGE_IN.value) {
                Log.print("Stage-in");
            }
            for (Task task : job.getTaskList()) {
                Log.print(task.getCloudletId() + ",");
            }
            Log.print(indent);

            if (job.getCloudletStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            } else if (job.getCloudletStatus() == Cloudlet.FAILED) {
                Log.print("FAILED");
                Log.printLine(indent + indent + job.getResourceId() + indent + indent + indent + job.getVmId()
                        + indent + indent + indent + dft.format(job.getActualCPUTime())
                        + indent + indent + dft.format(job.getExecStartTime()) + indent + indent + indent
                        + dft.format(job.getFinishTime()) + indent + indent + indent + job.getDepth());
            }
        }
    }
}
