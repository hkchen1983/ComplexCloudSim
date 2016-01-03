package complex.cloudsim;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowDatacenter;
import org.workflowsim.utils.Parameters;

import java.util.List;
import java.util.Random;

/**
 * Created by JIA on 02/01/2016.
 */
public class ComplexDatacenter extends WorkflowDatacenter {
    private double previuosTick;
    public ComplexDatacenter(String name, DatacenterCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy, List<Storage> storageList, double schedulingInterval) throws Exception {
        super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
        setPreviuosTick(0);

    }

    public void setPreviuosTick(double previuosTick){
        this.previuosTick=previuosTick;
    }

    public double getPreviuosTick() {return this.previuosTick;}

    protected void updateCloudletProcessing(double currentTick) {
        Log.printLine("Tick Time:" + currentTick);
        List<? extends Host> list = getVmAllocationPolicy().getHostList();
        double smallerTime = Double.MAX_VALUE;
        Random damageGenerator = new Random();
        for (int i = 0; i < list.size(); i++) {
            Host host = list.get(i);
            List<? extends Vm> vmList = host.getVmList();
            for (int j=0;j<vmList.size();j++) {
                ComplexVM vm = (ComplexVM) vmList.get(j);
                vm.setRuntimeMipsPerPE(0,((double) damageGenerator.nextInt((int) (vm.getDamageRatio()*100))/100));
                //Log.printLine("Tick Time:" + currentTick + ",Utilization "+ vm.getTotalUtilizationOfCpu(currentTick)+ ", Runtime Mips per  PE:" + vm.getRuntimeMipsPerPE() + ", Actual Mips per PE:"+vm.getMips()/vm.getNumberOfPes());

            }
            double time = host.updateVmsProcessing(currentTick);
            if (time < smallerTime) {
                smallerTime = time;
            }
            if (smallerTime < currentTick + 0.11) {
                smallerTime = currentTick + 0.11;
            }
            if (smallerTime != Double.MAX_VALUE) {
                schedule(getId(), (smallerTime - currentTick), CloudSimTags.VM_DATACENTER_EVENT);
            }
            setLastProcessTime(currentTick);
        }
    }

    @Override
    protected void processCloudletSubmit(SimEvent ev, boolean ack) {
        if(CloudSim.clock()>=(getPreviuosTick()+1)){
            for(int i=((int)getPreviuosTick()+1);i<CloudSim.clock();i++){
                setPreviuosTick(i);
                updateCloudletProcessing(i);
            }
        }
        updateCloudletProcessing();

        try {
            /**
             * cl is actually a job but it is not necessary to cast it to a job
             */
            Job job = (Job) ev.getData();

            if (job.isFinished()) {
                String name = CloudSim.getEntityName(job.getUserId());
                Log.printLine(getName() + ": Warning - Cloudlet #" + job.getCloudletId() + " owned by " + name
                        + " is already completed/finished.");
                Log.printLine("Therefore, it is not being executed again");
                Log.printLine();

                // NOTE: If a Cloudlet has finished, then it won't be processed.
                // So, if ack is required, this method sends back a result.
                // If ack is not required, this method don't send back a result.
                // Hence, this might cause CloudSim to be hanged since waiting
                // for this Cloudlet back.
                if (ack) {
                    int[] data = new int[3];
                    data[0] = getId();
                    data[1] = job.getCloudletId();
                    data[2] = CloudSimTags.FALSE;

                    // unique tag = operation tag
                    int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                    sendNow(job.getUserId(), tag, data);
                }

                sendNow(job.getUserId(), CloudSimTags.CLOUDLET_RETURN, job);

                return;
            }

            int userId = job.getUserId();
            int vmId = job.getVmId();
            Host host = getVmAllocationPolicy().getHost(vmId, userId);
            ComplexVM vm = (ComplexVM) host.getVm(vmId, userId);

            switch (Parameters.getCostModel()) {
                case DATACENTER:
                    // process this Cloudlet to this CloudResource
                    job.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(),
                            getCharacteristics().getCostPerBw());
                    break;
                case VM:
                    job.setResourceParameter(getId(), vm.getCost(), vm.getCostPerBW());
                    break;
                default:
                    break;
            }

            /**
             * Stage-in file && Shared based on the file.system
             */
            if (job.getClassType() == Parameters.ClassType.STAGE_IN.value) {
                stageInFile2FileSystem(job);
            }

            /**
             * Add data transfer time (communication cost
             */
            double fileTransferTime = 0.0;
            if (job.getClassType() == Parameters.ClassType.COMPUTE.value) {
                fileTransferTime = processDataStageInForComputeJob(job.getFileList(), job);
            }

            CloudletScheduler scheduler = vm.getCloudletScheduler();
            double estimatedFinishTime = scheduler.cloudletSubmit(job, fileTransferTime);
            updateTaskExecTime(job, vm);

            // if this cloudlet is in the exec queue
            if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
                send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
            } else {
                Log.printLine("Warning: You schedule cloudlet to a busy VM");
            }

            if (ack) {
                int[] data = new int[3];
                data[0] = getId();
                data[1] = job.getCloudletId();
                data[2] = CloudSimTags.TRUE;

                int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
                sendNow(job.getUserId(), tag, data);
            }
        } catch (ClassCastException c) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
        } catch (Exception e) {
            Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
            e.printStackTrace();
        }
        checkCloudletCompletion();
    }

}
