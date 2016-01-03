package complex.cloudsim;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.workflowsim.CondorVM;
import org.workflowsim.Job;
import org.workflowsim.WorkflowScheduler;
import org.workflowsim.WorkflowSimTags;
import org.workflowsim.failure.FailureGenerator;
import org.workflowsim.utils.Parameters;

/**
 * Created by JIA on 02/01/2016.
 */
public class ComplexDatacenterBroker extends WorkflowScheduler {
    /**
     * Created a new WorkflowScheduler object.
     *
     * @param name name to be associated with this entity (as required by
     *             Sim_entity class from simjava package)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public ComplexDatacenterBroker(String name) throws Exception {
        super(name);
    }

    @Override
    protected void processCloudletReturn(SimEvent ev) {
        Cloudlet cloudlet = (Cloudlet) ev.getData();
        Job job = (Job) cloudlet;

        /**
         * Generate a failure if failure rate is not zeros.
         */
        FailureGenerator.generate(job);

        getCloudletReceivedList().add(cloudlet);
        getCloudletSubmittedList().remove(cloudlet);

        ComplexVM vm = (ComplexVM) getVmsCreatedList().get(cloudlet.getVmId());
        //so that this resource is released
        vm.setState(WorkflowSimTags.VM_STATUS_IDLE);

        double delay = 0.0;
        if (Parameters.getOverheadParams().getPostDelay() != null) {
            delay = Parameters.getOverheadParams().getPostDelay(job);
        }
        schedule(super.getWorkflowEngineId(), delay, CloudSimTags.CLOUDLET_RETURN, cloudlet);

        cloudletsSubmitted--;
        //not really update right now, should wait 1 s until many jobs have returned
        schedule(this.getId(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE);

    }
}
