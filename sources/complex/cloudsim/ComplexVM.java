package complex.cloudsim;

import org.cloudbus.cloudsim.CloudletScheduler;
import org.cloudbus.cloudsim.Log;
import org.workflowsim.CondorVM;

import java.util.List;

/**
 * Created by JIA on 02/01/2016.
 */
public class ComplexVM extends CondorVM {

    //Change every second, affect by the CPU utilization of host (Not VM), damage apply to each PE
    //runtimeMipsPerPE = (total MIPS/num of PE)*(1-CPU Utilization)*(1 - Random Damage Ratio)
    private double runtimeMipsPerPE;
    private double damageRatio;

    public ComplexVM(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm, CloudletScheduler cloudletScheduler) {
        super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cloudletScheduler);
        this.runtimeMipsPerPE = mips/(double) numberOfPes;
    }

    public ComplexVM(int id, int userId, double mips, int numberOfPes, int ram, long bw, long size, String vmm, double cost, double costPerMem, double costPerStorage, double costPerBW, CloudletScheduler cloudletScheduler) {
        super(id, userId, mips, numberOfPes, ram, bw, size, vmm, cost, costPerMem, costPerStorage, costPerBW, cloudletScheduler);
        this.runtimeMipsPerPE = mips/(double) numberOfPes;
        this.damageRatio=0;
    }

    public void setRuntimeMipsPerPE(double hostCpuUtilization,double damageRatio){
        this.runtimeMipsPerPE=this.getMips()/(double) this.getNumberOfPes() * (1 - hostCpuUtilization) * (1-damageRatio);
    }

    public double getRuntimeMipsPerPE(){ return this.runtimeMipsPerPE;}

    public void setDamageRatio(double damageRatio){
        this.damageRatio=damageRatio;
    }

    public double getDamageRatio(){return this.damageRatio;}

    @Override
    public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
        if (mipsShare != null) {
            for (int i=0;i<mipsShare.size();i++) { // count the CPUs available to the VMM
                //Log.printLine(i + "current mips is "+ mipsShare.get(i));
                if (mipsShare.get(i) > 0) {
                    mipsShare.set(i,getRuntimeMipsPerPE());
                    //Log.printLine(i + "runtime mips is "+ mipsShare.get(i));
                }
            }
            return getCloudletScheduler().updateVmProcessing(currentTime, mipsShare);
        }
        return 0.0;
    }
}
