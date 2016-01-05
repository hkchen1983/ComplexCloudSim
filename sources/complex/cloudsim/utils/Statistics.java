package complex.cloudsim.utils;

import java.util.Arrays;

/**
 * Created by JIA on 03/01/2016.
 */
public class Statistics {
    double[] data;
    int size;
    public double minValue;
    public double maxValue;

    public Statistics(double[] data)
    {
        this.data = data;
        size = data.length;
    }

    public double getCOV(){
        return getStdDev()/getMean();
    }

    public double getMean()
    {
        double sum = 0.0;
        for(double a : data)
            sum += a;
        return sum/size;
    }

    public double getVariance()
    {
        double mean = getMean();
        double temp = 0;
        for(double a :data)
            temp += (mean-a)*(mean-a);
        return temp/size;
    }

    public double getStdDev()
    {
        return Math.sqrt(getVariance());
    }

    public double median()
    {
        Arrays.sort(data);
        this.minValue=data[0];
        this.maxValue=data[data.length-1];

        if (data.length % 2 == 0)
        {
            return (data[(data.length / 2) - 1] + data[data.length / 2]) / 2.0;
        }
        else
        {
            return data[data.length / 2];
        }
    }
}
