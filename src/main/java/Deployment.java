import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.ec2.model.InstanceType;

public class Deployment {

//Out of the box skeleton code need to be modified  to our needs.
    public static void main(String[] args) {
        //splitCorpus()
        //run map reduce on c1
        //run map reduce on c2


        AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient();
        HadoopJarStepConfig hadoopJarStep = new HadoopJarStepConfig()
                .withJar("s3n://yourbucket/yourfile.jar")
                .withMainClass("some.pack.MainClass")
                .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");
        StepConfig stepConfig = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStep)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.6.0").withEc2KeyName("yourkey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("jobname")
                .withInstances(instances)
                .withSteps(stepConfig)
                .withLogUri("s3n://yourbucket/logs/");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

        //get results an calculate N N1 N2 T1 T2
        //output result for each trigram
    }




}
