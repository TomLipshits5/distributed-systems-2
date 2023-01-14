import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class Deployment {

//Out of the box skeleton code need to be modified  to our needs.
    public static  void main(String[] args)  {
        String BucketName = "s3://ds-2-files/";
        //step1
        String input_1 = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data";
        String input_1_test = "s3://ds-2-files/input-file.txt";
        String output_1 = BucketName + "output_1/";
        String jar_1 = "step1.jar";
        //step2
        String input_2 = output_1 + "part-r-00000";
        String output_2 = BucketName + "output_2/";
        String jar_2 = "step2.jar";
        //step3
        String input_3_1 = input_2;
        String input_3_2 = output_2 + "part-r-00000";
        String output_3 = BucketName + "output_3/";;
        String jar_3 = "step3.jar";

        EmrClient emr = EmrClient.builder().build();
        HadoopJarStepConfig hadoopJarStepConfig_1 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_1)
                .args(input_1, output_1)
                .build();
        StepConfig step_1 = StepConfig.builder()
                .name("Step_1")
                .hadoopJarStep(hadoopJarStepConfig_1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        HadoopJarStepConfig hadoopJarStepConfig_2 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_2)
                .args(input_2, output_2)
                .build();
        StepConfig step_2 = StepConfig.builder()
                .name("Step_2")
                .hadoopJarStep(hadoopJarStepConfig_2)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        HadoopJarStepConfig hadoopJarStepConfig_3 = HadoopJarStepConfig.builder()
                .jar(BucketName + jar_3)
                .args(input_3_1, input_3_2, output_3)
                .build();
        StepConfig step_3 = StepConfig.builder()
                .name("Step_3")
                .hadoopJarStep(hadoopJarStepConfig_3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();
        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("AmitAndTomCluster")
                .instances(JobFlowInstancesConfig.builder()
                        .ec2KeyName("amit_tom")
                        .instanceCount(9)
                        .masterInstanceType("m4.xlarge")
                        .slaveInstanceType("m4.xlarge")
                        .keepJobFlowAliveWhenNoSteps(false)
                        .placement(PlacementType.builder().availabilityZone("us-east-1a").build())
                        .build())
                .logUri("s3://ds-2-files/logs/")
                .steps(step_1, step_2, step_3)
                .releaseLabel("emr-5.36.0")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole").build();
//                .applications(
//                        Application.builder()
//                                .name("Hadoop")
//                                .version("2.10.1")
//                                .build()
//                )

        RunJobFlowResponse response = emr.runJobFlow(request);
        String jobFlowId = response.jobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }




}
