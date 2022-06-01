package edu.uci.ics.tippers.execution;

import edu.uci.ics.tippers.common.PolicyConstants;
import edu.uci.ics.tippers.common.PolicyEngineException;
import edu.uci.ics.tippers.execution.experiments.performance.PolicyScaler;
import edu.uci.ics.tippers.execution.experiments.performance.QueryPerformance;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
@SpringBootApplication
public class Sieve {

    // public static void main(String[] args) {
    //     PolicyConstants.initialize();
    //     System.out.println("Running Sieve on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
    //             + PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
    //     runSieve();
    // }

    public static void runSieve() {
        boolean QUERY_PERFORMANCE_EXP = false;
        boolean POLICY_SCALER_EXP = false;
        boolean MIDDLEWARE_MODE = false;
        Configurations configs = new Configurations();
        try {
            Configuration datasetConfig = configs.properties("config/general.properties");
            QUERY_PERFORMANCE_EXP = datasetConfig.getBoolean("query_performance");
            POLICY_SCALER_EXP = datasetConfig.getBoolean("policy_scaler");
            MIDDLEWARE_MODE = datasetConfig.getBoolean("middleware_mode");
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }

        if(QUERY_PERFORMANCE_EXP) {
            if(PolicyConstants.DBMS_CHOICE.equalsIgnoreCase(PolicyConstants.PGSQL_DBMS))
                throw new PolicyEngineException("Query Performance experiments only supported on MySQL");
            QueryPerformance queryPerformance = new QueryPerformance();
            queryPerformance.runExperiment();
        }
        if(POLICY_SCALER_EXP) {
            PolicyScaler policyScaler = new PolicyScaler();
            policyScaler.runExperiment();
        }
    }
    // public static void main(String[] args) {
    //     PolicyConstants.initialize();
    //     boolean MIDDLEWARE_MODE = false;
    //     Configurations configs = new Configurations();
    //      try {
    //         Configuration datasetConfig = configs.properties("config/general.properties");
    //         MIDDLEWARE_MODE = datasetConfig.getBoolean("middleware_mode");
    //     } catch (ConfigurationException e) {
    //         e.printStackTrace();
    //     }
    //     // ops operation = new ops();
    //     // operation.get();
    //     // operation.delete();
    //     if (!MIDDLEWARE_MODE) {
    //         System.out.println("Running Sieve on " + PolicyConstants.DBMS_CHOICE + " at " + PolicyConstants.DBMS_LOCATION + " with "
    //             + PolicyConstants.TABLE_NAME.toLowerCase() + " and " + PolicyConstants.getNumberOfTuples() + " tuples");
    //         runSieve();
    //     } else {
    //         SpringApplication.run(Sieve.class, args);
    //     }
    // }
}
