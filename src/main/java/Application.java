import configurations.AppConstants;
import configurations.Config;
import controllers.application.Processor;
import utilities.JSONDesrializer;
import utilities.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Application {
    public static void main(String[] args) {

        Application application = new Application();

        String location = application.getConfigLocation(args);

        if (!Strings.isNullOrEmpty(location)) {
            Config config = application.getConfig(location);

            if (application.isValid(config)) {
                Processor processor = new Processor();
                processor.process(config);
            }
        }
    }

    private String getConfigLocation(String[] args) {
        String location = null;

        if (args.length == 2 &&
                args[0].equalsIgnoreCase("-config") &&
                !Strings.isNullOrEmpty(args[1])) {
            location = args[1];
        } else {
            System.out.println("Invalid Arguments");
        }

        return location;
    }

    private Config getConfig(String location) {
        Config config = null;

        try (BufferedReader reader = Files.newBufferedReader(Paths.get(location))){
            config = JSONDesrializer.fromJson(reader, Config.class);
        }
        catch (IOException ioException) {
            System.out.printf("Unable to open configuration file at location %s. %s. \n", location, ioException.getMessage());
        }

        return config;
    }

    private boolean isValid(Config config) {
        boolean flag = false;

        if (config == null) {
            System.out.println("No configuration found.");
        } else if (config.getLoadBalancer() == null || !config.getLoadBalancer().isValid()) {
            System.out.println("Need load balancer ip address and host.");
        } else if (config.isCreateTopic() && (config.getTopicsToCreate() == null ||
                                              config.getTopicsToCreate().size() == 0 ||
                                              config.isConsumer() ||
                                              config.isProducer())) {
            System.out.println("Invalid topic information found in the config");
        } else if (config.isProducer() && config.isConsumer()) {
            System.out.println("Node can't be both producer/consumer. Provide correct information.");
        } else if ((config.isProducer() || config.isConsumer()) && (config.getTopics() == null || config.getTopics().size() == 0)) {
            System.out.println("No producer/consumer details found");
        } else if (config.isConsumer() && !(config.getMethod() == AppConstants.METHOD.PULL.getValue() || config.getMethod() == AppConstants.METHOD.PUSH.getValue())) {
            System.out.println("Wrong method provided for framework.Consumer.(Accepted method: 0 for PULL & 1 for PUSH)");
        } else {
            flag = true;
        }

        return flag;
    }
}
