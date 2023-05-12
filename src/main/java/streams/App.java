package streams;

import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.LocalApplicationRunner;

import org.apache.samza.util.CommandLine;

public class App {

    public static void main(String[] args) {
        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);

        SamzaPipeline app = new SamzaPipeline();
        LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
        runner.run();
        runner.waitForFinish();
    }
}
