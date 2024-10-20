package de.ddm.configuration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Data;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Data
public class SystemConfiguration {

	public static final String MASTER_ROLE = "master";
	public static final String WORKER_ROLE = "worker";

	public static final int DEFAULT_MASTER_PORT = 7877;
	public static final int DEFAULT_WORKER_PORT = 7879;

	private String role = MASTER_ROLE;                 // This machine's role in the cluster.

	private String host = getDefaultHost();            // This machine's host name or IP that we use to bind this application against
	private int port = DEFAULT_MASTER_PORT;            // This machines port that we use to bind this application against

	private String masterHost = getDefaultHost();      // The host name or IP of the master; if this is a master, masterHost = host
	private int masterPort = DEFAULT_MASTER_PORT;      // The port of the master; if this is a master, masterPort = port

	private String ipAddress = getDefaultHost();            // The IP address of this machine

	private String actorSystemName = "ddm";            // The name of this application

	private boolean startPaused = false;               // Wait for some console input to start; useful, if we want to wait manually until all ActorSystems in the cluster are started (e.g. to avoid work stealing effects in performance evaluations)

	private boolean hardMode = false;					// Solve the hard version of the task

	private boolean runningInKubernetes = false;        // The application is running in Kubernetes

	private int performanceTestNumberOfMessagesFromWorker = 100; // The number of messages to send in performance tests from a worker instance to the master

	private int performanceTestMessageSizeInMB = 10; // The size of each message in performance tests in MB

	private boolean performanceTestUseLargeMessageProxy = true; // Use the LargeMessageProxy pattern for performance tests

	private static String getDefaultHost() {
		try {
			return InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			return "localhost";
		}
	}

	public void update(CommandMaster commandMaster) {
		this.role = MASTER_ROLE;
		this.host = commandMaster.host;
		this.port = commandMaster.port;
		this.ipAddress = commandMaster.ipAddress;
		this.masterHost = commandMaster.host;
		this.masterPort = commandMaster.port;
		this.startPaused = commandMaster.startPaused;
		this.hardMode = commandMaster.hardMode;
		this.runningInKubernetes = commandMaster.runningInKubernetes;
		this.performanceTestMessageSizeInMB = commandMaster.performanceTestMessageSizeInMB;
		this.performanceTestUseLargeMessageProxy = commandMaster.performanceTestUseLargeMessageProxy;
	}

	public void update(CommandWorker commandWorker) {
		this.role = WORKER_ROLE;
		this.host = commandWorker.host;
		this.port = commandWorker.port;
		this.ipAddress = commandWorker.ipAddress;
		this.masterHost = commandWorker.masterhost;
		this.masterPort = commandWorker.masterport;
		this.runningInKubernetes = commandWorker.runningInKubernetes;
		this.performanceTestMessageSizeInMB = commandWorker.performanceTestMessageSizeInMB;
		this.performanceTestNumberOfMessagesFromWorker = commandWorker.performanceTestNumberOfMessagesFromWorker;
		this.performanceTestUseLargeMessageProxy = commandWorker.performanceTestUseLargeMessageProxy;
	}

	public Config toAkkaConfig() {
		return ConfigFactory.parseString("" +
				"akka.remote.artery.canonical.hostname = \"" + this.host + "\"\n" +
				"akka.remote.artery.canonical.port = " + this.port + "\n" +
						(this.runningInKubernetes ?
								"akka.remote.artery.bind.hostname = \"" + this.ipAddress + "\"\n" +
										"akka.remote.artery.bind.port = " + this.port + "\n" : "") +
				"akka.cluster.roles = [" + this.role + "]\n" +
				"akka.cluster.seed-nodes = [\"akka://" + this.actorSystemName + "@" + this.masterHost + ":" + this.masterPort + "\"]")
				.withFallback(ConfigFactory.load("application"));
	}

	public Config toAkkaTestConfig() {
		return ConfigFactory.parseString("" +
				"akka.remote.artery.canonical.hostname = \"" + this.host + "\"\n" +
				"akka.remote.artery.canonical.port = " + this.port + "\n" +
						"akka.coordinated-shutdown.exit-jvm = off\n" +
				"akka.cluster.roles = [" + this.role + "]")
				.withFallback(ConfigFactory.load("application"));
	}
}
