package de.ddm;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.ddm.actors.Worker;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.patterns.Reaper;
import de.ddm.singletons.SystemConfigurationSingleton;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class WorkerTest {

	@ClassRule
	public static final TestKitJunitResource testKit = new TestKitJunitResource(SystemConfigurationSingleton.get().toAkkaTestConfig());

	@Before
	public void setUp() {
		testKit.spawn(Reaper.create(), Reaper.DEFAULT_NAME);
	}

	@Test
	public void testHandleDataMessage() {
		//given
		final TestProbe<LargeMessageProxy.Message> probe = testKit.createTestProbe();
		final ActorRef<Worker.Message> worker = testKit.spawn(Worker.create(), Worker.DEFAULT_NAME);
		final Worker.DataMessage dataMessage = new Worker.DataMessage(probe.getRef(), new byte[24]);

		//when
		worker.tell(dataMessage);

		//then
		probe.expectMessageClass(LargeMessageProxy.ConnectMessage.class);
		probe.expectNoMessage();
	}

}
