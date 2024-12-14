package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.patterns.Reaper;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Base64;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Worker extends AbstractBehavior<Worker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DataMessageWithLargeMessageProxy implements LargeMessageProxy.LargeMessage, Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> masterLargeMessageProxy;
		byte[] data;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DataMessageDirect implements Message {
		private static final long serialVersionUID = 3206934676797075802L;
		ActorRef<Master.Message> master;
		byte[] data;
	}


	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "worker";

	public static Behavior<Message> create() {
		return Behaviors.setup(Worker::new);
	}

	private Worker(ActorContext<Message> context) {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());

		final ActorRef<Receptionist.Listing> listingResponseAdapter =
				context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(Master.masterService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final Random random = new Random(4711);

	private final int messageSizeInMB = SystemConfigurationSingleton.get().getPerformanceTestMessageSizeInMB();

	private int numberOfMessagesSent = 0;

	private long performanceTestStartTime;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(DataMessageWithLargeMessageProxy.class, this::handle)
				.onMessage(DataMessageDirect.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		message.getListing().getServiceInstances(Master.masterService)
				.forEach(master -> master.tell(new Master.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy)));
		return this;
	}

	private Behavior<Message> handle(DataMessageWithLargeMessageProxy message) {
		this.handleDataMessage(message.getMasterLargeMessageProxy());
		return this;
	}

	private Behavior<Message> handle(DataMessageDirect message) {
		this.handleDataMessage(message.getMaster());
		return this;
	}

	private void handleDataMessage(ActorRef<?> sender) {
		if (this.numberOfMessagesSent == 0) {
			log.info("Starting performance analysis by sending messages to the master!");
			this.performanceTestStartTime = System.nanoTime();
		}

		if (SystemConfigurationSingleton.get().isPerformanceTestUseLargeMessageProxy()) {
			sendMessageToMasterUsingLargeMessageProxy((ActorRef<LargeMessageProxy.Message>) sender);
		} else {
			sendMessageToMasterDirectly((ActorRef<Master.Message>) sender);
		}
		this.numberOfMessagesSent++;

		if (this.numberOfMessagesSent >= SystemConfigurationSingleton.get().getPerformanceTestNumberOfMessagesFromWorker()) {
			long performanceTestEndTime = System.nanoTime();
			long elapsedTimeInNanoSeconds = performanceTestEndTime - this.performanceTestStartTime;
			log.info("Performance analysis finished! Sent {} messages (à {} MB) in {} ms.", this.numberOfMessagesSent,
					this.messageSizeInMB, TimeUnit.MILLISECONDS.convert(elapsedTimeInNanoSeconds, TimeUnit.NANOSECONDS));
			this.getContext().getSystem().unsafeUpcast().tell(new Guardian.ShutdownMessage());
		}
	}

	private void sendMessageToMasterUsingLargeMessageProxy(ActorRef<LargeMessageProxy.Message> workerMessageProxy) {
		this.getContext().getLog().info("Already sent {} messages. Sending another message to the master using the large message proxy!",
				this.numberOfMessagesSent);
		byte[] data = generateDataMessage();

		writePerformanceTestLogMessagesIfApplicable();
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(new Master.DataMessageWithLargeMessageProxy(this.largeMessageProxy, data), workerMessageProxy));
	}

	private void sendMessageToMasterDirectly(ActorRef<Master.Message> master) {
		this.getContext().getLog().info("Already sent {} messages. Sending another message to the master directly!", this.numberOfMessagesSent);
		byte[] data = generateDataMessage();
		writePerformanceTestLogMessagesIfApplicable();
		master.tell(new Master.DataMessageDirect(this.getContext().getSelf(), data));
	}

	private void writePerformanceTestLogMessagesIfApplicable() {
		if (SystemConfigurationSingleton.get().getPerformanceTestLogMessageSizeInBytes() > 0) {
			this.getContext().getLog().info("Random test log message {}",
					Base64.getEncoder().encodeToString(generateRandomByteArray(SystemConfigurationSingleton.get().getPerformanceTestLogMessageSizeInBytes())));
		}
	}

	private byte[] generateDataMessage() {
		return generateRandomByteArray(SystemConfigurationSingleton.get().getPerformanceTestMessageSizeInMB() * 1024 * 1024);
	}

	private byte[] generateRandomByteArray(final int size) {
		byte[] data = new byte[size];
		this.random.nextBytes(data);
		return data;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		// If we expect the system to still be active when the a ShutdownMessage is issued,
		// we should propagate this ShutdownMessage to all active child actors so that they
		// can end their protocols in a clean way. Simply stopping this actor also stops all
		// child actors, but in a hard way!
		return Behaviors.stopped();
	}
}
