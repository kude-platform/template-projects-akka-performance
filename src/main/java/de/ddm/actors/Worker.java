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

import java.util.Random;

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
	public static class DataMessage implements LargeMessageProxy.LargeMessage, Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> senderLargeMessageProxy;
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

	private int numberOfMessagesSent = 0;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(DataMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		message.getListing().getServiceInstances(Master.masterService)
				.forEach(master -> master.tell(new Master.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy)));
		return this;
	}

	private Behavior<Message> handle(DataMessage message) {
		sendBigMessageToMaster(message.getSenderLargeMessageProxy());
		if (this.numberOfMessagesSent >= SystemConfigurationSingleton.get().getPerformanceTestNumberOfMessagesFromWorker())
			this.getContext().getSystem().unsafeUpcast().tell(new Guardian.ShutdownMessage());
		return this;
	}

	private void sendBigMessageToMaster(ActorRef<LargeMessageProxy.Message> workerMessageProxy) {
		this.getContext().getLog().info("Sending a big message to the master!");
		byte[] data = new byte[SystemConfigurationSingleton.get().getPerformanceTestMessageSizeInMB() * 1024 * 1024];
		this.random.nextBytes(data);
		this.largeMessageProxy.tell(
				new LargeMessageProxy.SendMessage(new Master.DataMessage(this.largeMessageProxy, data), workerMessageProxy));
		this.numberOfMessagesSent++;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		// If we expect the system to still be active when the a ShutdownMessage is issued,
		// we should propagate this ShutdownMessage to all active child actors so that they
		// can end their protocols in a clean way. Simply stopping this actor also stops all
		// child actors, but in a hard way!
		return Behaviors.stopped();
	}
}
