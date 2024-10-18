package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.patterns.Reaper;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Master extends AbstractBehavior<Master.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<Worker.Message> worker;
		ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DataMessageWithLargeMessageProxy implements LargeMessageProxy.LargeMessage, Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> workerLargeMessageProxy;
		byte[] data;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DataMessageDirect implements Message {
		private static final long serialVersionUID = 4133699272904372501L;
		ActorRef<Worker.Message> worker;
		byte[] data;
	}

	@NoArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = 7516129288777469221L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "master";

	public static Behavior<Message> create() {
		return Behaviors.setup(Master::new);
	}

	public static final ServiceKey<Master.Message> masterService = ServiceKey.create(Master.Message.class, DEFAULT_NAME + "Service");

	private Master(ActorContext<Message> context) {
		super(context);
		Reaper.watchWithDefaultReaper(this.getContext().getSelf());

		this.workers = new ArrayList<>();
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
		context.getSystem().receptionist().tell(Receptionist.register(masterService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<Worker.Message>> workers;

	private final Random random = new Random(4711);

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(DataMessageWithLargeMessageProxy.class, this::handle)
				.onMessage(DataMessageDirect.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		return this;
	}


	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<Worker.Message> worker = message.getWorker();
		if (!this.workers.contains(worker)) {
			this.workers.add(worker);
			this.getContext().watch(worker);
			if (SystemConfigurationSingleton.get().isPerformanceTestUseLargeMessageProxy()) {
				sendMessageToWorkerUsingLargeMessageProxy(message.getLargeMessageProxy());
			} else {
				sendMessageToWorkerDirectly(worker);
			}

		}
		return this;
	}

	private Behavior<Message> handle(DataMessageWithLargeMessageProxy message) {
		sendMessageToWorkerUsingLargeMessageProxy(message.getWorkerLargeMessageProxy());
		return this;
	}

	private Behavior<Message> handle(DataMessageDirect message) {
		sendMessageToWorkerDirectly(message.getWorker());
		return this;
	}

	private void sendMessageToWorkerUsingLargeMessageProxy(ActorRef<LargeMessageProxy.Message> workerMessageProxy) {
		this.getContext().getLog().info("Sending a message to a worker via the Large Message Proxy!");
		byte[] data = generateDataMessage();
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(new Worker.DataMessageWithLargeMessageProxy(this.largeMessageProxy, data), workerMessageProxy));
	}

	private void sendMessageToWorkerDirectly(ActorRef<Worker.Message> worker) {
		this.getContext().getLog().info("Sending a message to a worker directly!");
		byte[] data = generateDataMessage();
		worker.tell(new Worker.DataMessageDirect(this.getContext().getSelf(), data));
	}

	private byte[] generateDataMessage() {
		byte[] data = new byte[SystemConfigurationSingleton.get().getPerformanceTestMessageSizeInMB() * 1024 * 1024];
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