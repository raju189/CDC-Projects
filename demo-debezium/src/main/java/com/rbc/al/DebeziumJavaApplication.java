package com.rbc.al;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DebeziumJavaApplication {

	public static void main(String[] args) {
		SpringApplication.run(DebeziumJavaApplication.class, args);
	}

//	public static void main(String[] args) {
//		new SpringApplicationBuilder(DebeziumJavaApplication.class)
//				.web(WebApplicationType.NONE)
//				.run(args);
//	}

//	@Bean
//	public MessageChannel debeziumInputChannel() {
//		return new DirectChannel();
//	}
//
//	@Bean
//	public MessageProducer debeziumMessageProducer(
//			DebeziumEngine.Builder<ChangeEvent<byte[], byte[]>> debeziumEngineBuilder,
//			MessageChannel debeziumInputChannel) {
//
//		DebeziumMessageProducer debeziumMessageProducer =
//				new DebeziumMessageProducer(debeziumEngineBuilder);
//		debeziumMessageProducer.setOutputChannel(debeziumInputChannel);
//		return debeziumMessageProducer;
//	}
//
//	@ServiceActivator(inputChannel = "debeziumInputChannel")
//	public void handler(Message<?> message) {
//
//		Object destination = message.getHeaders().get(DebeziumHeaders.DESTINATION);
//
//		String key = new String((byte[]) message.getHeaders().get(DebeziumHeaders.KEY));
//
//		String payload = new String((byte[]) message.getPayload());
//
//		System.out.println("KEY: " + key + ", DESTINATION: " + destination + ", PAYLOAD: " + payload);
//	}

}
