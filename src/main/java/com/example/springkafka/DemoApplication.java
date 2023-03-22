package com.example.springkafka;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.repository.CrudRepository;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

}

@RestController
@RequestMapping("/user")
@ResponseStatus(HttpStatus.OK)
class UserController {

	private final UserService userService;

	public UserController(UserService userService) {
		this.userService = userService;
	}

	@PostMapping
	public Long registerUser() {
		return userService.registerUser();
	}
}

class UserRegistrationRequest {
	private String email;

	public void setEmail(String email) {
		this.email = email;
	}

	public String getEmail() {
		return email;
	}
}

@Service
class UserService {

	private final UserRepository userRepository;
	private final KafkaTemplate<String, String> kafkaTemplate;

	public UserService(UserRepository userRepository, KafkaTemplate<String, String> kafkaTemplate) {
		this.userRepository = userRepository;
		this.kafkaTemplate = kafkaTemplate;
	}

	public Long registerUser() {
		/*
		 * 1. Save user to DB
		 * 2. Publish user registered notification
		 * 3. Respond OK
		 */

		var user = new UserRecord();
		user = userRepository.save(user);

		var id = user.getId();
		notify(id);

		return id;
	}

	private void notify(Long id) {
		kafkaTemplate.send(KafkaConfig.USER_REGISTERED_TOPIC, id.toString());
	}

	@KafkaListener(topics = KafkaConfig.USER_REGISTERED_TOPIC, groupId = KafkaConfig.GROUP_ID)
	public void listen(String msg) {
		System.out.println("Kafka Listener received msg: " + msg);
	}
}

interface UserRepository extends CrudRepository<UserRecord, Long> {
}

@Entity
class UserRecord {

	@Id
	@GeneratedValue
	private Long id;

	public Long getId() {
		return id;
	}
}

@EnableKafka
@Configuration
class KafkaConfig {

	public static final String USER_REGISTERED_TOPIC = "userRegisteredTopic";
	public static final String GROUP_ID = "group1";

	@Value(value = "${spring.kafka.bootstrap-servers}")
	private String bootstrapAddress;

	@Bean
	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
		return new KafkaAdmin(configs);
	}

	@Bean
	public NewTopic userRegisteredTopic() {
		return new NewTopic(USER_REGISTERED_TOPIC, 1, (short) 1);
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapAddress);
		configProps.put(
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		configProps.put(
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class);
		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapAddress);
		props.put(
				ConsumerConfig.GROUP_ID_CONFIG,
				GROUP_ID);
		props.put(
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		props.put(
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				StringDeserializer.class);
		return new DefaultKafkaConsumerFactory<>(props);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {

		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		return factory;
	}
}

