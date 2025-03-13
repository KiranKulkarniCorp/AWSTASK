package com.task10;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.AWSXRayRecorder;
import com.amazonaws.xray.handlers.TracingHandler;
import com.amazonaws.xray.interceptors.TracingInterceptor;
import com.amazonaws.xray.strategy.sampling.LocalizedSamplingStrategy;
import com.amazonaws.xray.strategy.sampling.SamplingStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;
import java.util.UUID;

@LambdaHandler(
		lambdaName = "processor",
		roleName = "processor-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED,
		tracingMode = TracingMode.Active // Enabling AWS X-Ray tracing
)
public class Processor implements RequestHandler<Object, String> {

	private static final String WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast?latitude=37.7749&longitude=-122.4194&hourly=temperature_2m";
	private static final String TABLE_NAME = System.getenv("target_table"); // Fetch from environment variable

	private final AmazonDynamoDB client;
	private final DynamoDB dynamoDB;

	public Processor() {
		AWSXRayRecorder recorder = AWSXRay.getGlobalRecorder();
		SamplingStrategy samplingStrategy = new LocalizedSamplingStrategy(this.getClass().getResource("/sampling-rules.json"));
		recorder.setSamplingStrategy(samplingStrategy);

		this.client = AmazonDynamoDBClientBuilder.standard()
				.withRequestHandlers(new TracingHandler(AWSXRay.getGlobalRecorder()))
				.build();
		this.dynamoDB = new DynamoDB(client);
	}

	@Override
	public String handleRequest(Object request, Context context) {
		AWSXRay.beginSegment("LambdaFunctionProcessor"); // Start tracing segment

		try {
			String jsonResponse = fetchWeatherData();
			JsonNode weatherData = parseJson(jsonResponse);
			storeWeatherData(weatherData);

			AWSXRay.endSegment(); // End tracing segment
			return "Weather data successfully stored in DynamoDB!";
		} catch (Exception e) {
			AWSXRay.endSegment();
			return "Error fetching/storing weather data: " + e.getMessage();
		}
	}

	private String fetchWeatherData() throws Exception {
		AWSXRay.beginSubsegment("FetchWeatherData");

		URL url = new URL(WEATHER_API_URL);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");

		Scanner scanner = new Scanner(url.openStream());
		StringBuilder result = new StringBuilder();
		while (scanner.hasNext()) {
			result.append(scanner.nextLine());
		}
		scanner.close();

		AWSXRay.endSubsegment();
		return result.toString();
	}

	private JsonNode parseJson(String jsonResponse) throws Exception {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readTree(jsonResponse);
	}

	private void storeWeatherData(JsonNode weatherData) {
		AWSXRay.beginSubsegment("StoreWeatherData");

		Table table = dynamoDB.getTable(TABLE_NAME);
		String uuid = UUID.randomUUID().toString();

		Item item = new Item()
				.withPrimaryKey("id", uuid)
				.withJSON("forecast", weatherData.toString());

		table.putItem(item);

		AWSXRay.endSubsegment();
	}
}
