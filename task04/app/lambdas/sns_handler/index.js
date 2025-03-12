public class SnsHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {
 
	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
		System.out.println("Hello from lambda");
 
		try {
			List<Map<String, Object>> records = (List<Map<String, Object>>) request.get("Records");
 
			if (records != null) {
				for (Map<String, Object> record : records) {
					Map<String, Object> sns = (Map<String, Object>) record.get("Sns");
					if (sns != null) {
						String message = (String) sns.get("Message");
						System.out.println("Received SNS message: " + message);
					}
				}
			}
		} catch (Exception e) {
			System.out.println("Error processing SNS message: " + e.getMessage());
		}
 
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", 200);
		resultMap.put("body", "Hello from Lambda");
		return resultMap;
	}
}
 