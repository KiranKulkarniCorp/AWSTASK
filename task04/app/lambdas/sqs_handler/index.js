public class SqsHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {
 
	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
		System.out.println("Hello from lambda");
 
		try {
            List<Map<String, Object>> records = (List<Map<String, Object>>) request.get("Records");
 
			for (Map<String, Object> record : records) {
				String messageBody = (String) record.get("body");
				System.out.println("Received SQS message: " + messageBody);
			}
		} catch (Exception e) {
			System.out.println("Error processing SQS message: " + e.getMessage());
		}
 
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", 200);
		resultMap.put("body", "Hello from SQS Lambda");
		return resultMap;
	}
}