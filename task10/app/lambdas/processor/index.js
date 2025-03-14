const AWSXRay = require("aws-xray-sdk");
const AWS = AWSXRay.captureAWS(require("aws-sdk"));
const axios = require("axios");
const { v4: uuidv4 } = require("uuid");

const dynamoDB = new AWS.DynamoDB.DocumentClient();
const TABLE_NAME = process.env.TABLE_NAME || "Weather";

exports.handler = async (event) => {
    console.log("Received event:", JSON.stringify(event, null, 2));
    console.log("Using DynamoDB Table:", TABLE_NAME);

    const path = event?.rawPath || "/";
    const method = event?.requestContext?.http?.method || "GET";

    console.log("Request Path:", path);
    console.log("HTTP Method:", method);

     if (method === "GET" && (path === "/" || path === "/weather")) {
        try {
            const url = "https://api.open-meteo.com/v1/forecast?latitude=50.4375&longitude=30.5&hourly=temperature_2m&timezone=auto";
            console.log("Fetching weather data from:", url);

            const response = await axios.get(url);
            console.log("Weather API Response:", JSON.stringify(response.data, null, 2));

            const forecastData = response.data;
            const weatherEntry = {
                id: uuidv4(),
                forecast: {
                    elevation: forecastData.elevation,
                    generationtime_ms: forecastData.generationtime_ms,
                    hourly: forecastData.hourly,
                    hourly_units: forecastData.hourly_units,
                    latitude: forecastData.latitude,
                    longitude: forecastData.longitude,
                    timezone: forecastData.timezone,
                    timezone_abbreviation: forecastData.timezone_abbreviation,
                    utc_offset_seconds: forecastData.utc_offset_seconds
                }
            };

            console.log("Prepared Weather Data:", JSON.stringify(weatherEntry, null, 2));

            console.log("Storing data in DynamoDB...");
            await dynamoDB.put({
                TableName: TABLE_NAME,
                Item: weatherEntry
            }).promise();
            console.log("Data successfully stored in DynamoDB.");

            return {
                statusCode: 200,
                body: JSON.stringify({ message: "Weather data stored successfully", weatherEntry }),
                headers: {
                    "content-type": "application/json"
                },
                isBase64Encoded: false
            };
        } catch (error) {
            console.error("Error fetching or storing weather data:", error);
            return {
                statusCode: 500,
                body: JSON.stringify({ message: "Internal Server Error", error: error.message }),
                headers: {
                    "content-type": "application/json"
                },
                isBase64Encoded: false
            };
        }
    } else {
        console.warn("Bad request - unsupported path or method:", { path, method });
        return {
            statusCode: 400,
            body: JSON.stringify({
                statusCode: 400,
                message: `Bad request syntax or unsupported method. Request path: ${path}. HTTP method: ${method}`
            }),
            headers: {
                "content-type": "application/json"
            },
            isBase64Encoded: false
        };
    }
};
