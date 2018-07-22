using IoTHubTrigger = Microsoft.Azure.WebJobs.ServiceBus.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventHubs;
using System.Text;
using System.Net.Http;
using System;
using Newtonsoft.Json;

namespace CrateDBPublisherFunc
{
    public static class Forwarder
    {
        private static HttpClient client = new HttpClient();

        [FunctionName("Forwarder")]
        public static async System.Threading.Tasks.Task RunAsync(
            [IoTHubTrigger("messages/events", Connection = "IoTHubConnectionString", 
            ConsumerGroup = "dbforwarder")]EventData message, TraceWriter log)
        {
            // Get the message payload and decode it
            string messagePayload = Encoding.UTF8.GetString(message.Body.Array);

            // Deserialize Json message payload
            var definition = new
            {
                temperature = 0.0,
                temperature_unit = "",
                humidity = 0.0,
                humidity_unit = "",
                pressure = 0.0,
                pressure_unit = ""
            };
            var chillerEvent = JsonConvert.DeserializeAnonymousType(messagePayload, definition);

            // Read Crate DB credentials from App settings and put it into the default headers for 
            // Basic authentication
            string crateDBCredentials = System.Environment.GetEnvironmentVariable("CrateDBCredentials");
            var byteArray = Encoding.ASCII.GetBytes(crateDBCredentials);
            client.DefaultRequestHeaders.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(byteArray)); ;

            // Use the enqueued timestamp from IoT Hub as event timestamp
            DateTime ts = message.SystemProperties.EnqueuedTimeUtc;

            // The device ID is taken from the message properties
            string deviceID = message.Properties["iothub-connection-device-id"].ToString();

            // Build the HTTP body message for a Crate DB insert
            FormattableString fmtString = $"{{ \"stmt\" : \"insert into chillertelemetrydata ( ts, deviceid, temperature, pressure, humidity ) values ( TO_TIMESTAMP('{ts:yyyy-MM-ddTHH:mm:ss.fff}'), '{deviceID}', {chillerEvent.temperature}, {chillerEvent.pressure}, {chillerEvent.humidity} )\"}}";
            string jsonInString = fmtString.ToString(System.Globalization.CultureInfo.GetCultureInfo("en-US"));

            // Call the REST interface of the Crate DB
            string crateDBURL = System.Environment.GetEnvironmentVariable("CrateDBURL");
            HttpResponseMessage response = await client.PostAsync(crateDBURL,
                new StringContent(jsonInString, Encoding.UTF8, "application/json"));

            if (!response.IsSuccessStatusCode)
            {
                log.Error($"Error {response.StatusCode} on inserting in Crate DB. Statement: {jsonInString}");
            } else
            {
                log.Info("Successfully written message to Crate DB");
            }
        }
    }
}