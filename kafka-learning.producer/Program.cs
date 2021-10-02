using Confluent.Kafka;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_learning.producer
{
    class Program
    {
        async static Task Main(string[] args)
        {
            // topic
            var topic = args[0];

            Console.WriteLine(topic);
            // config
            string configPath = @"c:\temp\.confluent\csharp.config";

            // certificado
            string certPath = @"c:\temp\cacert.pem";

            var config = await LoadConfig(configPath, certPath);

            Produce(topic, config);

            Console.WriteLine("Hello World!");
        }

        static async Task<ClientConfig> LoadConfig(string configPath, string certDir)
        {
            try
            {
                var cloudConfig = (await File.ReadAllLinesAsync(configPath))
                    .Where(line => !line.StartsWith("#"))
                    .ToDictionary(
                        line => line.Substring(0, line.IndexOf('=')),
                        line => line.Substring(line.IndexOf('=') + 1));

                var clientConfig = new ClientConfig(cloudConfig);

                if (certDir != null)
                {
                    clientConfig.SslCaLocation = certDir;
                }

                return clientConfig;
            }
            catch (Exception e)
            {
                Console.WriteLine($"An error occured reading the config file from '{configPath}': {e.Message}");
                System.Environment.Exit(1);
                return null; // avoid not-all-paths-return-value compiler error.
            }
        }

        static void Produce(string topic, ClientConfig config)
        {
            while (true)
            {
                Console.Write("Write Key and Value:");
                string writtenKeyValue = Console.ReadLine();

                if (string.IsNullOrEmpty(writtenKeyValue)) { break; }
                string[] keyvalue = writtenKeyValue.Split(" ");

                using (var producer = new ProducerBuilder<string, string>(config).Build())
                {

                    var key = keyvalue[0];
                    var val = JObject.FromObject(new { valor = keyvalue[1] }).ToString(Formatting.None);

                    Console.WriteLine($"Producing record: {key} {val}");

                    producer.Produce(topic, new Message<string, string> { Key = key, Value = val },
                        (deliveryReport) =>
                        {
                            if (deliveryReport.Error.Code != ErrorCode.NoError)
                            {
                                Console.WriteLine($"Failed to deliver message: {deliveryReport.Error.Reason}");
                            }
                            else
                            {
                                Console.WriteLine($"Produced message to: {deliveryReport.TopicPartitionOffset}");
                            }
                        });

                    producer.Flush(TimeSpan.FromSeconds(10));

                    Console.WriteLine($"New message produced to topic {topic}");
                }
            }
        }
    }
}
