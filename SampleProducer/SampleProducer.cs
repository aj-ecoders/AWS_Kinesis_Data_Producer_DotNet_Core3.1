using Amazon.Kinesis.Model;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;

/// <summary>
/// <para> Before running the code:
/// Fill in your AWS access credentials in the provided credentials file template,
/// and be sure to move the file to the default location under your home folder --
/// C:\users\username\.aws\credentials on Windows -- where the sample code will
/// load the credentials from.
/// https://console.aws.amazon.com/iam/home?#security_credential
/// </para>
/// <para>
/// WARNING:
/// To avoid accidental leakage of your credentials, DO NOT keep the credentials
/// file in your source directory.
/// </para>
/// </summary>

namespace Amazon.Kinesis.ClientLibrary.SampleProducer
{
    /// <summary>
    /// A sample producer of Kinesis records.
    /// </summary>
    class SampleRecordProducer
    {
        /// <summary>
        /// The AmazonKinesisClient instance used to establish a connection with AWS Kinesis,
        /// create a Kinesis stream, populate it with records, and (optionally) delete the stream.
        /// The SDK attempts to fetch credentials in the order described in:
        /// http://docs.aws.amazon.com/sdkfornet/latest/apidocs/items/MKinesis_KinesisClientctorNET4_5.html.
        /// You may also wish to change the RegionEndpoint.
        /// </summary>
        private static readonly AmazonKinesisClient kinesisClient = new AmazonKinesisClient(RegionEndpoint.USEast1);

        /// <summary>
        /// This method verifies your credentials, creates a Kinesis stream, waits for the stream
        /// to become active, then puts 10 records in it, and (optionally) deletes the stream.
        /// </summary>
        public static void Main(string[] args)
        {
            const string myStreamName = "practice-datastream";
            const int myStreamSize = 1;
            PutRecords(myStreamSize, myStreamName);

        }

        /// <summary>
        /// Use this method to write the data to data stream.
        /// <param name="myStreamName">Name of the stream from where we want write the data</param>
        /// <param name="myStreamSize">Name of the shardId from where we want write the data</param>
        /// </summary
        private static void PutRecords(int myStreamSize,string myStreamName)
        {
            try
            {
                ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
                //listStreamsRequest.Limit(20);
                listStreamsRequest.Limit = 20;
                bool isStreamexist = false;
                var listStreamsResult = kinesisClient.ListStreamsAsync(listStreamsRequest);
                var streamNames = listStreamsResult.Result.StreamNames;
                if (streamNames.Count > 0)
                {
                    foreach (var streamName in streamNames)
                    {
                        if (streamName == myStreamName) { isStreamexist = true; }
                    }
                }
                if (!isStreamexist)
                {
                    var createStreamRequest = new CreateStreamRequest();
                    createStreamRequest.StreamName = myStreamName;
                    createStreamRequest.ShardCount = myStreamSize;
                    var createStreamReq = createStreamRequest;
                    var CreateStreamResponse = kinesisClient.CreateStreamAsync(createStreamReq).Result;
                    Console.Error.WriteLine("Created Stream : " + myStreamName);
                }
            }
            catch (ResourceInUseException)
            {
                Console.Error.WriteLine("Producer is quitting without creating stream " + myStreamName +
                    " to put records into as a stream of the same name already exists.");
                Environment.Exit(1);
            }
            WaitForStreamToBecomeAvailable(myStreamName);
            Console.Error.WriteLine("Putting records in stream : " + myStreamName);
            for (int j = 0; j < 10; ++j)
            {
                TimeSpan t = DateTime.UtcNow - new DateTime(1970, 1, 1);
                int secondsSinceEpoch = (int)t.TotalSeconds;
                string property_value = Convert.ToString(secondsSinceEpoch);
                var json = new
                {
                    BatchActivityID = property_value,
                    MSMInstanceI = property_value,
                    SchSourceID = property_value,
                    VerSourceID = property_value,
                    ImportFileFilter = property_value,
                    ImportStarted = property_value,
                    ImportDurationSeconds = property_value
                };
                StringBuilder sb = new StringBuilder();
                using (StringWriter sw = new StringWriter(sb))
                using (JsonTextWriter writer = new JsonTextWriter(sw))
                {
                    writer.QuoteChar = '\'';

                    JsonSerializer ser = new JsonSerializer();
                    ser.Serialize(writer, json);
                }

                //convert to byte array in prep for adding to stream
                byte[] oByte = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(sb.ToString()));
                using (MemoryStream ms = new MemoryStream(oByte))
                {
                    PutRecordRequest requestRecord = new PutRecordRequest();
                    requestRecord.StreamName = myStreamName;
                    requestRecord.Data = ms;
                    requestRecord.PartitionKey = "partitionKey-" + j;

                    var putResultResponse = kinesisClient.PutRecordAsync(requestRecord).Result;
                    Console.Error.WriteLine(
                        String.Format("Successfully putrecord {0}:\n\t partition key = {1,15}, shard ID = {2}",
                            j, requestRecord.PartitionKey, putResultResponse.ShardId));
                    System.Threading.Thread.Sleep(5000);

                    //Call below methond if you want read/get the data from kinesis stream.
                    //GetRecords(putResultResponse.ShardId, myStreamName);
                }
            }
            PutRecords(myStreamSize, myStreamName);
        }

            /// <summary>
            /// Use this method to read the data from data stream.
            /// <param name="StreamName">Name of the stream from where we want read the data</param>
            /// <param name="shardId">Name of the shardId from where we want read the data</param>
            /// </summary
            private static void GetRecords(string shardId, string StreamName)
            {
                var siRequest = new GetShardIteratorRequest();
                siRequest.ShardId = shardId;
                siRequest.StreamName = StreamName;
                siRequest.ShardIteratorType = "TRIM_HORIZON";

                var siResponse = kinesisClient.GetShardIteratorAsync(siRequest);
                var request = new GetRecordsRequest();
                request.ShardIterator = siResponse.Result.ShardIterator;
                var getRecordsRequest = new GetRecordsRequest
                {
                    Limit = 10,
                    ShardIterator = request.ShardIterator,
                };

                var getRecordsResponse = kinesisClient.GetRecordsAsync(getRecordsRequest);
                var records = getRecordsResponse.Result;
                if (records.Records.Count > 0)
                {
                    Console.WriteLine($"Received {records.Records.Count} records.");
                    foreach (var record in records.Records)
                    {
                        var json = Encoding.UTF8.GetString(record.Data.ToArray());
                        Console.WriteLine("Json string: " + json);
                    }
                }
            }

        /// <summary>
        /// This method waits a maximum of 30 seconds for the specified stream to become active.
        /// <param name="myStreamName">Name of the stream whose active status is waited upon.</param>
        /// </summary>
        private static void WaitForStreamToBecomeAvailable(string myStreamName)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                DescribeStreamRequest describeStreamReq = new DescribeStreamRequest();
                describeStreamReq.StreamName = myStreamName;
                var describeResult = kinesisClient.DescribeStreamAsync(describeStreamReq).Result;
                string streamStatus = describeResult.StreamDescription.StreamStatus;
                Console.Error.WriteLine("  - current state: " + streamStatus);
                if (streamStatus == StreamStatus.ACTIVE)
                {
                    return;
                }
                System.Threading.Thread.Sleep(TimeSpan.FromSeconds(10));
            }

            throw new Exception("Stream " + myStreamName + " never went active.");
        }

    }
}