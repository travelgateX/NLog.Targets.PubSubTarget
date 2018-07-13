using System;
using System.Collections.Generic;
using NLog.Common;
//using Google.Apis.Pubsub.v1.Data;
using Google.Cloud.PubSub.V1;
using Nlog.Targets.PubSub;
using System.Threading.Tasks;
using System.Text;

namespace NLog.Targets.PubSubTarget
{
    [Target("PubSubTarget")]
    public class PubSubTarget : TargetWithLayout
    {

        public int MaxBytesPerRequest { get; set; } = 1048576;

        public int MaxMessagesPerRequest { get; set; } = 1000;

        public string FileName { get; set; }

        public string DirectoryPathJsonFile { get; set; } 

        public string Topic { get; set; }

        public string Project { get; set; }

        public bool? ConcatMessages { get; set; }

        public string Atributes { get; set; }

        private Dictionary<string, string> _atributesD = null;
        public PubSubTarget()
        {
            Name = "PubSubTarget";

            if (!string.IsNullOrEmpty(Atributes))
            {
                _atributesD = new Dictionary<string, string>();

                var ats = Atributes.Split(';');
                foreach(var at in ats)
                {
                    var kv = at.Split(':');
                    _atributesD.Add(kv[0], kv[1]);
                }
            }
        }

        protected override void InitializeTarget()
        {
            base.InitializeTarget();
        }

        protected override void Write(AsyncLogEventInfo logEvent)
        {
            SendBatch(new[] { logEvent });
        }

        protected override void Write(IList<AsyncLogEventInfo> logEvents)
        {
            SendBatch(logEvents);
        }

        private async void SendBatch(ICollection<AsyncLogEventInfo> logEvents)
        {
            try
            {

                if (!string.IsNullOrEmpty(Atributes) && _atributesD == null)
                {
                    _atributesD = new Dictionary<string, string>();

                    var ats = Atributes.Split(';');
                    foreach (var at in ats)
                    {
                        var kv = at.Split(':');
                        _atributesD.Add(kv[0], kv[1]);
                    }
                }

                List<List<PubsubMessage>> pubSubRequests = null;

                if (ConcatMessages == true )
                {
                    pubSubRequests = FormPayloadConcat(logEvents);
                }
                else
                {
                    pubSubRequests = FormPayload(logEvents);
                }

                List<Task<PublishResponse>> tasks = new List<Task<PublishResponse>>();

                GoogleResources gr = GoogleResources.Instance(FileName, DirectoryPathJsonFile, Project, Topic);

                foreach (var pubSubRequest in pubSubRequests)
                {
                    Task<PublishResponse> pubSubResponse = gr.publisherServiceApiClient.PublishAsync(gr.topic, pubSubRequest);
                    tasks.Add(pubSubResponse);
                }

                await Task.WhenAll(tasks);

                int count = 0;
                foreach (var task in tasks)
                {
                    if (task.Exception != null)
                    {
                        InternalLogger.Trace($"Failed to send message to PubSub: exception={task.Exception.ToString()}");
                    }
                    else if (task.Result.MessageIds.Count != pubSubRequests[count].Count)
                    {
                        InternalLogger.Trace($"Failed to send all messages to PubSub: total messages={pubSubRequests[count].Count}, messages received ={task.Result.MessageIds.Count}");
                    }
                    count += 1;
                }


                foreach (var ev in logEvents)
                {
                    ev.Continuation(null);
                }
            }
            catch (Exception ex)
            {
                InternalLogger.Error($"Error while sending log messages to PubSub: message=\"{ex.ToString()}\"");

                foreach (var ev in logEvents)
                {
                    ev.Continuation(ex);
                }
            }
        }

        private List<List<PubsubMessage>> FormPayload(ICollection<AsyncLogEventInfo> logEvents)
        {

            List<List<PubsubMessage>> pRequestList = new List<List<PubsubMessage>>();

            List<PubsubMessage> pRequest = new List<PubsubMessage>();

            int totalBytes = 0;

            foreach (var ev in logEvents)
            {
                var bytes = Google.Protobuf.ByteString.CopyFromUtf8(Layout.Render(ev.LogEvent));

                if (bytes.Length < MaxBytesPerRequest)
                {
                    if (bytes.Length + totalBytes > MaxBytesPerRequest)
                    {
                        pRequestList.Add(pRequest);

                        totalBytes = 0;
                    }

                    var message = new PubsubMessage() { Data = bytes };

                    foreach (var atr in _atributesD)
                    {
                        message.Attributes.Add(atr.Key, atr.Value);
                    }

                    pRequest.Add(message);

                    totalBytes += bytes.Length;
                }
                else
                {
                    InternalLogger.Trace($"Failed to send message to PubSub, message exceed limit bytesMessageSize:{bytes.Length}, bytesMaxSize:{MaxBytesPerRequest}");
                }

            }

            if (pRequest.Count > 0)
            {
                pRequestList.Add(pRequest);
            }
            return pRequestList;
        }

        private List<List<PubsubMessage>> FormPayloadConcat(ICollection<AsyncLogEventInfo> logEvents)
        {

            List<List<PubsubMessage>> pRequestList = new List<List<PubsubMessage>>();

            List<PubsubMessage> pRequest = new List<PubsubMessage>();

            int count = 0;

            StringBuilder sb = new StringBuilder();

            foreach (var ev in logEvents)
            {

                if (count > MaxMessagesPerRequest)
                {
                    var message = new PubsubMessage() { Data = Google.Protobuf.ByteString.CopyFromUtf8(sb.ToString()) };

                    foreach (var atr in _atributesD)
                    {
                        message.Attributes.Add(atr.Key, atr.Value);
                    }

                    pRequest.Add(message);
                    pRequestList.Add(pRequest);

                    pRequest = new List<PubsubMessage>();

                    sb = new StringBuilder();

                    count = 0;
                }

                sb.AppendLine(Layout.Render(ev.LogEvent));
                count += 1;
            }

            //Enviamos los últimos mensajes
            if (count > 0)
            {
                var message = new PubsubMessage() { Data = Google.Protobuf.ByteString.CopyFromUtf8(sb.ToString()) };

                foreach (var atr in _atributesD)
                {
                    message.Attributes.Add(atr.Key, atr.Value);
                }

                pRequest.Add(message);
                pRequestList.Add(pRequest);
            }

            return pRequestList;

        }
    }
}
