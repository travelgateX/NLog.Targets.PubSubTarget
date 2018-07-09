using System;
using System.Collections.Generic;
using NLog.Common;
using Google.Apis.Pubsub.v1.Data;
using Nlog.Targets.PubSub;
using System.Threading.Tasks;
using System.Text;

namespace NLog.Targets.PubSubTarget
{
    [Target("PubSubTarget")]
    public class PubSubTarget : TargetWithLayout
    {

        public int MaxBytesPerRequest { get; set; } = 1048576;

        public string ServiceAccountEmail { get; set; }

        public string ApplicationName { get; set; }

        public string FileNameCertificateP12 { get; set; }

        public string DirectoryCertificateP12 { get; set; } 

        public string PasswordCertificateP12 { get; set; }

        public string Topic { get; set; }

        public bool? ConcatMessages { get; set; }

        public int? Retry { get; set; } = 1;

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

                List<PublishRequest> pubSubRequests;

                if (ConcatMessages == true )
                {
                    pubSubRequests = FormPayloadConcat(logEvents);
                }
                else
                {
                    pubSubRequests = FormPayload(logEvents);
                }

                int retryCount = 0;

                List<Task<PublishResponse>> tasks = null;

                while (pubSubRequests.Count != 0 && retryCount < Retry)
                {
                    tasks = new List<Task<PublishResponse>>();

                    foreach (var pubSubRequest in pubSubRequests)
                    {
                        var pubSubResponse = GoogleResources.Instance(ServiceAccountEmail, ApplicationName, FileNameCertificateP12, PasswordCertificateP12, DirectoryCertificateP12).pubsubService.Projects.Topics.Publish(pubSubRequest, this.Topic).ExecuteAsync();
                        tasks.Add(pubSubResponse);
                    }

                    await Task.WhenAll(tasks);

                    List<PublishRequest> pubSubRequestsRetry = new List<PublishRequest>();

                    int count = 0;
                    foreach (var task in tasks)
                    {
                        if (task.Exception != null)
                        {
                            pubSubRequestsRetry.Add(pubSubRequests[count]);
                        }
                        else if (task.Result.MessageIds.Count != pubSubRequests[count].Messages.Count)
                        {
                            InternalLogger.Trace($"Failed to send all messages to PubSub: total messages={pubSubRequests[count].Messages.Count}, messages received ={task.Result.MessageIds.Count}");
                        }
                        count += 1;
                    }
                    retryCount += 1;
                    pubSubRequests = pubSubRequestsRetry;
                }

                foreach (var task in tasks)
                {
                    if (task.Exception != null)
                    {
                        InternalLogger.Trace($"Failed to send message to PubSub: exception={task.Exception.ToString()}");
                    }
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

        private List<PublishRequest> FormPayload(ICollection<AsyncLogEventInfo> logEvents)
        {

            List<PublishRequest> pRequestList = new List<PublishRequest>();

            PublishRequest pRequest = new PublishRequest()
            {
                Messages = new List<PubsubMessage>()
            };

            int totalBytes = 0;

            foreach (var ev in logEvents)
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(Layout.Render(ev.LogEvent));

                if (bytes.Length + totalBytes > MaxBytesPerRequest)
                {
                    pRequestList.Add(pRequest);
                    pRequest = new PublishRequest()
                    {
                        Messages = new List<PubsubMessage>()
                    };

                    totalBytes = 0;
                }

                pRequest.Messages.Add(new PubsubMessage() {Attributes = _atributesD, Data = Convert.ToBase64String(bytes) });
                totalBytes += bytes.Length;

            }

            if (pRequest.Messages.Count > 0)
            {
                pRequestList.Add(pRequest);
            }
            return pRequestList;
        }

        private List<PublishRequest> FormPayloadConcat(ICollection<AsyncLogEventInfo> logEvents)
        {

            List<PublishRequest> pRequestList = new List<PublishRequest>();

            PublishRequest pRequest = new PublishRequest()
            {
                Messages = new List<PubsubMessage>()
            };

            int totalBytes = 0;

           
            StringBuilder sb = new StringBuilder();
            
            foreach (var ev in logEvents)
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(Layout.Render(ev.LogEvent));

                if (bytes.Length + totalBytes > MaxBytesPerRequest)
                {
                    pRequest.Messages.Add(new PubsubMessage() { Attributes = _atributesD, Data = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(sb.ToString()))});
                    pRequestList.Add(pRequest);
                    pRequest = new PublishRequest()
                    {
                        Messages = new List<PubsubMessage>()
                    };

                    sb = new StringBuilder();

                    totalBytes = 0;
                }

                sb.AppendLine(Layout.Render(ev.LogEvent));

                totalBytes += bytes.Length;

            }

            //Enviamos los últimos mensajes
            if (totalBytes > 0)
            {
                pRequest.Messages.Add(new PubsubMessage() { Attributes = _atributesD, Data = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(sb.ToString()))});
                pRequestList.Add(pRequest);
            }

            return pRequestList;
        }
    }
}
