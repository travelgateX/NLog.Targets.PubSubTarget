using System;
using System.Collections.Generic;
using NLog.Common;
using Google.Apis.Pubsub.v1.Data;
using Nlog.Targets.PubSub;
using System.Threading.Tasks;

namespace NLog.Targets.PubSubTarget
{
    [Target("PubSubTarget")]
    public class PubSubTarget : TargetWithLayout
    {

        public int MaxBytesPerRequest { get; set; } = 1048576;

        public string ServiceAccountEmail { get; set; }

        public string ApplicationName { get; set; }

        public string FileNameCertificateP12 { get; set; }

        public string PasswordCertificateP12 { get; set; }

        public string Topic { get; set; }

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
            GoogleResources.ServiceAccountEmail = ServiceAccountEmail;
            GoogleResources.ApplicationName = ApplicationName;
            GoogleResources.FileNameCertificateP12 = FileNameCertificateP12;
            GoogleResources.PasswordCertificateP12 = PasswordCertificateP12;

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

                var pubSubRequests = FormPayload(logEvents);

                List<Task<PublishResponse>> tasks = new List<Task<PublishResponse>>();

                foreach(var pubSubRequest in pubSubRequests)
                {
                    var pubSubResponse = GoogleResources.Instance.pubsubService.Projects.Topics.Publish(pubSubRequest,this.Topic).ExecuteAsync();
                    tasks.Add(pubSubResponse);
                }

                await Task.WhenAll(tasks);

                int count = 0;
                foreach(var task in tasks)
                {
                    if (task.Exception != null)
                    {
                        InternalLogger.Trace($"Failed to send message to PubSub: exception={task.Exception.ToString()}");
                    }
                    else if (task.Result.MessageIds.Count != pubSubRequests[count].Messages.Count)
                    {
                        InternalLogger.Trace($"Failed to send all messages to PubSub: total messages={pubSubRequests[count].Messages.Count}, messages received ={task.Result.MessageIds.Count}");
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

                pRequest.Messages.Add(new PubsubMessage() { Attributes = _atributesD, Data = Convert.ToBase64String(bytes) });
                totalBytes += bytes.Length;

            }

            if (pRequest.Messages.Count > 0)
            {
                pRequestList.Add(pRequest);
            }
            return pRequestList;
        }
    }
}
