using Google.Api.Gax.Grpc;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.PubSub.V1;
using Grpc.Auth;
using Grpc.Core;
using NLog.Common;
using System;
using System.Collections.Generic;
using System.IO;

namespace Nlog.Targets.PubSub
{
    public class GoogleResources
    {
        private static readonly object SyncLock_LOCK = new object();
        private static Dictionary<string, GoogleResources> _mInstance;

        public PublisherServiceApiClient publisherServiceApiClient { get; private set; }

        public TopicName topic { get; private set; }

        public static GoogleResources Instance(string FileNameCertificateP12, string Directory, string project, string topic, int timeout)
        {
            if (_mInstance?.ContainsKey(topic) != true)
            {
                lock (SyncLock_LOCK)
                {
                    if (_mInstance?.ContainsKey(topic) != true)
                    {
                        if (_mInstance == null)
                        {
                            _mInstance = new Dictionary<string, GoogleResources>();
                        }

                        _mInstance.Add(topic, LoadResources(FileNameCertificateP12, Directory, project, topic, timeout));
                    }
                }
            }

            return _mInstance[topic];
        }

        private static GoogleResources LoadResources(string FileNameCertificateP12, string Directory, string project, string topic, int timeout)
        {
            GoogleResources bqResources = new GoogleResources();
            try
            {
                string dir = string.Empty;
                InternalLogger.Warn($"DirectoryPathJsonFile is={Directory}");
                if (string.IsNullOrEmpty(Directory))
                {
                    dir = Path.Combine(Environment.CurrentDirectory, FileNameCertificateP12);
                }
                else
                {
                    dir = Path.Combine(Directory, FileNameCertificateP12);
                }

                InternalLogger.Warn($"JsonFile is={dir}");
                GoogleCredential cred = GoogleCredential.FromFile(dir);
                Channel channel = new Channel(
                    PublisherServiceApiClient.DefaultEndpoint.Host,
                    PublisherServiceApiClient.DefaultEndpoint.Port,
                    cred.ToChannelCredentials());

                CallTiming ct = CallTiming.FromTimeout(TimeSpan.FromSeconds(timeout));
                PublisherServiceApiSettings pas = PublisherServiceApiSettings.GetDefault();
                pas.PublishSettings = CallSettings.FromCallTiming(ct);
                PublisherServiceApiClient client = PublisherServiceApiClient.Create(channel, pas);
                bqResources.topic = new TopicName(project, topic);
                bqResources.publisherServiceApiClient = client;
            }
            catch (Exception ex)
            {
                InternalLogger.Error($"Failed to initialize GoogleResources to PubSub ex={ex}");
            }

            return bqResources;
        }
    }
}