using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Google.Apis.Auth.OAuth2;
//using Google.Apis.Pubsub.v1;
using Google.Apis.Services;
using Google.Cloud.PubSub.V1;
using NLog.Common;
using Grpc.Auth;
using Grpc.Core;
using Google.Api.Gax.Grpc;
using Google.Api.Gax;

namespace Nlog.Targets.PubSub
{
    public class GoogleResources
    {
        private static readonly object SyncLock_LOCK = new object();
        private static Dictionary<string, GoogleResources> _mInstance = null;

        public PublisherServiceApiClient publisherServiceApiClient { get; private set; }

        public TopicName topic { get; private set; }


        public static GoogleResources Instance(string FileNameCertificateP12, string Directory, string project, string topic, int timeout)
        {
            if (_mInstance == null || !_mInstance.ContainsKey(topic))
            {
                lock (SyncLock_LOCK)
                {
                    if (_mInstance == null || !_mInstance.ContainsKey(topic))
                    {
                        if (_mInstance == null)
                        {
                            _mInstance = new Dictionary<string, GoogleResources>();
                        }
                        _mInstance.Add(topic, loadResources(FileNameCertificateP12, Directory, project, topic, timeout));
                    }
                }

            }

            return _mInstance[topic];
        }

        private static GoogleResources loadResources(string FileNameCertificateP12, string Directory, string project, string topic, int timeout)
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
                    PublisherServiceApiClient.DefaultEndpoint.Host, PublisherServiceApiClient.DefaultEndpoint.Port, cred.ToChannelCredentials());


                //BackoffSettings bofretries = new BackoffSettings(TimeSpan.FromMilliseconds(900), TimeSpan.FromMilliseconds(3000), 2);

                //BackoffSettings boftimeouts = new BackoffSettings(TimeSpan.FromMilliseconds(2000), TimeSpan.FromMilliseconds(2000), 1);

                //RetrySettings settings = new RetrySettings(bofretries, boftimeouts, Expiration.FromTimeout(TimeSpan.FromSeconds(3)));

                //CallTiming ct = CallTiming.FromRetry(settings);

                CallTiming ct = CallTiming.FromTimeout(TimeSpan.FromSeconds(timeout));

                PublisherServiceApiSettings pas = PublisherServiceApiSettings.GetDefault();
                pas.PublishSettings = CallSettings.FromCallTiming(ct);

                PublisherServiceApiClient client = PublisherServiceApiClient.Create(channel, pas);

                bqResources.topic = new TopicName(project, topic);

                bqResources.publisherServiceApiClient = client;

            }
            catch (Exception ex)
            {
                InternalLogger.Error($"Failed to initialize GoogleResources to PubSub ex={ex.ToString()}");
            }

            return bqResources;
        }


    }
}