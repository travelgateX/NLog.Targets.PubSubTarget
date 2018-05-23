using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Pubsub.v1;
using Google.Apis.Services;
using NLog.Common;

namespace Nlog.Targets.PubSub
{
    public class GoogleResources
    {
        private static readonly object SyncLock_LOCK = new object();
        private static GoogleResources _mInstance;


        public static string ServiceAccountEmail;
        public static string ApplicationName;
        public static string FileNameCertificateP12;
        public static string PasswordCertificateP12;

        public static GoogleResources Instance
        {
            get
            {
                if ((_mInstance == null))
                    lock (SyncLock_LOCK)
                        if ((_mInstance == null))
                            _mInstance = loadResources();
                return _mInstance;
            }
        }

        private static GoogleResources loadResources()
        {
            GoogleResources bqResources = new GoogleResources();
            try
            {
                X509Certificate2 certificate = new X509Certificate2(Path.Combine(Environment.CurrentDirectory, FileNameCertificateP12), PasswordCertificateP12, X509KeyStorageFlags.Exportable);
                var inicializer = new ServiceAccountCredential.Initializer(ServiceAccountEmail).FromCertificate(certificate);
                inicializer.Scopes = new List<string>(new string[] { Google.Apis.Pubsub.v1.PubsubService.Scope.Pubsub });
                ServiceAccountCredential credential = new ServiceAccountCredential(inicializer);
                var baseInicializer = new BaseClientService.Initializer();
                baseInicializer.HttpClientInitializer = credential;
                baseInicializer.ApplicationName = ApplicationName;
                PubsubService psService = new PubsubService(baseInicializer);
                bqResources.pubsubService = psService;
            }
            catch (Exception ex)
            {
                InternalLogger.Error($"Failed to send message to PubSub ex={ex.Message}");
            }

            return bqResources;
        }

        public PubsubService pubsubService { get; private set; }
    }
}