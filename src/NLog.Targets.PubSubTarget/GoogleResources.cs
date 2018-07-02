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

        public static GoogleResources Instance(string ServiceAccountEmail, string ApplicationName, string FileNameCertificateP12, string PasswordCertificateP12, string Directory)
        {
                if ((_mInstance == null))
                    lock (SyncLock_LOCK)
                        if ((_mInstance == null))
                            _mInstance = loadResources(ServiceAccountEmail, ApplicationName, FileNameCertificateP12, PasswordCertificateP12, Directory);
                return _mInstance;
        }

        private static GoogleResources loadResources(string ServiceAccountEmail, string ApplicationName, string FileNameCertificateP12, string PasswordCertificateP12, string Directory)
        {
            GoogleResources bqResources = new GoogleResources();
            try
            {
                X509Certificate2 certificate;

                string dir = string.Empty;

                if (string.IsNullOrEmpty(Directory))
                {
                    dir = Path.Combine(Environment.CurrentDirectory, FileNameCertificateP12);
                }
                else
                {
                    dir = Path.Combine(Directory, FileNameCertificateP12);
                }

                InternalLogger.Warn($"Get FileP12 from path={dir}");

                certificate = new X509Certificate2(dir, PasswordCertificateP12, X509KeyStorageFlags.Exportable);

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
                InternalLogger.Error($"Failed to initialize GoogleResources to PubSub ex={ex.ToString()}");
            }

            return bqResources;
        }

        public PubsubService pubsubService { get; private set; }
    }
}