using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Android.App;
using Android.Content;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using Xamarin.Forms;
using EWiki.XF.Droid.LocationFeeder.Config;
using EWiki.XF.Droid.LocationFeeder.Repository;
using Android.Util;
using EWiki.XF.Droid.LocationFeeder.Helper;

namespace EWiki.XF.Droid.BackgroundServices
{
    [Service]
    public class LocationFeederBackgroundService : Service
    {
        List<CancellationTokenSource> _ctsList = new List<CancellationTokenSource>();
        private readonly SniperInfoRepository _serverRepository;
        private readonly SniperInfoRepositoryManager _sniperInfoRepositoryManager;

        public LocationFeederBackgroundService()
        {
            _serverRepository = new SniperInfoRepository();
            _sniperInfoRepositoryManager = new SniperInfoRepositoryManager(_serverRepository);
        }

        public override IBinder OnBind(Intent intent)
        {
            return null;
        }

        public override StartCommandResult OnStartCommand(Intent intent, StartCommandFlags flags, int startId)
        {
            var settings = GlobalSettings.Load();
            var rarePokemonRepositories = RarePokemonRepositoryFactory.CreateRepositories(settings);

            List<Task> repoTasks = new List<Task>();
            repoTasks =
                    rarePokemonRepositories.Select(
                        rarePokemonRepository => StartPollRarePokemonRepository(settings, rarePokemonRepository))
                        .Cast<Task>()
                        .ToList();

            try
            {
                // Manage repo tasks
                for (var i = 0; i < repoTasks.Count; ++i)
                {
                    var t = repoTasks[i];
                    if (t.Status != TaskStatus.Running && t.Status != TaskStatus.WaitingToRun &&
                        t.Status != TaskStatus.WaitingForActivation)
                    {
                        // Replace broken tasks with a new one
                        repoTasks[i].Dispose();
                        repoTasks[i] = StartPollRarePokemonRepository(settings, rarePokemonRepositories[i]);
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error($"Exception in thread manager: ", e.Message);
                throw;
            }

            return StartCommandResult.Sticky;
        }


        private async Task RareRepoThread(IRarePokemonRepository rarePokemonRepository)
        {
            var cts = new CancellationTokenSource();
            _ctsList.Add(cts);
            const int delay = 5 * 1000;
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    Thread.Sleep(delay);
                    for (var retrys = 0; retrys <= 1; retrys++)
                    {
                        var pokeSniperList = rarePokemonRepository.FindAll();
                        if (pokeSniperList != null)
                        {
                            if (pokeSniperList.Any())
                            {
                                WriteOutListeners(pokeSniperList);
                            }
                            break;
                        }
                        Thread.Sleep(1000);
                    }
                }
            }
            catch (Android.OS.OperationCanceledException)
            {
            }
            finally
            {
                if (cts.IsCancellationRequested)
                {
                    var message = new CancelledMessage();
                    Device.BeginInvokeOnMainThread(
                        () => MessagingCenter.Send(message, "CancelledMessage")
                    );
                }
            }
        }

        private void WriteOutListeners(List<SniperInfo> sniperInfos)
        {
            List<SniperInfo> sniperInfosToSend = sniperInfos;
            sniperInfosToSend = sniperInfosToSend.OrderBy(m => m.ExpirationTimestamp).ToList();
            foreach (SniperInfo sniperInfo in sniperInfosToSend)
            {
                _sniperInfoRepositoryManager.AddToRepository(sniperInfo);
            }
        }

        private async Task<Task> StartPollRarePokemonRepository(GlobalSettings globalSettings, IRarePokemonRepository rarePokemonRepository)
        {
            return await Task.Factory.StartNew(async () => await RareRepoThread(rarePokemonRepository), TaskCreationOptions.LongRunning);
        }

        public override void OnDestroy()
        {
            foreach(var cts in _ctsList)
            {
                cts?.Cancel();
            }

            base.OnDestroy();
        }
    }
}