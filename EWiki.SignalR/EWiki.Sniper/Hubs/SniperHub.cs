using System;
using System.Threading;
using System.Threading.Tasks;
using EWiki.SignalR.Hubs.Models;
using Microsoft.AspNet.SignalR;
using Newtonsoft.Json;
using PokemonGo.RocketAPI;
using PokemonGo.RocketAPI.Enums;
using PokemonGo.RocketAPI.Exceptions;
using PokemonGo.RocketAPI.Logic;
using EWiki.Sniper.PokeFeeder;
using System.Collections.Generic;

namespace EWiki.SignalR.Hubs
{
    public class SniperHub : Hub
    {
        public void SendMessage(string connectionId, SniperMessage message)
        {
            Clients.Client(connectionId).GetMessage(JsonConvert.SerializeObject(message));
        }

        public async Task SniperMessage(string message)
        {
            var connectionId = Context.ConnectionId;

            // Disable snipping feature
            Clients.Client(connectionId).GetSniperStatus(JsonConvert.SerializeObject(new SniperStatus
            {
                IsAvailable = false
            }));
            return;

            try
            {
                var snipeRq = JsonConvert.DeserializeObject<SnipeRq>(message);
                await Snipe(snipeRq);
            }
            catch (Exception e)
            {
                Logger.Error($"Error: {e.Message}");
            }
        }

        public void FetchSniperInfos()
        {
            try
            {
                var sniperInfos = PokeFeeder.FetchPokemon();
                SendSniperInfos(sniperInfos);
            }
            catch(Exception e)
            {
                Logger.Error($"Error: {e.Message}");
            }
        }

        private void SendSniperInfos(List<SniperInfo> sniperInfos)
        {
            var connectionId = Context.ConnectionId;
            Clients.Client(connectionId).GetSniperInfos(JsonConvert.SerializeObject(sniperInfos));
        }

        public override Task OnConnected()
        {
            var connectionId = Context.ConnectionId;
            SendMessage(connectionId, new SniperMessage()
            {
                Color = "Magenta",
                Content = "Connected",
                CreatedDate = DateTime.Now
            });

            return base.OnConnected();
        }

        private async Task Snipe(SnipeRq snipeRq)
        {
            var connectionId = Context.ConnectionId;
            Logic logic = null;
            await Task.Run(async () =>
            {
                if (string.IsNullOrWhiteSpace(snipeRq.UserName) || string.IsNullOrWhiteSpace(snipeRq.Password))
                {
                    Logger.Error("Please input UserName and Password.");
                    return;
                }

                var settings = new Settings();

                if (snipeRq.UserName.Contains("@gmail.com"))
                {
                    settings.AuthType = AuthType.Google;
                    settings.GoogleUsername = snipeRq.UserName;
                    settings.GooglePassword = snipeRq.Password;
                }
                else
                {
                    settings.AuthType = AuthType.Ptc;
                    settings.PtcUsername = snipeRq.UserName;
                    settings.PtcPassword = snipeRq.Password;
                }

                try
                {
                    logic = new Logic(settings, new LogicInfoObservable());
                }
                catch (PtcOfflineException)
                {
                    Logger.Error("PTC Servers are probably down OR you credentials are wrong.", connectionId, this);
                    Logger.Error("Trying again in 20 seconds...", connectionId, this);
                    Thread.Sleep(20000);
                    logic = new Logic(settings, new LogicInfoObservable());
                }
                catch (AccountNotVerifiedException)
                {
                    Logger.Error("Your PTC Account is not activated. Exiting in 10 Seconds.", connectionId, this);
                    Thread.Sleep(10000);
                    Environment.Exit(0);
                }
                catch (Exception ex)
                {
                    Logger.Error($"Unhandled exception: {ex}", connectionId, this);
                    Logger.Error("Restarting in 20 Seconds.", connectionId, this);
                    Thread.Sleep(20000);
                    logic = new Logic(settings, new LogicInfoObservable());
                }
                
                await logic.SnipePokemon(snipeRq.PokemonId, snipeRq.Latitude, snipeRq.Longitude, connectionId, this, snipeRq.Language);
            });
        }
    }
}
