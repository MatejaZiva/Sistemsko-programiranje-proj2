using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics; //za stopwatch
using System.Diagnostics.Tracing;
using System.Linq;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Sistemsko_programiranje_projekat1
{
    internal class WebServer
    {
        public HttpListener listener;
        public Cache cache;
        public Europeana api;
        private readonly ConcurrentDictionary<string, Task<EuropeanaMapper?>> queryE = new();

        private CancellationTokenSource cls = new();
        private CancellationToken gracefulExitToken;
        private List<Task> pendingTasks = new();
        private QueryQueue requestQueue;
        private SemaphoreSlim threadLimit;

        public WebServer(AppSettings settings, string address)
        {
            listener = new HttpListener();
            listener.Prefixes.Add($"http://localhost:{settings.port}/");
            api = new Europeana(address, settings);
            cache = new Cache(settings.maxCacheSize);
            cache.startCleanCache();
            gracefulExitToken = cls.Token;
            requestQueue = new QueryQueue(settings.maxTasksAtOnce, gracefulExitToken);
            threadLimit = new SemaphoreSlim(settings.maxTasksAtOnce);
        }

        private void gracefulExit()
        {
            Logger.Log("The server is gracefully shutting down");
            cls.Cancel();
            listener.Close();
        }

        public async Task asyncStartTheServer()
        {
            try
            {
                listener.Start();
                Logger.Log("The server has started");
            }
            catch (Exception e)
            {
                Logger.Log("The server couldn't start " + e.Message);
            }

            _ = DispatcherLoopAsync();

            var keyboardThread = new Thread(() =>
            {
                // while (Console.ReadKey(true).Key != ConsoleKey.Z) { } //samo "z" za exit (ima problema zbog watch run)
                while (Console.ReadLine()?.Trim().ToLower() != "z") { } // z + enter (uvek radi)
                gracefulExit();
            })
            { IsBackground = true };
            keyboardThread.Start();

            while (true)
            {
                //ovde je main Thread i konstanto ceka za neku query i salje ga European-i
                try
                {
                    var context = await listener.GetContextAsync();
                    if (context.Request.Url?.ToString() == "http://localhost:8080/favicon.ico")
                        continue;
                    requestQueue.Add(context);
                }
                catch (HttpListenerException)
                {
                    Logger.Log("HttpListenerException: request za exit");
                    break;
                }
                catch (ObjectDisposedException e)
                {
                    Logger.Log("ObjectDisposedException" + e.ToString());
                    break;
                }
                catch (Exception e)
                {
                    Logger.Log(e.ToString());
                }
            }


            //ceka da se svaki rquest zavrsi ako shutdownujemo dok se nesto obradjuje
            try
            {
                await Task.WhenAll(pendingTasks);
            }
            catch (Exception e)
            {
                Logger.Log("exc" + e);
            }
            Logger.Log("Shutdown");
        }


        public async Task DispatcherLoopAsync()
        {
            while (!gracefulExitToken.IsCancellationRequested)
            {
                try
                {
                    Console.WriteLine("Dispatcher");

                    await threadLimit.WaitAsync(gracefulExitToken);

                    var context = await requestQueue.Remove();
                    if (context != null)
                    {
                        Task requestTask = asyncHandleRequest(context);
                        _ = requestTask.ContinueWith((t) =>
                        {
                            threadLimit.Release();
                        }, TaskContinuationOptions.ExecuteSynchronously);

                        lock(pendingTasks)
                        {
                            pendingTasks.Add(requestTask);
                        }
                    }
                    else
                    {
                        threadLimit.Release();
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        public async Task asyncHandleRequest(Object? obj)
        {

            gracefulExitToken.ThrowIfCancellationRequested();
            if (obj is not HttpListenerContext context)
                return;

            HttpListenerRequest request = context.Request;

            var keys = request.QueryString.AllKeys;
            if (keys.Length == 0 || string.IsNullOrWhiteSpace(request.QueryString["query"]))
            {
                await sendDataToClient(null, context, 200);
                return;
            }

            string query = "?";
            foreach (var key in keys)
            {
                query += key;
                query += "=";
                query += request.QueryString.Get(key);
                query += "&";
            }

            string withoutKeyQuery = query;
            query += $"wskey={api.apiKey}";

            EuropeanaMapper? mapper = null;

            //inicijalizuj stopwatch
            TimeSpan elapsedTime;
            long startTime = Stopwatch.GetTimestamp();

            //Odma proveri u kes da li postoji
            if (cache.checkForKey(query, out mapper))
            {
                elapsedTime = Stopwatch.GetElapsedTime(startTime);
                Logger.Log("The query was found in the cache");
                await sendDataToClient(mapper, context, 200);

                Logger.Log($"Vreme potrebno za cache hit: {elapsedTime.TotalMilliseconds} ms");
                return;
            }

            bool coalescedHit = true;
            int codeSend = 200;

            try
            {
                mapper = await queryE.GetOrAdd(query, async (key) =>
                {
                    if (cache.checkForKey(key, out var cachedMapper))
                    {
                        return cachedMapper;
                    }

                    coalescedHit = false;
                    Logger.Log("The query wasn't found in the cache");

                    startTime = Stopwatch.GetTimestamp();

                    var response = await api.client.GetAsync(key, gracefulExitToken);

                    elapsedTime = Stopwatch.GetElapsedTime(startTime);
                    Logger.Log($"Vreme potrebno za cache miss/api call: {elapsedTime.TotalMilliseconds} ms");

                    if (response.IsSuccessStatusCode == false)
                    {
                        codeSend = 500;
                        throw new Eexceptions("The GET method has failed", codeSend);
                    }

                    var stream = await response.Content.ReadAsStreamAsync();
                    var result = await JsonSerializer.DeserializeAsync<EuropeanaMapper>(stream, new JsonSerializerOptions { PropertyNameCaseInsensitive = true }, gracefulExitToken);

                    if (result == null)
                    {
                        throw new InvalidOperationException("Failed to deserialize Europeana response.");
                    }

                    if (result.itemsCount == 0)
                    {
                        Logger.Log($"The query: {key} is not valid");
                        codeSend = 404;
                    }
                    else
                    {
                        cache.addToCache(key, result);
                        codeSend = 200;
                    }

                    return result;
                });

                if (coalescedHit && codeSend != 500 && codeSend != 404)
                {
                    Logger.Log("The query was found in the cache");
                    codeSend = 200;
                }

                await sendDataToClient(mapper, context, codeSend);
            }
            catch (OperationCanceledException e)
            {
                Logger.Log(e.ToString());
            }
            catch (Eexceptions e)
            {
                Logger.Log($"An error has occured: {e.Message} {e.errorCode}");
                await sendDataToClient(null, context, e.errorCode);
            }
            catch (Exception e)
            {
                Logger.Log("Something went really wrong: " + e.Message);
            }
            finally
            {
                queryE.TryRemove(query, out _);
            }
        }
        public async Task sendDataToClient(EuropeanaMapper mapper, HttpListenerContext context, int statusCode, string response = " ")
        {
            context.Response.StatusCode = statusCode;
            if (statusCode == 200)
            {
                context.Response.ContentType = "application/json";
                await JsonSerializer.SerializeAsync(context.Response.OutputStream, mapper, new JsonSerializerOptions { WriteIndented = true }, gracefulExitToken);
            }
            else
            {
                context.Response.ContentType = "text/html; charset=utf-8";

                byte[] buffer = Encoding.UTF8.GetBytes(response);
                context.Response.ContentLength64 = buffer.Length;

                await context.Response.OutputStream.WriteAsync(buffer, 0, buffer.Length, gracefulExitToken);
            }
            context.Response.Close();

        }
    }
}
