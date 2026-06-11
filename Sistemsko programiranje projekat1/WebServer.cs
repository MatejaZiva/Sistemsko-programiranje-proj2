using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics; //za stopwatch
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
        public ConcurrentDictionary<string, SemaphoreSlim> queryE;
        private CancellationTokenSource cls = new();
        private CancellationToken gracefulExitToken;
        private List<Task> pendingTasks = new();
        private QueryQueue requestQueue;
        public List<Task> workerTasks = new();
        public readonly int numberOfTasks;

        public WebServer(AppSettings settings, string address)
        {
            listener = new HttpListener();
            listener.Prefixes.Add($"http://localhost:{settings.port}/");
            api = new Europeana(address, settings);
            cache = new Cache(settings.maxCacheSize);
            cache.startCleanCache();
            queryE = new ConcurrentDictionary<string, SemaphoreSlim>();
            gracefulExitToken = cls.Token;
            requestQueue = new QueryQueue(settings.maxTasksAtOnce,gracefulExitToken);
            numberOfTasks = settings.maxTasksAtOnce;
        }

        private void gracefulExit()
        {
            Logger.Log("The server is gracefully shutting down");
            cls.Cancel();
            listener.Stop();
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
                Console.WriteLine("The server couldn't start " + e.Message);
            }

            //Console.WriteLine(numberOfTasks);
            for(int i=0; i<6;i++)
            {
                pendingTasks.Add(Task.Run(()=>WorkerLoopAsync()));
            }

            Console.WriteLine("Dodao sam tasks workes");
            var keyboardTask = Task.Run(() => //mozda ovde moze thread
            {
                while (Console.ReadKey(true).Key != ConsoleKey.Z) { }
                gracefulExit();

            });


            pendingTasks.Add(keyboardTask);

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
                catch (HttpListenerException e)
                {
                    break;
                }
                catch (ObjectDisposedException e)
                {
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
                await Task.WhenAll(pendingTasks); //WaitAll(blokirajuce)? grupise sve taskove i vraca jedan task koji se zavrsi samo kad se svi taskovi unutar njega zavrse
            }
            catch (Exception e)
            {
            
            }
            Logger.Log("Shutdown");
            listener.Close();
        }


        public async Task WorkerLoopAsync()
        {
            while (!gracefulExitToken.IsCancellationRequested)
            {
                try
                {
                    var context = await requestQueue.Remove();
                    if (context != null)
                    {
                        await asyncHandleRequest(context);
                    }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception e)
                {
                    Logger.Log("Greška u radniku: " + e.Message);
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

            //Posto nema u kes onda mora da vidi dal postoji semafor za njega
            SemaphoreSlim semForQuery = queryE.GetOrAdd(query, (query) => new SemaphoreSlim(1));
            int codeSend;
            await semForQuery.WaitAsync(gracefulExitToken);

            try
            {
                if (!cache.checkForKey(query, out mapper))
                {

                    Logger.Log("The query wasn't found in the cache");

                    startTime = Stopwatch.GetTimestamp();

                    var response = await api.client.GetAsync(query, gracefulExitToken);

                    elapsedTime = Stopwatch.GetElapsedTime(startTime);
                    Logger.Log($"Vreme potrebno za cache miss/api call: {elapsedTime.TotalMilliseconds} ms");

                    if (response.IsSuccessStatusCode == false)
                    {
                        codeSend = 500;
                        await sendDataToClient(null, context, codeSend);
                        throw new Eexceptions("The GET method has failed", codeSend);
                    }

                    var stream = await response.Content.ReadAsStreamAsync();
                    mapper = await  JsonSerializer.DeserializeAsync<EuropeanaMapper>(stream,new JsonSerializerOptions { PropertyNameCaseInsensitive = true},gracefulExitToken);

                    if (mapper == null)
                    {
                        throw new InvalidOperationException("Failed to deserialize Europeana response.");
                    }

                    if (mapper.itemsCount == 0)
                    {
                        Logger.Log($"The query: {query} is not valid");
                        codeSend = 404;
                    }
                    else
                    {
                        cache.addToCache(query, mapper);
                        codeSend = 200;
                    }

                }
                else
                {
                    Logger.Log("The query was found in the cache");
                    codeSend = 200;
                }

                string responseHtml;
                if (codeSend == 404)
                {
                    responseHtml = $"The query: {withoutKeyQuery.Remove(withoutKeyQuery.Length - 1)} is not valid";
                }
                else
                {
                    responseHtml = "<p>Failure while fetching from Europeana.</p>";
                }

                await sendDataToClient(mapper, context, codeSend, responseHtml);

            }
            catch (OperationCanceledException e)
            {
                //Logger.Log(e.ToString());
            }
            catch (Eexceptions e)
            {
                Logger.Log($"An error has occured: {e.Message} {e.errorCode}");
            }
            catch (Exception e)
            {
                Logger.Log("Something went really wrong: " + e.Message);
            }
            finally
            {
                semForQuery.Release();
                if (semForQuery.CurrentCount == 1)
                {
                    queryE.TryRemove(query, out _);
                }
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
