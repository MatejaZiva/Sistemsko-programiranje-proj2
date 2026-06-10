using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Diagnostics; //za stopwatch

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

        public WebServer(AppSettings settings, string address)
        {
            listener = new HttpListener();
            listener.Prefixes.Add($"http://localhost:{settings.port}/");
            api = new Europeana(address, settings);
            cache = new Cache(settings.maxCacheSize);
            cache.startCleanCache();
            queryE = new ConcurrentDictionary<string, SemaphoreSlim>();
            gracefulExitToken = cls.Token;
        }

        private void gracefulExit()
        {
            Logger.Log("The server is gracefully shutting down");
            cls.Cancel();
            listener.Stop();
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
                Console.WriteLine("The server couldn't start " + e.Message);
            }

            Task.Run(() =>
            {
                while (Console.ReadKey(true).Key != ConsoleKey.Z) { }
                gracefulExit();

            });

            while (true)
            {
                //ovde je main Thread i konstanto ceka za neku query i salje ga European-i
                try
                {
                    var context = await listener.GetContextAsync();
                    var _ = Task.Run(() => asyncHandleRequest(context));
                }
                catch (HttpListenerException e)
                {
                    Logger.Log(e.ToString());
                    break;
                }
                catch (ObjectDisposedException e)
                {
                    Logger.Log(e.ToString());
                    break;
                }
                catch (Exception e)
                {
                    Logger.Log(e.ToString());
                }
            }
        }


        public async Task asyncHandleRequest(Object? obj)
        {

            gracefulExitToken.ThrowIfCancellationRequested();
            if (obj is not HttpListenerContext context)
                return;

            HttpListenerRequest request = context.Request;

            //kada se napravi klijent sa browser-a request on zbog nekog razloga na pravi dva requesta
            //i taj drugi request je ovaj dole string pa da bez potrebe pravi greske samo ce ga hardkodujem
            if (context.Request.Url?.ToString() == "http://localhost:8080/favicon.ico")
                return;

            var keys = request.QueryString.AllKeys;
            if (keys.Length == 0 || string.IsNullOrWhiteSpace(request.QueryString["query"]))
            {
                string page = CreateHtmlResponse(
                    "Europeana Search",
                    "<p>Use the <strong>query</strong> parameter to search the Europeana API.</p>"
                    + "<p>Example: <a href=\"?query=Lisa\">?query=Lisa</a></p>"
                    + "<form method=\"get\">"
                    + "<label>Search query: <input name=\"query\" value=\"\" /></label>"
                    + "<button type=\"submit\">Search</button>"
                    + "</form>"
                );
                await sendDataToClient(null, context, 200, page);
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

            String jsonDataAsString = String.Empty;
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
                        await sendDataToClient(null, context, codeSend, "<p>Failure while fetching from Europeana.</p>\"");
                        throw new Eexceptions("The GET method has failed", codeSend);
                        return;
                    }

                    jsonDataAsString = await response.Content.ReadAsStringAsync();
                    //Za searilizaciju ne bih rekao da treba Task posto kao sta ce ti MIHALJO
                    mapper = JsonSerializer.Deserialize<EuropeanaMapper>(jsonDataAsString);
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
                Logger.Log("AAA");
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

        private static string RenderJsonPage(string json)
        {
            string escaped = WebUtility.HtmlEncode(json);
            return CreateHtmlResponse("Europeana result", $"<pre>{escaped}</pre>");
        }

        private static string CreateHtmlResponse(string title, string bodyHtml)
        {
            return "<!DOCTYPE html>\n"
                + "<html lang=\"en\">\n"
                + "<head>\n"
                + "    <meta charset=\"utf-8\">\n"
                + "    <title>" + WebUtility.HtmlEncode(title) + "</title>\n"
                + "    <style>\n"
                + "        body { font-family: Segoe UI, Arial, sans-serif; margin: 1rem; background: #f6f8fb; color: #1a1a1a; }\n"
                + "        pre { background: #ffffff; border: 1px solid #d1d5db; padding: 1rem; overflow: auto; white-space: pre-wrap; word-wrap: break-word; }\n"
                + "        a { color: #0366d6; text-decoration: none; }\n"
                + "        a:hover { text-decoration: underline; }\n"
                + "        button { margin-top: 0.5rem; padding: 0.5rem 1rem; }\n"
                + "    </style>\n"
                + "</head>\n"
                + "<body>\n"
                + "    <h1>" + WebUtility.HtmlEncode(title) + "</h1>\n"
                + bodyHtml + "\n"
                + "</body>\n"
                + "</html>";
        }
    }
}
