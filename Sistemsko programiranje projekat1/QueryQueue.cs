using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Sistemsko_programiranje_projekat1
{
    internal class QueryQueue
    {

        public Queue<HttpListenerContext> queue;
        public readonly object _lock = new object();
        public CancellationToken gracefulExitToken;
        private SemaphoreSlim allowToEnqueue;


        public QueryQueue(int count, CancellationToken token) 
        {
            allowToEnqueue = new SemaphoreSlim(0);
            queue = new Queue<HttpListenerContext>();
            gracefulExitToken = token;
        }

        
        public void Add(HttpListenerContext context)
        {
            lock(_lock)
            {
                queue.Enqueue(context);
            }
            allowToEnqueue.Release();
        }   


        public async Task<HttpListenerContext> Remove()
        {
            await allowToEnqueue.WaitAsync(gracefulExitToken);
            lock (_lock)
            {
               return queue.Dequeue();
            }
        }

    }
}
