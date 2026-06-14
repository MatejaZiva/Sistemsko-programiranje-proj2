using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sistemsko_programiranje_projekat1
{
    internal class QuerySemaphore
    {

        private readonly object _lock = new object();
        public SemaphoreSlim semaphore;
        public int workerCount {get; set;}

        public QuerySemaphore() 
        {
            semaphore = new SemaphoreSlim(1);
            workerCount = 0;
        }


        public void addWorker()
        {
            lock(_lock)
            {
                workerCount++;
            }
        }

        public int getWorkers()
        {
            lock (_lock)
            {
                return workerCount;
            }
        }




    }
}
