using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sistemsko_programiranje_projekat1
{
    internal class Cache
    {
        public Queue<string> listOfKeys;
        public static Dictionary<string, EuropeanaMapper> cache;
        ReaderWriterLockSlim lockSlim;
        private readonly CancellationTokenSource cToken = new();
        private Task cleanUpTask;
        public int sizeOfCache { get; set; }


        public Cache(int sizeForCache)
        {
            sizeOfCache = sizeForCache;
            cache = new Dictionary<string, EuropeanaMapper>(sizeOfCache);
            listOfKeys = new Queue<string>();
            lockSlim = new ReaderWriterLockSlim();
        }

        public bool checkForKey(string key, out EuropeanaMapper value)
        {
            lockSlim.EnterReadLock();
            try
            {
                if (cache.TryGetValue(key, out value))
                {
                    return true;
                }

                value = null;
                return false;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                throw;
            }
            finally
            {
                lockSlim.ExitReadLock();
            }
        }

        public void addToCache(string key, EuropeanaMapper value)
        {
            //Ako je pun "kes" onda izbaci kljuc koji je najdavnije koriscen sto je na pocetak liste
            lockSlim.EnterWriteLock();
            try
            {
                if (sizeOfCache == listOfKeys.Count)
                {
                    Logger.Log("Cache is full, replacing the first added element");
                    cache.Remove(listOfKeys.Dequeue());
                }

                listOfKeys.Enqueue(key);
                cache.Add(key, value);
            }
            catch (Exception e)
            {
                Console.WriteLine("An error occured while adding and element to cache" + e.Message);
            }
            finally
            {
                lockSlim.ExitWriteLock();
            }
        }


        public void startCleanCache()
        {
            try
            {
                cleanUpTask = Task.Run(async () =>
                {

                    var timer = new PeriodicTimer(TimeSpan.FromMinutes(3));

                    while (await timer.WaitForNextTickAsync(cToken.Token))
                    {
                        cleanCache();
                    }
                });
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private void cleanCache()
        {
            lockSlim.EnterWriteLock();
            try
            {
                cache.Clear();
                listOfKeys.Clear();
                Logger.Log("The cahce was successfully cleaned");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                lockSlim.ExitWriteLock();
            }
        }

    }
}
