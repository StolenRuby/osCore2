/* 31 August 2018 ( Nani added this :D )
 * 
 * Copyright (c) Contributors, http://opensimulator.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the OpenSimulator Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using log4net;
using Nini.Config;
using Mono.Addins;
using OpenMetaverse;
using OpenSim.Data.MySQL;
using OpenSim.Framework;
using OpenSim.Framework.Console;
using OpenSim.Framework.Monitoring;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using OpenSim.Services.Interfaces;


//[assembly: Addin("FlotsamAssetCache", "1.1")]
//[assembly: AddinDependency("OpenSim", "0.8.1")]

namespace OpenSim.Region.CoreModules.Asset
{
    [Extension(Path = "/OpenSim/RegionModules", NodeName = "RegionModule", Id = "MySQLDataAssetCache")]
    public class MySQLDataCache : ISharedRegionModule, IAssetCache, IAssetService
    {
        private static readonly ILog m_log =
                LogManager.GetLogger(
                MethodBase.GetCurrentMethod().DeclaringType);

        private bool m_Enabled = false;

        private const string m_ModuleName = "MySQLDataAssetCache";

        private static ulong m_Requests = 0;
        private static ulong m_DatabaseHits = 0;
        private static ulong m_weakRefHits = 0;

        private static HashSet<string> m_inDBQueue = new HashSet<string>();
        private static Queue<AssetBase> m_dbQueue = new Queue<AssetBase>();
        private static ManualResetEvent m_dbSignal = new ManualResetEvent(true);
        private static object m_dbQueueSync = new object();

        private static volatile bool m_Running = false;

        // half an hour
        private static int m_timerStartTime  = 1800000;
        // and hour
        private static int m_timerRepeatTime = 3600000;
        private static double m_cacheTimeout = 0.0;
        private static object m_timerLock = new object();

        private MySQLAssetCacheData m_database = null;
        private string m_connectionString = string.Empty;

        private IAssetService m_AssetService;
        private List<Scene> m_Scenes = new List<Scene>();
        private static object m_TouchLock = new object();

        private Dictionary<string,WeakReference> weakAssetReferences = new Dictionary<string, WeakReference>();
        private object weakAssetReferencesLock = new object();

        private static System.Threading.Timer[] m_queueTimer = new System.Threading.Timer[2] { null, null };

        private static System.Threading.Timer m_expireTimer = null;

        public MySQLDataCache()
        {

        }

        public Type ReplaceableInterface
        {
            get { return null; }
        }

        public string Name
        {
            get { return m_ModuleName; }
        }

        public void Initialise(IConfigSource source)
        {
            IConfig moduleConfig = source.Configs["Modules"];

            if (moduleConfig != null)
            {
                string name = moduleConfig.GetString("AssetCaching", string.Empty);

                if (name == Name)
                {
                    m_Enabled = false;

                    m_log.InfoFormat("[DATABASE ASSET CACHE]: {0} enabled", this.Name);

                    IConfig assetConfig = source.Configs["AssetCache"];
                    if (assetConfig == null)
                    {
                        m_log.Debug(
                           "[DATABASE ASSET CACHE]: AssetCache section missing from config (not copied config-include/FlotsamCache.ini.example?  Using defaults.");
                    }
                    else
                    {                        
                        m_Enabled = assetConfig.GetBoolean("CacheEnabled", m_Enabled);
                        if (m_Enabled)
                        {
                            m_connectionString = assetConfig.GetString("ConnectionString", m_connectionString);

                            if (m_connectionString == string.Empty)
                            {
                                m_Enabled = false;
                            }
                            else try
                            {
                                m_database = new MySQLAssetCacheData();
                                m_database.Initialise(m_connectionString);

                                m_cacheTimeout = assetConfig.GetDouble("CacheTimeout", m_cacheTimeout);
                            }
                            catch
                            {
                                m_Enabled = false;
                            }
                        }
                    }

                    MainConsole.Instance.Commands.AddCommand("Assets", true, "dbcache status", "dbcache status", "Display cache status", HandleConsoleCommand);
                    MainConsole.Instance.Commands.AddCommand("Assets", true, "dbcache assets", "dbcache assets", "Attempt a deep scan and cache of all assets in all scenes", HandleConsoleCommand);
                    MainConsole.Instance.Commands.AddCommand("Assets", true, "dbcache expire", "dbcache expire <hours>", "Purge cached assets older then the specified hours", HandleConsoleCommand);
                }
            }
        }

        public void PostInitialise()
        {
        }

        public void Close()
        {
        }

        // Run only in m_dbQueueSync lock.
        private void StartHandlerThreads()
        {
            if (!m_Running)
            {
                m_Running = true;

                for (int i=0; i<2; i++)
                     m_queueTimer[i] = new System.Threading.Timer( 
                                           delegate { HandleDatabaseCacheRequests(this); },
                                                      null, 0, Timeout.Infinite);

                if (m_cacheTimeout > 0.0)
                    m_expireTimer = new System.Threading.Timer(
                                           delegate { HandleExpireTimer(this); },
                                                      null, m_timerStartTime, Timeout.Infinite);
            }
        }

        public void AddRegion(Scene scene)
        {
            if (m_Enabled)
            {
                scene.RegisterModuleInterface<IAssetCache>(this);
                m_Scenes.Add(scene);

                lock (m_dbQueueSync)
                {
                    m_dbSignal.Set();
                    StartHandlerThreads();
                }
            }
        }

        public void RemoveRegion(Scene scene)
        {
            if (m_Enabled)
            {
                scene.UnregisterModuleInterface<IAssetCache>(this);
                m_Scenes.Remove(scene);

                if (m_Scenes.Count <= 0)
                {
                    lock (m_dbQueueSync)
                    {
                        m_Running = false;
                        m_dbQueue.Clear();
                        m_dbSignal.Set(); // To wake the handler thread so it sees the change.
                    }

                    lock (m_inDBQueue)
                    {
                        m_inDBQueue.Clear();
                    }
                }
            }
        }

        public void RegionLoaded(Scene scene)
        {
            if (m_Enabled)
            {
                if(m_AssetService == null)
                    m_AssetService = scene.RequestModuleInterface<IAssetService>();

                lock (weakAssetReferencesLock)
                    weakAssetReferences = new Dictionary<string, WeakReference>();

                RunTouchAllAssetsScan(true);
            }
        }

        ////////////////////////////////////////////////////////////
        // IAssetCache
        //

        private void UpdateWeakReference(AssetBase asset)
        {
            WeakReference aref = new WeakReference(asset);
            lock(weakAssetReferencesLock)
                weakAssetReferences[asset.ID] = aref;
        }

        private void UpdateDatabaseCache(AssetBase asset)
        {
            try
            {
                lock (m_inDBQueue)
                {
                    if (!m_inDBQueue.Add(asset.ID))
                    {
                        // was already in the queue,
                        // some other thread must have snuck it in.
                        // Which is impressive but not impossible.
                        return;
                    }
                }

                lock (m_dbQueueSync)
                {
                    m_dbQueue.Enqueue(asset);
                    m_dbSignal.Set();

                    StartHandlerThreads();
                }
            }
            catch (Exception e)
            {
                m_log.ErrorFormat(
                    "[ASSET CACHE]: Failed to update cache database for asset {0}.  Exception {1} {2}",
                    asset.ID, e.Message, e.StackTrace);
            }
        }

        public void Cache(AssetBase asset)
        {
            if (asset != null)
            {
                UpdateWeakReference(asset);
                UpdateDatabaseCache(asset);
            }
        }

        public void CacheNegative(string id)
        {
            // We do not use a negative cache.
        }

        private AssetBase GetFromWeakReference(string id)
        {
            AssetBase asset = null;
            WeakReference aref;

            lock(weakAssetReferencesLock)
            {
                if (!weakAssetReferences.TryGetValue(id, out aref))
                    return null;

                asset = aref.Target as AssetBase;
                if(asset == null)
                { 
                    weakAssetReferences.Remove(id);
                    return null;                   
                }
            }
            m_weakRefHits++;
            return asset;
        }

        private AssetBase GetFromDatabaseCache(string id)
        {
            AssetBase asset = m_database.GetAsset(id);
            if (asset != null)
                m_DatabaseHits++;
            
            return asset;
        }

        private bool CheckFromDatabaseCache(string id)
        {
            return m_database.AssetExists(id);
        }

        // Used by HandleDatabaseCacheRequests
        public bool WriteAssetDataToDatabase(AssetBase asset)
        {
            try
            { 
                if (m_database.AssetExists(asset.ID))
                    return true;    

                return m_database.StoreAsset(asset);    
            }
            finally
            {
                // Even if the write fails with an exception, we need to make sure
                // that we release the lock on that asset, otherwise it'll never get
                // cached
                lock (m_inDBQueue)
                {
                    // we use key instead of filename now because key is shorter
                    // and just as unique.
                    m_inDBQueue.Remove(asset.ID);
                }
            }
        }

        // Used by HandleExpireTimer
        public void RemoveExpiredAssets()
        {
            try
            {
                m_database.Expire(m_cacheTimeout);
            }
            catch { }
        }

        // For IAssetService
        public AssetBase Get(string id)
        {
            AssetBase asset;
            Get(id, out asset);
            return asset;
        }

        public bool Get(string id, out AssetBase asset)
        {
            m_Requests++;

            asset = GetFromWeakReference(id);
            if (asset == null)
            {
                asset = GetFromDatabaseCache(id);
            }
            return true;
        }

        public bool Check(string id)
        {
            return CheckFromDatabaseCache(id);
        }

        public AssetBase GetCached(string id)
        {
            AssetBase asset;
            Get(id, out asset);
            return asset;
        }

        public void Expire(string id)
        {
            try
            {
                m_database.Delete(id);

                lock (weakAssetReferencesLock)
                      weakAssetReferences.Remove(id);
            }
            catch { }
        }

        public void Clear()
        {
            // We do not clear the database since it can be shared by multiple simulators.
            lock(weakAssetReferencesLock)
                weakAssetReferences = new Dictionary<string, WeakReference>();
        }

        /// <summary>
        /// Iterates through all Scenes, doing a deep scan through assets
        /// to update the access time of all assets present in the scene or referenced by assets
        /// in the scene.
        /// </summary>
        /// <returns>Number of distinct asset references found in the scene.</returns>
        private int TouchAllSceneAssets(bool RestartScripts)
        {
            UuidGatherer gatherer = new UuidGatherer(m_AssetService);

            Dictionary<UUID, bool> assetsFound = new Dictionary<UUID, bool>();

            foreach (Scene s in m_Scenes)
            {
                if (RestartScripts)
                    s.HardRestartScripts();

                s.ForEachSOG(delegate(SceneObjectGroup e)
                {
                    gatherer.AddForInspection(e);
                    gatherer.GatherAll();

                    foreach (UUID assetID in gatherer.GatheredUuids.Keys)
                    {
                        if (!assetsFound.ContainsKey(assetID))
                        {
                            string ID = assetID.ToString();

                            if (m_database.AssetExists(ID))
                            { 
                                m_database.UpdateAccessTime(ID);
                                assetsFound[assetID] = true;
                            }
                            else
                            {
                                // And HERE we fall back on the asset service (so basicly the grid)
                                // to te the asset. This will also cache the newly gotten asset.
                                AssetBase cachedAsset = m_AssetService.Get(ID);
                                if (cachedAsset == null && gatherer.GatheredUuids[assetID] != (sbyte)AssetType.Unknown)
                                    assetsFound[assetID] = false;
                                else
                                    assetsFound[assetID] = true;
                            }
                        }
                        else if (!assetsFound[assetID])
                        {
                            m_log.DebugFormat(
                                "[ASSET CACHE]: Could not find asset {0}, type {1} referenced by object {2} at {3} in scene {4} when pre-caching all scene assets",
                                assetID, gatherer.GatheredUuids[assetID], e.Name, e.AbsolutePosition, s.Name);
                        }
                    }
                    gatherer.GatheredUuids.Clear();
                });
            }
            return assetsFound.Count;
        }

        /// <summary>
        /// Deletes all cache contents
        /// </summary>

        private List<string> GenerateCacheHitReport()
        {
            List<string> outputLines = new List<string>();

            try { }
            finally // a finnally block can not be interrupted.
            {
                if (Monitor.TryEnter(m_TouchLock))
                {
                    // We could enter the lock so it was not set.
                    Monitor.Exit(m_TouchLock);
                }
                else
                {
                    // We could noy enter the lock so it was already set.
                    outputLines.Add("Beware, assets are being cached at this moment.");
                }
            }

            double invReq = (m_Requests > 0) ? 100.0 / m_Requests : 1;

            double weakHitRate = m_weakRefHits * invReq;
            int weakEntries = weakAssetReferences.Count;

            double TotalHitRate = weakHitRate;

            outputLines.Add(
                string.Format("Total requests: {0}", m_Requests));

            outputLines.Add(
                string.Format("unCollected Hit Rate: {0}% ({1} entries)", weakHitRate.ToString("0.00"),weakEntries));

            double HitRate = m_DatabaseHits * invReq;
            outputLines.Add(
                string.Format("Database Hit Rate: {0}%", HitRate.ToString("0.00")));

            TotalHitRate += HitRate;

            outputLines.Add(
                string.Format("Total Hit Rate: {0}%", TotalHitRate.ToString("0.00")));

            return outputLines;
        }

        #region Console Commands
        
        private void RunTouchAllAssetsScan(bool wait)
        {
            ICommandConsole con = MainConsole.Instance;

            if (Monitor.TryEnter(m_TouchLock))
            {
                // We could set the lock so it was not already set.
                try
                {
                    con.Output("Ensuring assets are cached for all scenes.");
                    WorkManager.RunInThreadPool(delegate
                    {
                        lock (m_TouchLock)
                        {
                            // we only wait at region load start to make sure all
                            // (or most) assets were loaded.
                            if (wait)
                            {
                                Thread.Sleep(30000);
                            }

                            int assetReferenceTotal = TouchAllSceneAssets(wait);
                            GC.Collect();

                            con.OutputFormat("Completed check with {0} assets.", assetReferenceTotal);
                        }
                    }, null, "TouchAllSceneAssets", false);
                }
                finally
                {
                    Monitor.Exit(m_TouchLock);
                }
            }
            else
            { 
                // we could not set the lock so it was already set.
                con.OutputFormat("Database assets cache check already running");
            }
        }

        private void HandleConsoleCommand(string module, string[] cmdparams)
        {
            ICommandConsole con = MainConsole.Instance;

            if (cmdparams.Length >= 2)
            {
                string cmd = cmdparams[1];

                switch (cmd)
                {
                    case "status":
                        GenerateCacheHitReport().ForEach(l => con.Output(l));
                        break;

                    case "assets":
                        RunTouchAllAssetsScan(false);
                        break;

                    case "expire":
                        try { }
                        finally // a finally block can not to interrupted.
                        {
                            if (Monitor.TryEnter(m_TouchLock))
                            {
                                Monitor.Exit(m_TouchLock);
                            }
                            else
                            {
                                con.OutputFormat("Please wait until all assets are cached.", cmd);
                            }
                        }    
                        if (cmdparams.Length < 3)
                        {
                            con.OutputFormat("Invalid parameters for Expire, please specify a valid date & time", cmd);
                            break;
                        }

                        double hours = 0;
                        if (!Double.TryParse( cmdparams[2], out hours ))
                        { 
                            con.OutputFormat("{0} is not a valid number of hours", cmd);
                            break;
                        }

                        Util.FireAndForget(
                            delegate
                            {
                                try
                                {
                                    m_database.Expire(hours);
                                } catch { }
                            },null,"ExpireDatabaseCache",false);

                        break;
                    default:
                        con.OutputFormat("Unknown command {0}", cmd);
                        break;
                }
            }
            else if (cmdparams.Length == 1)
            {
                con.Output("dbcache assets - Attempt a deep cache of all assets in all scenes");
                con.Output("dbcache expire <hours> - Purge assets older then the specified number of hours.");
                con.Output("dbcache status - Display cache status");
            }
        }

        #endregion

        #region IAssetService Members

        public AssetMetadata GetMetadata(string id)
        {
            AssetBase asset;
            Get(id, out asset);
            return asset.Metadata;
        }

        public byte[] GetData(string id)
        {
            AssetBase asset;
            Get(id, out asset);
            return asset.Data;
        }

        public bool Get(string id, object sender, AssetRetrieved handler)
        {
            AssetBase asset;
            if (!Get(id, out asset))
                return false;
            handler(id, sender, asset);
            return true;
        }

        public bool[] AssetsExist(string[] ids)
        {
            bool[] exist = new bool[ids.Length];

            for (int i = 0; i < ids.Length; i++)
            {
                exist[i] = Check(ids[i]);
            }

            return exist;
        }

        public string Store(AssetBase asset)
        {
            if (asset.FullID == UUID.Zero)
            {
                asset.FullID = UUID.Random();
            }

            Cache(asset);

            return asset.ID;
        }

        public bool UpdateContent(string id, byte[] data)
        {
            AssetBase asset;
            if (!Get(id, out asset))
                return false;
            asset.Data = data;
            Cache(asset);
            return true;
        }

        public bool Delete(string id)
        {
            Expire(id);
            return true;
        }

        #endregion

        private static void HandleDatabaseCacheRequests(MySQLDataCache CacheHandler)
        {
            while (m_Running)
            {
                try
                {
                    AssetBase asset = null;

                    bool wait = false;
                    lock (m_dbQueue)
                    {
                        if (m_dbQueue.Count > 0)
                        {
                            asset = m_dbQueue.Dequeue();
                        }
                        else
                        {
                            wait = true;
                            try
                            {
                                // Reset flag to wait for a new request.
                                m_dbSignal.Reset();
                            }
                            catch { }
                        }
                    }

                    // Wait until there are new requests.
                    // We want to wait outside of the lock.
                    if (wait)
                    {
                        try
                        {
                            m_dbSignal.WaitOne();
                        }
                        catch { }
                        // get next request.
                        continue;
                    }

                    try
                    {
                        CacheHandler.WriteAssetDataToDatabase(asset);
                    }
                    catch { }

                    // Make sure the thread stays awake while there are requests.
                    m_dbSignal.Set();
                }
                catch { }
            }
            // exiting the thread now
            try
            {
                m_dbSignal.Set(); // Wake other threads as well so they will end.
            }
            catch { }
        }

        private static void HandleExpireTimer(MySQLDataCache CacheHandler)
        {
            if (m_Running && Monitor.TryEnter(m_timerLock))
            {
                try
                {
                    CacheHandler.RemoveExpiredAssets();
                }
                finally
                {
                    Monitor.Exit(m_timerLock);

                    if (m_Running)
                        m_expireTimer.Change(m_timerRepeatTime, 0); 
                }
            }
        }
    }
}
