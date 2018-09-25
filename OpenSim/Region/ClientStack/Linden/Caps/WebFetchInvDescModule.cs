/* 30 August 2018
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
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using log4net;
using Nini.Config;
using Mono.Addins;
using OpenMetaverse;
using OpenSim.Framework.Servers.HttpServer;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Region.Framework.Scenes;
using OpenSim.Framework.Capabilities;
using OpenSim.Services.Interfaces;
using Caps = OpenSim.Framework.Capabilities.Caps;
using OpenSim.Capabilities.Handlers;
using OpenSim.Framework.Monitoring;

namespace OpenSim.Region.ClientStack.Linden
{
    /// <summary>
    /// This module implements both WebFetchInventoryDescendents and FetchInventoryDescendents2 capabilities.
    /// </summary>
    [Extension(Path = "/OpenSim/RegionModules", NodeName = "RegionModule", Id = "WebFetchInvDescModule")]
    public class WebFetchInvDescModule : INonSharedRegionModule
    {
        class aPollRequest
        {
            public PollServiceInventoryEventArgs thepoll;
            public UUID reqID;
            public Hashtable request;
            public ScenePresence presence;
            public List<UUID> folders;
        }

        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// Control whether requests will be processed asynchronously.
        /// </summary>
        /// <remarks>
        /// Defaults to true.  Can currently not be changed once a region has been added to the module.
        /// </remarks>
        public bool ProcessQueuedRequestsAsync { get; private set; }

        /// <summary>
        /// Number of inventory requests processed by this module.
        /// </summary>
        /// <remarks>
        /// It's the PollServiceRequestManager that actually sends completed requests back to the requester.
        /// </remarks>
        public static int ProcessedRequestsCount { get; set; }

        private static Stat s_queuedRequestsStat;
        private static Stat s_processedRequestsStat;

        private static Scene m_scene = null;
        public Scene Scene { get { return m_scene; }
                             private set { m_scene = value; }  }

        private static IInventoryService m_InventoryService;
        private static ILibraryService m_LibraryService;

        private bool m_Enabled;

        private string m_fetchInventoryDescendents2Url;

        // private static FetchInvDescHandler m_webFetchHandler = null;

        private static int m_NumberScenes = 0;

        private static Queue<aPollRequest> m_queue = new Queue<aPollRequest>();
        private static ManualResetEvent m_signal = new ManualResetEvent(true);

        private static object m_queueSync = new object();
        private static volatile bool m_running = true;

        private static System.Threading.Timer[] m_queueTimer = new System.Threading.Timer[2] { null, null };

        #region ISharedRegionModule Members

        public WebFetchInvDescModule() : this(true) {}

        public WebFetchInvDescModule(bool processQueuedResultsAsync)
        {
            ProcessQueuedRequestsAsync = true; //  processQueuedResultsAsync;
        }

        public void Initialise(IConfigSource source)
        {
            IConfig config = source.Configs["ClientStack.LindenCaps"];
            if (config == null)
                return;

            m_fetchInventoryDescendents2Url = config.GetString("Cap_FetchInventoryDescendents2", string.Empty);
            if (m_fetchInventoryDescendents2Url != string.Empty)
            {
                m_Enabled = true;
            }
        }

        public void AddRegion(Scene s)
        {
            if (!m_Enabled)
                return;

            Scene = s;
        }

        public void RemoveRegion(Scene s)
        {
            if (!m_Enabled)
                return;

            Scene.EventManager.OnRegisterCaps -= RegisterCaps;

            StatsManager.DeregisterStat(s_processedRequestsStat);
            StatsManager.DeregisterStat(s_queuedRequestsStat);

            m_NumberScenes--;
            Scene = null;

            if (m_NumberScenes <= 0)
            {
                m_running = false;
            }
        }

        public void RegionLoaded(Scene s)
        {
            if (!m_Enabled)
                return;

            if (s_processedRequestsStat == null)
                s_processedRequestsStat =
                    new Stat(
                        "ProcessedFetchInventoryRequests",
                        "Number of processed fetch inventory requests",
                        "These have not necessarily yet been dispatched back to the requester.",
                        "",
                        "inventory",
                        "httpfetch",
                        StatType.Pull,
                        MeasuresOfInterest.AverageChangeOverTime,
                        stat => { stat.Value = ProcessedRequestsCount; },
                        StatVerbosity.Debug);

            if (s_queuedRequestsStat == null)
            {
                s_queuedRequestsStat =
                    new Stat(
                        "QueuedFetchInventoryRequests",
                        "Number of fetch inventory requests queued for processing",
                        "",
                        "",
                        "inventory",
                        "httpfetch",
                        StatType.Pull,
                        MeasuresOfInterest.AverageChangeOverTime,
                        stat => { lock (m_queue) { stat.Value = m_queue.Count; } },
                        StatVerbosity.Debug);
            }

            StatsManager.RegisterStat(s_processedRequestsStat);
            StatsManager.RegisterStat(s_queuedRequestsStat);

            m_InventoryService = Scene.InventoryService;
            m_LibraryService = Scene.LibraryService;

            Scene.EventManager.OnRegisterCaps += RegisterCaps;

            m_NumberScenes++;

            if (m_NumberScenes == 1)
            {
                m_running = true;
                for (int i = 0; i < 2; i++)
                {
                    m_queueTimer[i] = new System.Threading.Timer(
                                           delegate { DoInventoryRequests(); },
                                           null, 0, Timeout.Infinite);
                }
            }
        }

        public void PostInitialise()
        {
        }

        public void Close()
        {
            if (!m_Enabled)
                return;

            if (m_NumberScenes <= 0)
            {
                m_log.DebugFormat("[WebFetchInvDescModule] Closing");
                try
                {
                    lock (m_queueSync)
                    {
                        m_running = false;
                        m_queue.Clear();

                        // Wake the threads so they will notice m_running = false and end.
                        m_signal.Set();
                    }
                }
                catch { }
            }
        }

        public string Name { get { return "WebFetchInvDescModule"; } }

        public Type ReplaceableInterface
        {
            get { return null; }
        }

        #endregion

        private class PollServiceInventoryEventArgs : PollServiceEventArgs
        {
            private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

            private Dictionary<UUID, Hashtable> responses =
                    new Dictionary<UUID, Hashtable>();

            private WebFetchInvDescModule m_module;

            public PollServiceInventoryEventArgs(WebFetchInvDescModule module, string url, UUID pId) :
                base(null, url, null, null, null, pId, int.MaxValue)
            {
                m_module = module;

                HasEvents = (x, y) => { lock (responses) return responses.ContainsKey(x); };
                GetEvents = (x, y) =>
                {
                    lock (responses)
                    {
                        try
                        {
                            return responses[x];
                        }
                        finally
                        {
                            responses.Remove(x);
                        }
                    }
                };

                Request = (x, y) =>
                {
                    ScenePresence sp = m_module.Scene.GetScenePresence(Id);

                    aPollRequest reqinfo = new aPollRequest();
                    reqinfo.thepoll = this;
                    reqinfo.reqID = x;
                    reqinfo.request = y;
                    reqinfo.presence = sp;
                    reqinfo.folders = new List<UUID>();

                    // Decode the request here
                    string request = y["body"].ToString();

                    request = request.Replace("<string>00000000-0000-0000-0000-000000000000</string>", "<uuid>00000000-0000-0000-0000-000000000000</uuid>");

                    request = request.Replace("<key>fetch_folders</key><integer>0</integer>", "<key>fetch_folders</key><boolean>0</boolean>");
                    request = request.Replace("<key>fetch_folders</key><integer>1</integer>", "<key>fetch_folders</key><boolean>1</boolean>");

                    Hashtable hash = new Hashtable();
                    try
                    {
                        hash = (Hashtable)LLSD.LLSDDeserialize(Utils.StringToBytes(request));
                    }
                    catch (LLSD.LLSDParseException e)
                    {
                        m_log.ErrorFormat("[INVENTORY]: Fetch error: {0}{1}" + e.Message, e.StackTrace);
                        m_log.Error("Request: " + request);
                        return;
                    }
                    catch (System.Xml.XmlException)
                    {
                        m_log.ErrorFormat("[INVENTORY]: XML Format error");
                    }

                    ArrayList foldersrequested = (ArrayList)hash["folders"];

                    for (int i = 0; i < foldersrequested.Count; i++)
                    {
                        Hashtable inventoryhash = (Hashtable)foldersrequested[i];
                        string folder = inventoryhash["folder_id"].ToString();
                        UUID folderID;
                        if (UUID.TryParse(folder, out folderID))
                        {
                            if (!reqinfo.folders.Contains(folderID))
                            {
                                reqinfo.folders.Add(folderID);
                            }
                        }
                    }

                    lock (m_queueSync)
                    {
                        m_queue.Enqueue(reqinfo);
                        m_signal.Set();
                    }
                };

                NoEvents = (x, y) =>
                {
                    Hashtable response = new Hashtable();

                    response["int_response_code"] = 500;
                    response["str_response_string"] = "Script timeout";
                    response["content_type"] = "text/plain";
                    response["keepalive"] = false;
                    response["reusecontext"] = false;

                    return response;
                };
            }

            public void Process(aPollRequest requestinfo, FetchInvDescHandler webFetchHandler)
            {
                if(m_module == null || m_module.Scene == null || m_module.Scene.ShuttingDown)
                    return;

                UUID requestID = requestinfo.reqID;

                Hashtable response = new Hashtable();

                response["int_response_code"] = 200;
                response["content_type"] = "text/plain";
                response["keepalive"] = false;
                response["reusecontext"] = false;

                response["str_response_string"] = webFetchHandler.FetchInventoryDescendentsRequest(
                        requestinfo.request["body"].ToString(), String.Empty, String.Empty, null, null);

                lock (responses)
                {
                    if (responses.ContainsKey(requestID))
                        m_log.WarnFormat("[FETCH INVENTORY DESCENDENTS2 MODULE]: Caught in the act of loosing responses! Please report this on mantis #7054");
                    responses[requestID] = response;
                }
                requestinfo.folders.Clear();
                requestinfo.request.Clear();
                WebFetchInvDescModule.ProcessedRequestsCount++;
            }
        }

        private void RegisterCaps(UUID agentID, Caps caps)
        {
            RegisterFetchDescendentsCap(agentID, caps, "FetchInventoryDescendents2", m_fetchInventoryDescendents2Url);
        }

        private void RegisterFetchDescendentsCap(UUID agentID, Caps caps, string capName, string url)
        {
            string capUrl;

            // disable the cap clause
            if (url == "")
            {
                return;
            }
            // handled by the simulator
            else if (url == "localhost")
            {
                capUrl = "/CAPS/" + UUID.Random() + "/";

                // Register this as a poll service
                PollServiceInventoryEventArgs args = new PollServiceInventoryEventArgs(this, capUrl, agentID);
                args.Type = PollServiceEventArgs.EventType.Inventory;

                caps.RegisterPollHandler(capName, args);
            }
            // external handler
            else
            {
                capUrl = url;
                IExternalCapsModule handler = Scene.RequestModuleInterface<IExternalCapsModule>();
                if (handler != null)
                    handler.RegisterExternalUserCapsHandler(agentID,caps,capName,capUrl);
                else
                    caps.RegisterHandler(capName, capUrl);
            }
        }

        private static void DoInventoryRequests()
        {
            FetchInvDescHandler getHandler = new FetchInvDescHandler(m_InventoryService, m_LibraryService, m_scene);
            while (m_running)
            {
                try
                {
                    aPollRequest poolreq = new aPollRequest();
                    bool wait = false;
                    lock (m_queueSync)
                    {
                        if (m_queue.Count > 0)
                        {
                            poolreq = m_queue.Dequeue();
                            m_signal.Set();
                        }
                        else
                        {
                            // Reset flag to wait for a new requests.
                            m_signal.Reset();
                            wait = true;
                        }
                    }

                    try
                    {
                        if (wait)
                        {
                            m_signal.WaitOne();
                        }
                        else
                        {
                            poolreq.thepoll.Process(poolreq, getHandler);
                        }
                    }
                    catch { }

                    // Make sure the thread stays awake while there are requests.
                    m_signal.Set();
                }
                catch { }
            }
            // exiting the thread now
            try
            {
                m_signal.Set(); // Wake other threads as well so they will end.
            }
            catch { }
        }
    }
}
