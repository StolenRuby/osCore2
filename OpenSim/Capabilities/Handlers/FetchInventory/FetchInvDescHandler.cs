/* 24 July 2018
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
using System.Linq;
using System.Reflection;
using log4net;
using Nini.Config;
using OpenMetaverse;
using OpenMetaverse.StructuredData;
using OpenSim.Framework;
using OpenSim.Framework.Capabilities;
using OpenSim.Region.Framework.Interfaces;
using OpenSim.Framework.Servers.HttpServer;
using OpenSim.Services.Interfaces;
using Caps = OpenSim.Framework.Capabilities.Caps;

namespace OpenSim.Capabilities.Handlers
{
    public class FetchInvDescHandler
    {
        private static int m_badcachetime = 1800; // 30 minutes? 

        private static readonly ILog m_log =
            LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private IInventoryService m_InventoryService;
        private ILibraryService m_LibraryService;
        private IScene m_Scene;

        private readonly static ExpiringCache<UUID, HashSet<UUID>> badfoldercache = new ExpiringCache<UUID, HashSet<UUID>>();

        public FetchInvDescHandler(IInventoryService invService, ILibraryService libService, IScene s)
        {
            m_InventoryService = invService;
            m_LibraryService = libService;
            m_Scene = s;
        }

        public string FetchInventoryDescendentsRequest(string request, string path, string param, IOSHttpRequest httpRequest, IOSHttpResponse httpResponse)
        {
            //m_log.DebugFormat("[XXX]: FetchInventoryDescendentsRequest in {0}, {1}", (m_Scene == null) ? "none" : m_Scene.Name, request);

            // nasty temporary hack here, the linden client falsely
            // identifies the uuid 00000000-0000-0000-0000-000000000000
            // as a string which breaks us
            //
            // correctly mark it as a uuid
            //
            request = request.Replace("<string>00000000-0000-0000-0000-000000000000</string>", "<uuid>00000000-0000-0000-0000-000000000000</uuid>");

            // another hack <integer>1</integer> results in a
            // System.ArgumentException: Object type System.Int32 cannot
            // be converted to target type: System.Boolean
            //
            request = request.Replace("<key>fetch_folders</key><integer>0</integer>", "<key>fetch_folders</key><boolean>0</boolean>");
            request = request.Replace("<key>fetch_folders</key><integer>1</integer>", "<key>fetch_folders</key><boolean>1</boolean>");

            Hashtable hash = new Hashtable();
            try
            {
                hash = (Hashtable)LLSD.LLSDDeserialize(Utils.StringToBytes(request));
            }
            catch (LLSD.LLSDParseException e)
            {
                m_log.ErrorFormat("[WEB FETCH INV DESC HANDLER]: Fetch error: {0}{1}" + e.Message, e.StackTrace);
                m_log.Error("Request: " + request);
            }

            ArrayList foldersrequested = (ArrayList)hash["folders"];

            string response = "";
            string bad_folders_response = "";

            List<LLSDFetchInventoryDescendents> folders = new List<LLSDFetchInventoryDescendents>();
            for (int i = 0; i < foldersrequested.Count; i++)
            {
                Hashtable inventoryhash = (Hashtable)foldersrequested[i];

                LLSDFetchInventoryDescendents llsdRequest = new LLSDFetchInventoryDescendents();

                try
                {
                    LLSDHelpers.DeserialiseOSDMap(inventoryhash, llsdRequest);
                }
                catch (Exception e)
                {
                    m_log.Debug("[WEB FETCH INV DESC HANDLER]: caught exception doing OSD deserialize" + e);
                    continue;
                }

                // Filter duplicate folder ids that bad viewers may send
                if (folders.Find(f => f.folder_id == llsdRequest.folder_id) == null)
                    folders.Add(llsdRequest);
            }

            if (folders.Count > 0)
            {
                List<UUID> bad_folders = new List<UUID>();
                List<InventoryCollectionWithDescendents> invcollSet = Fetch(folders, bad_folders);

                if (invcollSet == null)
                {
                    m_log.DebugFormat("[WEB FETCH INV DESC HANDLER]: Multiple folder fetch failed. Trying old protocol.");
#pragma warning disable 0612
                    return FetchInventoryDescendentsRequest(foldersrequested, httpRequest, httpResponse);
#pragma warning restore 0612
                }

                string inventoryitemstr = string.Empty;
                foreach (InventoryCollectionWithDescendents icoll in invcollSet)
                {
                    LLSDInventoryDescendents reply = ToLLSD(icoll.Collection, icoll.Descendents);

                    inventoryitemstr = LLSDHelpers.SerialiseLLSDReply(reply);
                    inventoryitemstr = inventoryitemstr.Replace("<llsd><map><key>folders</key><array>", "");
                    inventoryitemstr = inventoryitemstr.Replace("</array></map></llsd>", "");

                    response += inventoryitemstr;
                }

                foreach (UUID bad in bad_folders)
                    bad_folders_response += "<uuid>" + bad + "</uuid>";
            }

            if (response.Length == 0)
            {
                /* Viewers expect a bad_folders array when not available */
                if (bad_folders_response.Length != 0)
                {
                    response = "<llsd><map><key>bad_folders</key><array>" + bad_folders_response + "</array></map></llsd>";
                }
                else
                {
                    response = "<llsd><map><key>folders</key><array /></map></llsd>";
                }
            }
            else
            {
                if (bad_folders_response.Length != 0)
                {
                    response = "<llsd><map><key>folders</key><array>" + response + "</array><key>bad_folders</key><array>" + bad_folders_response + "</array></map></llsd>";
                }
                else
                {
                    response = "<llsd><map><key>folders</key><array>" + response + "</array></map></llsd>";
                }
            }

            return response;
        }

        /// <summary>
        /// Construct an LLSD reply packet to a CAPS inventory request
        /// </summary>
        /// <param name="invFetch"></param>
        /// <returns></returns>
        private LLSDInventoryDescendents FetchInventoryReply(LLSDFetchInventoryDescendents invFetch)
        {
            LLSDInventoryDescendents reply = new LLSDInventoryDescendents();
            LLSDInventoryFolderContents contents = new LLSDInventoryFolderContents();
            contents.agent_id = invFetch.owner_id;
            contents.owner_id = invFetch.owner_id;
            contents.folder_id = invFetch.folder_id;

            reply.folders.Array.Add(contents);
            InventoryCollection inv = new InventoryCollection();
            inv.Folders = new List<InventoryFolderBase>();
            inv.Items = new List<InventoryItemBase>();
            int version = 0;
            int descendents = 0;

#pragma warning disable 0612
            inv = Fetch(
                    invFetch.owner_id, invFetch.folder_id, invFetch.owner_id,
                    invFetch.fetch_folders, invFetch.fetch_items, invFetch.sort_order, out version, out descendents);
#pragma warning restore 0612

            if (inv != null && inv.Folders != null)
            {
                foreach (InventoryFolderBase invFolder in inv.Folders)
                {
                    contents.categories.Array.Add(ConvertInventoryFolder(invFolder));
                }
                descendents += inv.Folders.Count;
            }

            if (inv != null && inv.Items != null)
            {
                foreach (InventoryItemBase invItem in inv.Items)
                {
                    contents.items.Array.Add(ConvertInventoryItem(invItem));
                }
            }

            contents.descendents = descendents;
            contents.version = version;

            return reply;
        }

        private LLSDInventoryDescendents ToLLSD(InventoryCollection inv, int descendents)
        {
            LLSDInventoryDescendents reply = new LLSDInventoryDescendents();
            LLSDInventoryFolderContents contents = new LLSDInventoryFolderContents();
            contents.agent_id = inv.OwnerID;
            contents.owner_id = inv.OwnerID;
            contents.folder_id = inv.FolderID;

            reply.folders.Array.Add(contents);

            if (inv.Folders != null)
            {
                foreach (InventoryFolderBase invFolder in inv.Folders)
                {
                    contents.categories.Array.Add(ConvertInventoryFolder(invFolder));
                }
                descendents += inv.Folders.Count;
            }

            if (inv.Items != null)
            {
                foreach (InventoryItemBase invItem in inv.Items)
                {
                    contents.items.Array.Add(ConvertInventoryItem(invItem));
                }
            }

            contents.descendents = descendents;
            contents.version = inv.Version;

            return reply;
        }

        /// <summary>
        /// Old style. Soon to be deprecated.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="httpRequest"></param>
        /// <param name="httpResponse"></param>
        /// <returns></returns>
        [Obsolete]
        private string FetchInventoryDescendentsRequest(ArrayList foldersrequested, IOSHttpRequest httpRequest, IOSHttpResponse httpResponse)
        {
            string response = "";
            string bad_folders_response = "";

            for (int i = 0; i < foldersrequested.Count; i++)
            {
                string inventoryitemstr = "";
                Hashtable inventoryhash = (Hashtable)foldersrequested[i];

                LLSDFetchInventoryDescendents llsdRequest = new LLSDFetchInventoryDescendents();

                try
                {
                    LLSDHelpers.DeserialiseOSDMap(inventoryhash, llsdRequest);
                }
                catch (Exception e)
                {
                    m_log.Debug("[WEB FETCH INV DESC HANDLER]: caught exception doing OSD deserialize" + e);
                }

                LLSDInventoryDescendents reply = FetchInventoryReply(llsdRequest);

                if (null == reply)
                {
                    bad_folders_response += "<uuid>" + llsdRequest.folder_id.ToString() + "</uuid>";
                }
                else
                {
                    inventoryitemstr = LLSDHelpers.SerialiseLLSDReply(reply);
                    inventoryitemstr = inventoryitemstr.Replace("<llsd><map><key>folders</key><array>", "");
                    inventoryitemstr = inventoryitemstr.Replace("</array></map></llsd>", "");
                }

                response += inventoryitemstr;
            }

            if (response.Length == 0)
            {
                /* Viewers expect a bad_folders array when not available */
                if (bad_folders_response.Length != 0)
                {
                    response = "<llsd><map><key>bad_folders</key><array>" + bad_folders_response + "</array></map></llsd>";
                }
                else
                {
                    response = "<llsd><map><key>folders</key><array /></map></llsd>";
                }
            }
            else
            {
                if (bad_folders_response.Length != 0)
                {
                    response = "<llsd><map><key>folders</key><array>" + response + "</array><key>bad_folders</key><array>" + bad_folders_response + "</array></map></llsd>";
                }
                else
                {
                    response = "<llsd><map><key>folders</key><array>" + response + "</array></map></llsd>";
                }
            }
            return response;
        }

        /// <summary>
        /// Handle the caps inventory descendents fetch.
        /// </summary>
        /// <param name="agentID"></param>
        /// <param name="folderID"></param>
        /// <param name="ownerID"></param>
        /// <param name="fetchFolders"></param>
        /// <param name="fetchItems"></param>
        /// <param name="sortOrder"></param>
        /// <param name="version"></param>
        /// <returns>An empty InventoryCollection if the inventory look up failed</returns>
        [Obsolete]
        private InventoryCollection Fetch(
            UUID agentID, UUID folderID, UUID ownerID,
            bool fetchFolders, bool fetchItems, int sortOrder, out int version, out int descendents)
        {
            version = 0;
            descendents = 0;

            if (Util.isKownBadInventoryFolder(ownerID, folderID))
            {
                return new InventoryCollection(); // known bad folder, return empty collection.
            }

            InventoryFolderImpl fold;
            if (m_LibraryService != null && m_LibraryService.LibraryRootFolder != null && agentID == m_LibraryService.LibraryRootFolder.Owner)
            {
                if ((fold = m_LibraryService.LibraryRootFolder.FindFolder(folderID)) != null)
                {
                    InventoryCollection ret = new InventoryCollection();
                    ret.Folders = new List<InventoryFolderBase>();
                    ret.Items = fold.RequestListOfItems();
                    descendents = ret.Folders.Count + ret.Items.Count;

                    return ret;
                }
            }

            InventoryCollection contents = new InventoryCollection();

            if (folderID != UUID.Zero)
            {
                InventoryCollection fetchedContents = null;
                try
                {
                    fetchedContents = m_InventoryService.GetFolderContent(agentID, folderID);
                }
                catch
                {
                    fetchedContents = null;
                }

                if (fetchedContents == null)
                {
                    m_log.WarnFormat("[WEB FETCH INV DESC HANDLER]: Could not get contents of folder {0} for user {1}", folderID, agentID);

                    Util.CacheBadInventoryFolder(ownerID, folderID, m_badcachetime);
                    return contents;
                }
                contents = fetchedContents;
                InventoryFolderBase containingFolder = m_InventoryService.GetFolder(agentID, folderID);

                if (containingFolder != null)
                {
                    version = containingFolder.Version;

                    if (fetchItems && containingFolder.Type != (short)FolderType.Trash)
                    {
                        List<InventoryItemBase> itemsToReturn = contents.Items;
                        List<InventoryItemBase> originalItems = new List<InventoryItemBase>(itemsToReturn);

                        // descendents must only include the links, not the linked items we add
                        descendents = originalItems.Count;

                        // Add target items for links in this folder before the links themselves.
                        foreach (InventoryItemBase item in originalItems)
                        {
                            if (item.AssetType == (int)AssetType.Link)
                            {
                                InventoryItemBase linkedItem = m_InventoryService.GetItem(agentID, item.AssetID);

                                // Take care of genuinely broken links where the target doesn't exist
                                // HACK: Also, don't follow up links that just point to other links.  In theory this is legitimate,
                                // but no viewer has been observed to set these up and this is the lazy way of avoiding cycles
                                // rather than having to keep track of every folder requested in the recursion.
                                if (linkedItem != null && linkedItem.AssetType != (int)AssetType.Link)
                                    itemsToReturn.Insert(0, linkedItem);
                            }
                        }
                    }
                }
            }
            else
            {
                // Lost items don't really need a version
                version = 1;
            }

            return contents;
        }

        private void AddLibraryFolders(List<LLSDFetchInventoryDescendents> fetchFolders, List<InventoryCollectionWithDescendents> result)
        {
            InventoryFolderImpl fold;
            if (m_LibraryService != null && m_LibraryService.LibraryRootFolder != null)
            {
                List<LLSDFetchInventoryDescendents> libfolders = fetchFolders.FindAll(f => f.owner_id == m_LibraryService.LibraryRootFolder.Owner);
                fetchFolders.RemoveAll(f => libfolders.Contains(f));

                foreach (LLSDFetchInventoryDescendents f in libfolders)
                {
                    if ((fold = m_LibraryService.LibraryRootFolder.FindFolder(f.folder_id)) != null)
                    {
                        InventoryCollectionWithDescendents ret = new InventoryCollectionWithDescendents();
                        ret.Collection = new InventoryCollection();
                        ret.Collection.Folders = new List<InventoryFolderBase>();
                        ret.Collection.Items = fold.RequestListOfItems();
                        ret.Collection.OwnerID = m_LibraryService.LibraryRootFolder.Owner;
                        ret.Collection.FolderID = f.folder_id;
                        ret.Collection.Version = fold.Version;

                        ret.Descendents = ret.Collection.Items.Count;
                        result.Add(ret);
                    }
                }
            }
        }

        private List<InventoryCollectionWithDescendents> Fetch(List<LLSDFetchInventoryDescendents> fetchFolders, List<UUID> bad_folders)
        {
            List<InventoryCollectionWithDescendents> result = new List<InventoryCollectionWithDescendents>();

            AddLibraryFolders(fetchFolders, result);

            // Filter folder Zero right here. Some viewers (Firestorm) send request for folder Zero, which doesn't make sense
            // and can kill the sim (all root folders have parent_id Zero)
            LLSDFetchInventoryDescendents zero = fetchFolders.Find(f => f.folder_id == UUID.Zero);
            if (zero != null)
            {
                fetchFolders.Remove(zero);
                BadFolder(zero, null, bad_folders);
            }

            if (fetchFolders.Count > 0)
            {
                UUID ownerid = fetchFolders[0].owner_id;

                List<UUID> fids = new List<UUID>();
                foreach (LLSDFetchInventoryDescendents f in fetchFolders)
                {
                    if (Util.isKownBadInventoryFolder(ownerid, f.folder_id))
                    {
                        BadFolder(f, null, bad_folders);
                    }
                    else
                    {
                        fids.Add(f.folder_id);
                    }
                }

                InventoryCollection[] fetchedContents = null;
                try
                {
                    fetchedContents = m_InventoryService.GetMultipleFoldersContent(ownerid, fids.ToArray());
                }
                catch
                {
                    fetchedContents = null;
                }

                if (fetchedContents == null || (fetchedContents != null && fetchedContents.Length == 0))
                {
                    m_log.WarnFormat("[WEB FETCH INV DESC HANDLER]: Could not get contents of multiple folders for user {0}", fetchFolders[0].owner_id);

                    foreach (LLSDFetchInventoryDescendents freq in fetchFolders)
                    {
                        BadFolder(freq, null, bad_folders);
                        Util.CacheBadInventoryFolder(freq.owner_id, freq.folder_id, m_badcachetime);
                    }
                    return null;
                }

                int i = 0;
                // Do some post-processing. May need to fetch more from inv server for links
                foreach (InventoryCollection contents in fetchedContents)
                {
                    // Find the original request
                    LLSDFetchInventoryDescendents freq = fetchFolders[i++];

                    InventoryCollectionWithDescendents coll = new InventoryCollectionWithDescendents();
                    coll.Collection = contents;

                    if (BadFolder(freq, contents, bad_folders))
                    {
                        Util.CacheBadInventoryFolder(freq.owner_id, freq.folder_id,m_badcachetime);
                        continue;
                    }

                    // Next: link management
                    ProcessLinks(freq, coll);

                    result.Add(coll);
                }
            }
            return result;
        }

        private bool BadFolder(LLSDFetchInventoryDescendents freq, InventoryCollection contents, List<UUID> bad_folders)
        {
            bool bad = false;
            if (contents == null)
            {
                bad_folders.Add(freq.folder_id);
                bad = true;
            }

            // The inventory server isn't sending FolderID in the collection...
            // Must fetch it individually
            else if (contents.FolderID == UUID.Zero)
            {
                try
                {
                    InventoryFolderBase containingFolder = m_InventoryService.GetFolder(freq.owner_id, freq.folder_id);

                    if (containingFolder != null)
                    {
                        contents.FolderID = containingFolder.ID;
                        contents.OwnerID = containingFolder.Owner;
                        contents.Version = containingFolder.Version;
                    }
                    else
                    {
                        // Was it really a request for folder Zero?
                        // This is an overkill, but Firestorm really asks for folder Zero.
                        // I'm leaving the code here for the time being, but commented.
                        if (freq.folder_id != UUID.Zero)
                        {
                            m_log.WarnFormat("[WEB FETCH INV DESC HANDLER]: Unable to fetch folder {0}", freq.folder_id);
                            bad_folders.Add(freq.folder_id);
                        }
                        bad = true;
                    }
                }
                catch { bad = true; }
            }

            return bad;
        }

        private void ProcessLinks(LLSDFetchInventoryDescendents freq, InventoryCollectionWithDescendents coll)
        {
            InventoryCollection contents = coll.Collection;

            if (freq.fetch_items && contents.Items != null)
            {
                List<InventoryItemBase> itemsToReturn = contents.Items;

                // descendents must only include the links, not the linked items we add
                coll.Descendents = itemsToReturn.Count;

                // Add target items for links in this folder before the links themselves.
                List<UUID> itemIDs = new List<UUID>();
                // List<UUID> folderIDs = new List<UUID>(); // HELLO?? Created it so count = 0;
                foreach (InventoryItemBase item in itemsToReturn)
                {
                    if (item.AssetType == (int)AssetType.Link)
                        itemIDs.Add(item.AssetID);
                }

                // Scan for folder links and insert the items they target and those links at the head of the return data

                // HELLO?? We just created folderIDs and added NOTHING to it,
                // so now can count ever be > 0???
                /*
                if (folderIDs.Count > 0)
                {
                    InventoryCollection[] linkedFolders = m_InventoryService.GetMultipleFoldersContent(coll.Collection.OwnerID, folderIDs.ToArray());
                    foreach (InventoryCollection linkedFolderContents in linkedFolders)
                    {
                        if (linkedFolderContents == null)
                            continue;

                        List<InventoryItemBase> links = linkedFolderContents.Items;

                        itemsToReturn.InsertRange(0, links);
                    }
                }
                */

                if (itemIDs.Count > 0)
                {
                    InventoryItemBase[] linked = m_InventoryService.GetMultipleItems(freq.owner_id, itemIDs.ToArray());
                    if (linked == null)
                    {
                        // OMG!!! One by one!!! This is fallback code, in case the backend isn't updated
                        m_log.WarnFormat("[WEB FETCH INV DESC HANDLER]: GetMultipleItems failed. Falling back to fetching inventory items one by one.");
                        linked = new InventoryItemBase[itemIDs.Count];
                        int i = 0;
                        foreach (UUID id in itemIDs)
                        {
                            linked[i++] = m_InventoryService.GetItem(freq.owner_id, id);
                        }
                    }

                    if (linked != null)
                    {
                        int nLinked = linked.Length;
                        for(int i=0; i<nLinked; i++)
                        {
                            // Take care of genuinely broken links where the target doesn't exist
                            // HACK: Also, don't follow up links that just point to other links.  In theory this is legitimate,
                            // but no viewer has been observed to set these up and this is the lazy way of avoiding cycles
                            // rather than having to keep track of every folder requested in the recursion.
                            if (linked[i] != null && linked[i].AssetType != (int)AssetType.Link)
                            {
                                itemsToReturn.Insert(0, linked[i]);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Convert an internal inventory folder object into an LLSD object.
        /// </summary>
        /// <param name="invFolder"></param>
        /// <returns></returns>
        private LLSDInventoryFolder ConvertInventoryFolder(InventoryFolderBase invFolder)
        {
            LLSDInventoryFolder llsdFolder = new LLSDInventoryFolder();
            llsdFolder.folder_id = invFolder.ID;
            llsdFolder.parent_id = invFolder.ParentID;
            llsdFolder.name = invFolder.Name;
            llsdFolder.type = invFolder.Type;
            llsdFolder.preferred_type = -1;

            return llsdFolder;
        }

        /// <summary>
        /// Convert an internal inventory item object into an LLSD object.
        /// </summary>
        /// <param name="invItem"></param>
        /// <returns></returns>
        private LLSDInventoryItem ConvertInventoryItem(InventoryItemBase invItem)
        {
            LLSDInventoryItem llsdItem = new LLSDInventoryItem();
            llsdItem.asset_id = invItem.AssetID;
            llsdItem.created_at = invItem.CreationDate;
            llsdItem.desc = invItem.Description;
            llsdItem.flags = (int)invItem.Flags;
            llsdItem.item_id = invItem.ID;
            llsdItem.name = invItem.Name;
            llsdItem.parent_id = invItem.Folder;
            llsdItem.type = invItem.AssetType;
            llsdItem.inv_type = invItem.InvType;

            llsdItem.permissions = new LLSDPermissions();
            llsdItem.permissions.creator_id = invItem.CreatorIdAsUuid;
            llsdItem.permissions.base_mask = (int)invItem.CurrentPermissions;
            llsdItem.permissions.everyone_mask = (int)invItem.EveryOnePermissions;
            llsdItem.permissions.group_id = invItem.GroupID;
            llsdItem.permissions.group_mask = (int)invItem.GroupPermissions;
            llsdItem.permissions.is_owner_group = invItem.GroupOwned;
            llsdItem.permissions.next_owner_mask = (int)invItem.NextPermissions;
            llsdItem.permissions.owner_id = invItem.Owner;
            llsdItem.permissions.owner_mask = (int)invItem.CurrentPermissions;
            llsdItem.sale_info = new LLSDSaleInfo();
            llsdItem.sale_info.sale_price = invItem.SalePrice;
            llsdItem.sale_info.sale_type = invItem.SaleType;

            return llsdItem;
        }
    }

    class InventoryCollectionWithDescendents
    {
        public InventoryCollection Collection;
        public int Descendents;
    }
}
