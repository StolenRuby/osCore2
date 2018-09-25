/* 25 May 2018
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
using log4net;
using OpenMetaverse;
using OpenSim.Framework;

namespace OpenSim.Region.Framework.Scenes
{
    public class EntityManager
    {
        protected readonly Dictionary<UUID, EntityBase> m_entities_U = new Dictionary<UUID, EntityBase>();
        protected readonly Dictionary<uint, EntityBase> m_entities_L = new Dictionary<uint, EntityBase>();

        protected readonly Object m_syncLock = new Object();

        public int Count
        {
            get
            {
                lock (m_syncLock)
                      return m_entities_U.Count;
            }
        }

        public void Add(EntityBase entity)
        {
            lock (m_syncLock)
            {
                m_entities_U[entity.UUID] = entity;
                m_entities_L[entity.LocalId] = entity;
            }
        }

        public void Clear()
        {
            lock (m_syncLock)
            {
                 m_entities_U.Clear();
                 m_entities_L.Clear();
            }
        }

        public bool ContainsKey(UUID id)
        {
            try
            {
                return m_entities_U.ContainsKey(id);
            }
            catch
            {
                return false;
            }
        }

        public bool ContainsKey(uint localID)
        {
            try
            {
                return m_entities_L.ContainsKey(localID);
            }
            catch
            {
                return false;
            }
        }


        private void RemoveEntity(UUID id, uint localID)
        {
            m_entities_L.Remove(localID);
            m_entities_U.Remove(id);
        }

        public bool Remove(uint localID)
        {
            EntityBase entity;
            lock (m_syncLock)
            {
                if (m_entities_L.TryGetValue(localID, out entity))
                {
                    RemoveEntity(entity.UUID, localID);
                    return true;
                }
            }
            return false;
        }

        public bool Remove(UUID id)
        {
            EntityBase entity;
            lock (m_syncLock)
            {
                if (m_entities_U.TryGetValue(id, out entity))
                {
                    RemoveEntity(id, entity.LocalId);
                    return true;
                }
            }
            return false;
        }

        public EntityBase[] GetEntities()
        {
            EntityBase[] tmpArray;
            lock (m_syncLock)
            {
                tmpArray  = new EntityBase[m_entities_U.Count];
                m_entities_U.Values.CopyTo(tmpArray, 0);
            }
            return tmpArray;
        }

        public EntityBase[] GetAllByType<T>()
        {
            EntityBase[] tmpArray = GetEntities();

            List<EntityBase> tmpList = new List<EntityBase>();
            // benchmarking shows for is slightly faster than foreach.
            for (int i = 0; i < tmpArray.Length; i++)
            {
                if (tmpArray[i] is T)
                {
                    tmpList.Add(tmpArray[i]);
                }
            }

            return tmpList.ToArray();
        }

        public EntityBase this[UUID id]
        {
            get
            {
                EntityBase entity;
                TryGetValue(id, out entity);
                return entity;
            }
            set
            {
                Add(value);
            }
        }

        public EntityBase this[uint localID]
        {
            get
            {
                EntityBase entity;
                TryGetValue(localID, out entity);
                return entity;
            }
            set
            {
                Add(value);
            }
        }

        public bool TryGetValue(UUID key, out EntityBase obj)
        {
            try { return m_entities_U.TryGetValue(key, out obj); }
            catch (Exception)
            {
                obj = null;
                return false;
            }
        }

        public bool TryGetValue(uint key, out EntityBase obj)
        {
            try { return m_entities_L.TryGetValue(key, out obj); }
            catch (Exception)
            {
                obj = null;
                return false;
            }
        }
    }
}
