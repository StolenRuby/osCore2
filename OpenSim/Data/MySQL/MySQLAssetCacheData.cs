/* 26 August 2018
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
using System.Data;
using System.Reflection;
using System.Collections.Generic;
using log4net;
using MySql.Data.MySqlClient;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Data;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace OpenSim.Data.MySQL
{
    /// <summary>
    /// A MySQL Interface for the Asset Cache database
    /// </summary>
    public class MySQLAssetCacheData
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private string m_connectionString;

        // Fixed sizes.
        private const int _ID_SIZE_ = 16;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        #region IPlugin Members

        public string Version { get { return "1.0.0.0"; } }

        /// <summary>
        /// <para>Initialises Asset interface</para>
        /// <para>
        /// <list type="bullet">
        /// <item>Loads and initialises the MySQL storage plugin.</item>
        /// <item>Warns and uses the obsolete mysql_connection.ini if connect string is empty.</item>
        /// <item>Check for migration</item>
        /// </list>
        /// </para>
        /// </summary>
        /// <param name="connect">connect string</param>
        public void Initialise(string connect)
        {
            m_connectionString = connect;

            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                Migration m = new Migration(dbcon, Assembly, "AssetCache");
                m.Update();
                dbcon.Close();
            }
        }

        public void Initialise()
        {
            throw new NotImplementedException();
        }

        public void Dispose() { }

        /// <summary>
        /// The name of this DB provider
        /// </summary>
        public string Name
        {
            get { return "MySQL Asset Cache engine"; }
        }

        #endregion

        #region IAssetDataPlugin Members

        public AssetBase GetAsset(string ID)
        {
            AssetBase asset = null;
            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                using (MySqlCommand cmd = new MySqlCommand(
                    "SELECT asset FROM assets_"+ID[0]+" WHERE id=?id", dbcon))
                {
                    cmd.Parameters.Add("?id", MySqlDbType.Binary, _ID_SIZE_).Value = Util.UUID_unHex(ID);
                    try
                    {
                        using (MySqlDataReader dbReader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                        {
                            if (dbReader.Read())
                            {                                
                                using (MemoryStream ms = new MemoryStream((byte[])dbReader["asset"]))
                                {
                                    IFormatter bf = new BinaryFormatter();
                                    asset = (AssetBase)bf.Deserialize(ms);
                                }
                            }
                        }
                    }
                    catch
                    {
                        asset = null;
                    }
                }
                dbcon.Close();
            }
            return asset;
        }

        public bool StoreAsset(AssetBase asset)
        {
            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {                
                dbcon.Open();
                using (MySqlCommand cmd =
                    new MySqlCommand(
                        "replace INTO assets_"+ asset.ID[0] + "(id, access_time, asset)" +
                        "VALUES(?id, ?access_time, ?asset)",
                        dbcon))
                {
                    try
                    {
                        // create unix epoch time
                        int now = (int)Utils.DateTimeToUnixTime(DateTime.UtcNow);
                        cmd.Parameters.Add("?id", MySqlDbType.Binary, _ID_SIZE_).Value = Util.UUID_unHex(asset.ID);
                        cmd.Parameters.AddWithValue("?access_time", now);
                        using (var ms = new MemoryStream())
                        {
                            BinaryFormatter bf = new BinaryFormatter();
                            bf.Serialize(ms, asset);
                            cmd.Parameters.Add("?asset", MySqlDbType.LongBlob).Value = ms.ToArray();
                        }
                        cmd.ExecuteNonQuery();
                    }
                    catch 
                    {
                        dbcon.Close();
                        return false;
                    }
                }
                dbcon.Close();
            }
            return true;
        }

        public void UpdateAccessTime(string ID)
        {
            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                using (MySqlCommand cmd
                    = new MySqlCommand(
                    "update assets_"+ ID[0] + " set access_time=?access_time where id=?id", dbcon))
                {
                    try
                    {
                        // create unix epoch time
                        int now = (int)Utils.DateTimeToUnixTime(DateTime.UtcNow);
                        cmd.Parameters.Add("?id", MySqlDbType.Binary, _ID_SIZE_).Value = Util.UUID_unHex(ID);
                        cmd.Parameters.AddWithValue("?access_time", now);
                        cmd.ExecuteNonQuery();
                    }
                    catch { }
                }
                dbcon.Close();
            }
        }

        public bool AssetExists(string ID)
        {
            bool result = false;
            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                using (MySqlCommand cmd = new MySqlCommand(
                       "SELECT id FROM assets_" + ID[0] + " WHERE id=?id", dbcon))
                {
                    try
                    {
                        cmd.Parameters.Add("?id", MySqlDbType.Binary, _ID_SIZE_).Value = Util.UUID_unHex(ID);
                        using (MySqlDataReader dbReader = cmd.ExecuteReader())
                        {
                            result = dbReader.HasRows;
                        }
                    }
                    catch
                    {
                        dbcon.Close();
                        return false;
                    }
                }
                dbcon.Close();
            }
            return result;
        }

        public bool Delete(string ID)
        {
            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                using (MySqlCommand cmd = new MySqlCommand(
                       "delete from assets_" + ID[0] + " where id=?id", dbcon))
                {
                    try
                    {
                        cmd.Parameters.Add("?id", MySqlDbType.Binary, _ID_SIZE_).Value = Util.UUID_unHex(ID);
                        cmd.ExecuteNonQuery();
                    }
                    catch
                    {
                        dbcon.Close();
                        return false;
                    }
                }
                dbcon.Close();
            }
            return true;
        }

        private static readonly string[] _tableEnd = 
            { "0","1","3","4","5","6","7","8","9","a","b","c","d","e","f" };

        public void Expire(double AgeInHours)
        {
            if (AgeInHours <= 0)
                return;

            // create unix epoch time
            int timestamp = (int)Utils.DateTimeToUnixTime(DateTime.UtcNow.AddHours(-1.0 * AgeInHours));

            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                for (int i = 0; i < 16; i++)
                {
                    using (MySqlCommand cmd = new MySqlCommand(
                    "delete from assets_" + _tableEnd[i] + " where access_time<=?access_time", dbcon))
                    {
                        try
                        {
                            cmd.Parameters.AddWithValue("?access_time", timestamp);
                            cmd.ExecuteNonQuery();
                        } catch { }
                    }
                }
                dbcon.Close();
            }
        }

        #endregion
    }
}
